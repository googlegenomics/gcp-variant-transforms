# Copyright 2017 Google Inc.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

r"""Pipeline for loading VCF files to BigQuery.

Run locally:
python -m gcp_variant_transforms.vcf_to_bq \
  --input_pattern <path to VCF file(s)> \
  --output_table projectname:bigquerydataset.tablename

Run on Dataflow:
python -m gcp_variant_transforms.vcf_to_bq \
  --input_pattern gs://bucket/vcfs/vcffile.vcf \
  --output_table projectname:bigquerydataset.tablename \
  --project projectname \
  --staging_location gs://bucket/staging \
  --temp_location gs://bucket/temp \
  --job_name vcf-to-bq \
  --setup_file ./setup.py \
  --runner DataflowRunner
"""



import argparse  # pylint: disable=unused-import
from datetime import datetime
import logging
import sys
import tempfile
from typing import List, Optional  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam import pvalue  # pylint: disable=unused-import
from apache_beam.io import filesystems
from apache_beam.options import pipeline_options

from gcp_variant_transforms import pipeline_common
from gcp_variant_transforms.beam_io import vcf_parser
from gcp_variant_transforms.libs import avro_util
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import metrics_util
from gcp_variant_transforms.libs import partitioning
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import sample_info_table_schema_generator
from gcp_variant_transforms.libs import schema_converter
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.libs import variant_sharding
from gcp_variant_transforms.libs.annotation.vep import vep_runner_util
from gcp_variant_transforms.libs.variant_merge import merge_with_non_variants_strategy
from gcp_variant_transforms.libs.variant_merge import move_to_calls_strategy
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy  # pylint: disable=unused-import
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import annotate_files
from gcp_variant_transforms.transforms import sample_info_to_avro
from gcp_variant_transforms.transforms import combine_sample_ids
from gcp_variant_transforms.transforms import densify_variants
from gcp_variant_transforms.transforms import extract_input_size
from gcp_variant_transforms.transforms import filter_variants
from gcp_variant_transforms.transforms import infer_headers
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.transforms import merge_variants
from gcp_variant_transforms.transforms import shard_variants
from gcp_variant_transforms.transforms import variant_to_avro
from gcp_variant_transforms.transforms import write_variants_to_shards

_COMMAND_LINE_OPTIONS = [
    variant_transform_options.VcfReadOptions,
    variant_transform_options.BigQueryWriteOptions,
    variant_transform_options.AnnotationOptions,
    variant_transform_options.FilterOptions,
    variant_transform_options.MergeOptions,
    variant_transform_options.ExperimentalOptions,
]

_ESTIMATE_SIZES_FILE_NAME = 'estimate-sizes'
_ESTIMATE_SIZES_JOB_NAME = 'estimate-input-size'
_MERGE_HEADERS_FILE_NAME = 'merged_headers.vcf'
_MERGE_HEADERS_JOB_NAME = 'merge-vcf-headers'
_ANNOTATE_FILES_JOB_NAME = 'annotate-files'
_SHARD_VCF_FILES_JOB_NAME = 'shard-files'
_BQ_SCHEMA_FILE_SUFFIX = 'schema.json'
_AVRO_FOLDER = 'avro'
_SHARDS_FOLDER = 'shards'
_GCS_RECURSIVE_WILDCARD = '**'
SampleNameEncoding = vcf_parser.SampleNameEncoding

_newly_created_tables = []
def _record_newly_created_table(full_table_id):
  global _newly_created_tables  # pylint: disable=global-statement
  _newly_created_tables.append(full_table_id)


def _read_variants(all_patterns,  # type: List[str]
                   pipeline,  # type: beam.Pipeline
                   known_args,  # type: argparse.Namespace
                   pipeline_mode,  # type: int
                   pre_infer_headers=False,  # type: bool
                   keep_raw_sample_names=False,  # type: bool
                   use_1_based_coordinate=True  # type: bool
                  ):
  # type: (...) -> pvalue.PCollection
  """Helper method for returning a PCollection of Variants from VCFs."""
  representative_header_lines = None
  if known_args.representative_header_file:
    representative_header_lines = vcf_header_parser.get_metadata_header_lines(
        known_args.representative_header_file)
  return pipeline_common.read_variants(
      pipeline,
      all_patterns,
      pipeline_mode,
      known_args.allow_malformed_records,
      representative_header_lines,
      pre_infer_headers=pre_infer_headers,
      sample_name_encoding=(
          SampleNameEncoding.NONE if keep_raw_sample_names else
          SampleNameEncoding[known_args.sample_name_encoding]),
      use_1_based_coordinate=use_1_based_coordinate)


def _get_variant_merge_strategy(known_args  # type: argparse.Namespace
                               ):
  # type: (...) -> Optional[variant_merge_strategy.VariantMergeStrategy]
  merge_options = variant_transform_options.MergeOptions
  if (not known_args.variant_merge_strategy or
      known_args.variant_merge_strategy == merge_options.NONE):
    return None
  elif known_args.variant_merge_strategy == merge_options.MOVE_TO_CALLS:
    return move_to_calls_strategy.MoveToCallsStrategy(
        known_args.info_keys_to_move_to_calls_regex,
        known_args.copy_quality_to_calls,
        known_args.copy_filter_to_calls)
  elif (known_args.variant_merge_strategy ==
        merge_options.MERGE_WITH_NON_VARIANTS):
    return merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        known_args.info_keys_to_move_to_calls_regex,
        known_args.copy_quality_to_calls,
        known_args.copy_filter_to_calls)
  else:
    raise ValueError('Merge strategy is not supported.')


def _add_inferred_headers(all_patterns,  # type: List[str]
                          pipeline,  # type: beam.Pipeline
                          known_args,  # type: argparse.Namespace
                          merged_header,  # type: pvalue.PCollection
                          pipeline_mode  # type: int
                         ):
  # type: (...) -> pvalue.PCollection
  annotation_fields_to_infer = (known_args.annotation_fields if
                                known_args.infer_annotation_types else [])
  inferred_headers = (
      _read_variants(all_patterns,
                     pipeline,
                     known_args,
                     pipeline_mode,
                     pre_infer_headers=known_args.infer_headers)
      | 'FilterVariants' >> filter_variants.FilterVariants(
          reference_names=known_args.reference_names)
      | 'InferHeaderFields' >> infer_headers.InferHeaderFields(
          pvalue.AsSingleton(merged_header),
          known_args.allow_incompatible_records,
          known_args.infer_headers,
          annotation_fields_to_infer))
  merged_header = (
      (inferred_headers, merged_header)
      | 'FlattenHeaders' >> beam.Flatten()
      | 'MergeHeadersFromVcfAndVariants' >> merge_headers.MergeHeaders(
          known_args.split_alternate_allele_info_fields,
          known_args.allow_incompatible_records))
  return merged_header


def _shard_variants(known_args, pipeline_args, pipeline_mode):
  # type: (argparse.Namespace, List[str], int) -> List[str]
  """Reads the variants and writes them to VCF shards.

  Returns:
   The VCF shards directory.
  """
  options = pipeline_options.PipelineOptions(pipeline_args)
  google_cloud_options = options.view_as(pipeline_options.GoogleCloudOptions)
  shard_files_job_name = pipeline_common.generate_unique_name(
      _SHARD_VCF_FILES_JOB_NAME)
  _update_google_cloud_job_name(google_cloud_options, shard_files_job_name)
  vcf_shards_output_dir = filesystems.FileSystems.join(
      known_args.annotation_output_dir, _SHARDS_FOLDER)
  with beam.Pipeline(options=options) as p:
    variants = _read_variants(known_args.all_patterns,
                              p,
                              known_args,
                              pipeline_mode,
                              pre_infer_headers=False,
                              keep_raw_sample_names=True,
                              use_1_based_coordinate=False)
    sample_ids = (variants
                  | 'CombineSampleIds' >>
                  combine_sample_ids.SampleIdsCombiner()
                  | 'CombineToList' >> beam.combiners.ToList())
    # TODO(tneymanov): Annotation pipeline currently stores sample IDs instead
    # of sample names in the the sharded VCF files, which would lead to double
    # hashing of samples. Needs to be fixed ASAP.
    _ = (variants
         | 'DensifyVariants' >> densify_variants.DensifyVariants(
             beam.pvalue.AsSingleton(sample_ids))
         | 'WriteToShards' >> write_variants_to_shards.WriteToShards(
             vcf_shards_output_dir,
             beam.pvalue.AsSingleton(sample_ids),
             known_args.number_of_variants_per_shard))

  return [vep_runner_util.format_dir_path(vcf_shards_output_dir) +
          _GCS_RECURSIVE_WILDCARD]


def _get_input_dimensions(known_args, pipeline_args):
  pipeline_mode = pipeline_common.get_pipeline_mode(known_args.all_patterns)
  beam_pipeline_options = pipeline_options.PipelineOptions(pipeline_args)
  google_cloud_options = beam_pipeline_options.view_as(
      pipeline_options.GoogleCloudOptions)

  estimate_sizes_job_name = pipeline_common.generate_unique_name(
      _ESTIMATE_SIZES_JOB_NAME)
  if google_cloud_options.job_name:
    google_cloud_options.job_name += '-' + estimate_sizes_job_name
  else:
    google_cloud_options.job_name = estimate_sizes_job_name
  temp_directory = google_cloud_options.temp_location or tempfile.mkdtemp()
  temp_estimated_input_size_file_name = '-'.join(
      [google_cloud_options.job_name,
       _ESTIMATE_SIZES_FILE_NAME])
  temp_estimated_input_size_file_path = filesystems.FileSystems.join(
      temp_directory, temp_estimated_input_size_file_name)
  with beam.Pipeline(options=beam_pipeline_options) as p:
    estimates = pipeline_common.get_estimates(
        p, pipeline_mode, known_args.all_patterns)

    files_size = (estimates
                  | 'GetFilesSize' >> extract_input_size.GetFilesSize())
    file_count = (estimates
                  | 'CountAllFiles' >> beam.combiners.Count.Globally())
    sample_map = (estimates
                  | 'ExtractSampleMap' >> extract_input_size.GetSampleMap())
    estimated_value_count = (sample_map
                             | extract_input_size.GetEstimatedValueCount())
    estimated_sample_count = (sample_map
                              | extract_input_size.GetEstimatedSampleCount())
    estimated_variant_count = (estimates
                               | 'GetEstimatedVariantCount'
                               >> extract_input_size.GetEstimatedVariantCount())
    _ = (estimated_variant_count
         | beam.ParDo(extract_input_size.print_estimates_to_file,
                      beam.pvalue.AsSingleton(estimated_sample_count),
                      beam.pvalue.AsSingleton(estimated_value_count),
                      beam.pvalue.AsSingleton(files_size),
                      beam.pvalue.AsSingleton(file_count),
                      temp_estimated_input_size_file_path))

  with filesystems.FileSystems.open(temp_estimated_input_size_file_path) as f:
    estimates = f.readlines()
  if len(estimates) != 5:
    raise ValueError('Exactly 5 estimates were expected in {}.'.format(
        temp_estimated_input_size_file_path))

  known_args.estimated_variant_count = int(estimates[0].strip())
  known_args.estimated_sample_count = int(estimates[1].strip())
  known_args.estimated_value_count = int(estimates[2].strip())
  known_args.files_size = int(estimates[3].strip())
  known_args.file_count = int(estimates[4].strip())


def _annotate_vcf_files(all_patterns, known_args, pipeline_args):
  # type: (List[str], argparse.Namespace, List[str]) -> str
  """Annotates the VCF files using VEP.

  Returns:
    The annotated VCF files directory.
  """
  options = pipeline_options.PipelineOptions(pipeline_args)
  google_cloud_options = options.view_as(pipeline_options.GoogleCloudOptions)
  annotate_files_job_name = pipeline_common.generate_unique_name(
      _ANNOTATE_FILES_JOB_NAME)
  _update_google_cloud_job_name(google_cloud_options, annotate_files_job_name)

  with beam.Pipeline(options=options) as p:
    _ = (p
         | beam.Create(all_patterns)
         | 'AnnotateShards' >> beam.ParDo(
             annotate_files.AnnotateFile(known_args, pipeline_args)))
  if known_args.annotation_fields:
    known_args.annotation_fields.append(known_args.vep_info_field)
  else:
    known_args.annotation_fields = [known_args.vep_info_field]
  # TODO(bashir2): The VEP runner by default runs VEP with --allele_number hence
  # we turn on this feature here. However, this might be inconsistent with other
  # annotation fields that are originally present in input files, if they do not
  # have ALLELE_NUM annotation. The fix is to make annotation ALT matching
  # smarter to fall back on other matching methods if ALLELE_NUM is not present.
  # When this is implemented, we may even consider removing use_allele_num flag
  # and always start by checking if ALLELE_NUM is present.
  known_args.use_allele_num = True
  return vep_runner_util.get_output_pattern(known_args.annotation_output_dir)


def _update_google_cloud_job_name(google_cloud_options, job_name):
  if google_cloud_options.job_name:
    google_cloud_options.job_name += '-' + job_name
  else:
    google_cloud_options.job_name = job_name


def _merge_headers(known_args, pipeline_args,
                   pipeline_mode, avro_root_path, annotated_vcf_pattern=None):
  # type: (str, argparse.Namespace, List[str], int, str) -> None
  """Merges VCF headers using beam based on pipeline_mode."""
  options = pipeline_options.PipelineOptions(pipeline_args)

  # Always run pipeline locally if data is small.
  if (pipeline_mode == pipeline_common.PipelineModes.SMALL and
      not known_args.infer_headers and not known_args.infer_annotation_types):
    options.view_as(pipeline_options.StandardOptions).runner = 'DirectRunner'

  google_cloud_options = options.view_as(pipeline_options.GoogleCloudOptions)
  merge_headers_job_name = pipeline_common.generate_unique_name(
      _MERGE_HEADERS_JOB_NAME)
  if google_cloud_options.job_name:
    google_cloud_options.job_name += '-' + merge_headers_job_name
  else:
    google_cloud_options.job_name = merge_headers_job_name

  temp_directory = google_cloud_options.temp_location or tempfile.mkdtemp()
  temp_merged_headers_file_name = '-'.join([google_cloud_options.job_name,
                                            _MERGE_HEADERS_FILE_NAME])
  temp_merged_headers_file_path = filesystems.FileSystems.join(
      temp_directory, temp_merged_headers_file_name)

  with beam.Pipeline(options=options) as p:
    headers = pipeline_common.read_headers(
        p, pipeline_mode,
        known_args.all_patterns)
    _ = (headers
         | 'SampleInfoToAvro'
         >> sample_info_to_avro.SampleInfoToAvro(
             avro_root_path +
             sample_info_table_schema_generator.SAMPLE_INFO_TABLE_SUFFIX,
             SampleNameEncoding[known_args.sample_name_encoding]))
    if known_args.representative_header_file:
      return
    merged_header = pipeline_common.get_merged_headers(
        headers,
        known_args.split_alternate_allele_info_fields,
        known_args.allow_incompatible_records)
    if annotated_vcf_pattern:
      merged_header = pipeline_common.add_annotation_headers(
          p, known_args, pipeline_mode, merged_header,
          annotated_vcf_pattern)
    if known_args.infer_headers or known_args.infer_annotation_types:
      infer_headers_input_pattern = (
          [annotated_vcf_pattern] if
          annotated_vcf_pattern else known_args.all_patterns)
      merged_header = _add_inferred_headers(infer_headers_input_pattern, p,
                                            known_args, merged_header,
                                            pipeline_mode)

    pipeline_common.write_headers(merged_header, temp_merged_headers_file_path)
    known_args.representative_header_file = temp_merged_headers_file_path


def _validate_annotation_pipeline_args(known_args, pipeline_args):
  match_results = filesystems.FileSystems.match(['{}*'.format(
      vep_runner_util.format_dir_path(known_args.annotation_output_dir))])
  if match_results and match_results[0].metadata_list:
    raise ValueError('Output directory {} already exists.'.format(
        known_args.annotation_output_dir))

  flags_dict = pipeline_options.PipelineOptions(pipeline_args).get_all_options()
  expected_flags = ['max_num_workers', 'num_workers']
  for flag in expected_flags:
    if flag in flags_dict and flags_dict[flag] > 0:
      return
  raise ValueError('Could not find any of {} with a valid value among pipeline '
                   'flags {}'.format(expected_flags, flags_dict))


def _run_annotation_pipeline(known_args, pipeline_args):
  # type: (argparse.Namespace, List[str]) -> str
  annotated_vcf_pattern = None
  if known_args.run_annotation_pipeline:
    _validate_annotation_pipeline_args(known_args, pipeline_args)
    known_args.omit_empty_sample_calls = True

    files_to_be_annotated = known_args.all_patterns
    if known_args.shard_variants:
      pipeline_mode = pipeline_common.get_pipeline_mode(files_to_be_annotated)
      files_to_be_annotated = _shard_variants(known_args,
                                              pipeline_args,
                                              pipeline_mode)
    annotated_vcf_pattern = _annotate_vcf_files(files_to_be_annotated,
                                                known_args,
                                                pipeline_args)
  return annotated_vcf_pattern


def _write_schema_to_temp_file(schema, path):
  if not path or not path.startswith('gs://'):
    raise ValueError('Schema must be stored on a GS bucket.')
  schema_json = schema_converter.convert_table_schema_to_json_bq_schema(schema)
  schema_file = tempfile.mkstemp(suffix=_BQ_SCHEMA_FILE_SUFFIX)[1]
  with filesystems.FileSystems.create(schema_file) as file_to_write:
    file_to_write.write(schema_json)
  gs_file_path = path + _BQ_SCHEMA_FILE_SUFFIX
  with filesystems.FileSystems.create(gs_file_path) as file_to_write:
    file_to_write.write(schema_json)
  return schema_file


def _get_avro_root_path(beam_pipeline_options):
  google_cloud_options = beam_pipeline_options.view_as(
      pipeline_options.GoogleCloudOptions)
  return filesystems.FileSystems.join(google_cloud_options.temp_location,
                                      _AVRO_FOLDER,
                                      google_cloud_options.job_name,
                                      datetime.now().strftime('%Y%m%d_%H%M%S'),
                                      '')

def run(argv=None):
  # type: (List[str]) -> None
  """Runs VCF to BigQuery pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  known_args, pipeline_args = pipeline_common.parse_args(argv,
                                                         _COMMAND_LINE_OPTIONS)

  if known_args.auto_flags_experiment:
    _get_input_dimensions(known_args, pipeline_args)

  annotated_vcf_pattern = _run_annotation_pipeline(known_args, pipeline_args)

  all_patterns = (
      [annotated_vcf_pattern] if annotated_vcf_pattern
      else known_args.all_patterns)

  variant_merger = _get_variant_merge_strategy(known_args)

  pipeline_mode = pipeline_common.get_pipeline_mode(all_patterns)

  beam_pipeline_options = pipeline_options.PipelineOptions(pipeline_args)
  avro_root_path = _get_avro_root_path(beam_pipeline_options)
  # Starts a pipeline to merge VCF headers in beam if the total files that
  # match the input pattern exceeds _SMALL_DATA_THRESHOLD
  _merge_headers(known_args, pipeline_args,
                 pipeline_mode, avro_root_path, annotated_vcf_pattern)


  # Retrieve merged headers prior to launching the pipeline. This is needed
  # since the BigQuery schema cannot yet be dynamically created based on input.
  # See https://issues.apache.org/jira/browse/BEAM-2801.
  header_fields = vcf_header_parser.get_vcf_headers(
      known_args.representative_header_file)
  counter_factory = metrics_util.CounterFactory()
  processed_variant_factory = processed_variant.ProcessedVariantFactory(
      header_fields,
      known_args.split_alternate_allele_info_fields,
      known_args.allow_malformed_records,
      known_args.annotation_fields,
      known_args.use_allele_num,
      known_args.minimal_vep_alt_matching,
      known_args.infer_annotation_types,
      counter_factory)

  schema = schema_converter.generate_schema_from_header_fields(
      header_fields, processed_variant_factory, variant_merger,
      known_args.use_1_based_coordinate)

  sharding = variant_sharding.VariantSharding(known_args.sharding_config_path)
  if sharding.should_keep_shard(sharding.get_residual_index()):
    num_shards = sharding.get_num_shards()
  else:
    num_shards = sharding.get_num_shards() - 1

  if known_args.update_schema_on_append:
    for i in range(num_shards):
      table_suffix = sharding.get_output_table_suffix(i)
      table_name = bigquery_util.compose_table_name(known_args.output_table,
                                                    table_suffix)
      bigquery_util.update_bigquery_schema_on_append(schema.fields, table_name)

  pipeline = beam.Pipeline(options=beam_pipeline_options)
  variants = _read_variants(
      all_patterns, pipeline, known_args, pipeline_mode,
      use_1_based_coordinate=known_args.use_1_based_coordinate)
  if known_args.allow_malformed_records:
    variants |= 'DropMalformedRecords' >> filter_variants.FilterVariants()
  sharded_variants = variants | 'ShardVariants' >> beam.Partition(
      shard_variants.ShardVariants(sharding), sharding.get_num_shards())
  variants = []
  for i in range(num_shards):
    suffix = sharding.get_output_table_suffix(i)
    # Convert tuples to list
    variants.append(sharded_variants[i])
    if variant_merger:
      variants[i] |= ('MergeVariants' + suffix >>
                      merge_variants.MergeVariants(variant_merger))
    variants[i] |= (
        'ProcessVariants' + suffix >>
        beam.Map(processed_variant_factory.create_processed_variant). \
        with_output_types(processed_variant.ProcessedVariant))
    _ = (
        variants[i] | 'VariantToAvro' + suffix >>
        variant_to_avro.VariantToAvroFiles(
            avro_root_path + suffix,
            schema,
            allow_incompatible_records=known_args.allow_incompatible_records,
            omit_empty_sample_calls=known_args.omit_empty_sample_calls,
            null_numeric_value_replacement=(
                known_args.null_numeric_value_replacement))
    )
  result = pipeline.run()
  try:
    state = result.wait_until_finish()
    if state != beam.runners.runner.PipelineState.DONE:
      logging.error('Dataflow pipeline terminated in an unexpected state: %s',
                    state)
      raise AssertionError(
          'Dataflow pipeline terminated in {} state'.format(state))
  except Exception as e:
    logging.error('Dataflow pipeline failed.')
    raise e
  else:
    logging.info('Dataflow pipeline finished successfully.')
    metrics_util.log_all_counters(result)

  # After pipeline is done, create output tables and load AVRO files into them.
  schema_file = _write_schema_to_temp_file(schema, avro_root_path)
  suffixes = []
  try:
    for i in range(num_shards):
      suffixes.append(sharding.get_output_table_suffix(i))
      partition_range_end = sharding.get_output_table_partition_range_end(i)
      if not known_args.append:
        table_name = bigquery_util.compose_table_name(known_args.output_table,
                                                      suffixes[i])
        partitioning.create_bq_table(
            table_name, schema_file,
            bigquery_util.ColumnKeyConstants.START_POSITION,
            partition_range_end)
        _record_newly_created_table(table_name)
        logging.info('Integer range partitioned table %s was created.',
                     table_name)
    if not known_args.append:
      _record_newly_created_table(
          sample_info_table_schema_generator.create_sample_info_table(
              known_args.output_table))

    suffixes.append(sample_info_table_schema_generator.SAMPLE_INFO_TABLE_SUFFIX)
    load_avro = avro_util.LoadAvro(
        avro_root_path, known_args.output_table, suffixes, False)
    not_empty_variant_suffixes = load_avro.start_loading()
    logging.info('Following tables were loaded with at least 1 row:')
    for suffix in not_empty_variant_suffixes:
      logging.info(bigquery_util.compose_table_name(known_args.output_table,
                                                    suffix))
    # Remove sample_info table from both lists to avoid duplicating it when
    # --sample_lookup_optimized_output_table flag is set
    suffixes.remove(sample_info_table_schema_generator.SAMPLE_INFO_TABLE_SUFFIX)
    if sample_info_table_schema_generator.SAMPLE_INFO_TABLE_SUFFIX in\
        not_empty_variant_suffixes:
      not_empty_variant_suffixes.remove(
          sample_info_table_schema_generator.SAMPLE_INFO_TABLE_SUFFIX)
  except Exception as e:
    logging.error('Something unexpected happened during the loading of AVRO '
                  'files to BigQuery: %s', str(e))
    logging.info('Since the write to BigQuery stage failed, we did not delete '
                 'AVRO files in your GCS bucket. You can manually import them '
                 'to BigQuery. To avoid extra storage charges, delete them if '
                 'you do not need them, AVRO files are located at: %s',
                 avro_root_path)
    raise e
  else:
    logging.warning('All AVRO files were successfully loaded to BigQuery.')
    if known_args.keep_intermediate_avro_files:
      logging.info('Since "--keep_intermediate_avro_files" flag is set, the '
                   'AVRO files are kept and stored at: %s', avro_root_path)
    else:
      if bigquery_util.delete_gcs_files(avro_root_path) != 0:
        logging.error('Deletion of intermediate AVRO files located at "%s" has '
                      'failed.', avro_root_path)


  if known_args.sample_lookup_optimized_output_table:
    flatten_call_column = partitioning.FlattenCallColumn(
        known_args.output_table, not_empty_variant_suffixes, known_args.append)
    try:
      flatten_schema_file = tempfile.mkstemp(suffix=_BQ_SCHEMA_FILE_SUFFIX)[1]
      if not flatten_call_column.get_flatten_table_schema(flatten_schema_file):
        raise ValueError('Failed to extract schema of flatten table')
      # Create output flatten tables if needed
      if not known_args.append:
        # Create all sample optimized tables including those that will be empty.
        for suffix in suffixes:
          output_table_id = bigquery_util.compose_table_name(
              known_args.sample_lookup_optimized_output_table, suffix)
          partitioning.create_bq_table(
              output_table_id, flatten_schema_file,
              bigquery_util.ColumnKeyConstants.CALLS_SAMPLE_ID,
              partitioning.MAX_RANGE_END)
          _record_newly_created_table(output_table_id)
          logging.info('Sample lookup optimized table %s was created.',
                       output_table_id)
      # Copy to flatten sample lookup tables from the variant lookup tables.
      # Note: uses WRITE_TRUNCATE to overwrite the existing tables (issue #607).
      flatten_call_column.copy_to_flatten_table(
          known_args.sample_lookup_optimized_output_table)
      logging.info('All sample lookup optimized tables are fully loaded.')
    except Exception as e:
      logging.error('Something unexpected happened during the loading rows to '
                    'sample optimized table stage: %s', str(e))
      raise e

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  try:
    run()
  except Exception as e:
    if _newly_created_tables:
      logging.info('Trying to delete all newly created tables.')
      bigquery_util.rollback_newly_created_tables(_newly_created_tables)
    else:
      logging.warning(
          'Since tables were appended, added rows cannot be reverted. You can '
          'utilize BigQuery snapshot decorators to recover your table up to 7 '
          'days ago. For more information please refer to: '
          'https://cloud.google.com/bigquery/table-decorators '
          'Here is the list of tables that you need to manually rollback:')
    raise e
