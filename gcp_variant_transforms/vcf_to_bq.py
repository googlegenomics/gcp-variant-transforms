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

from __future__ import absolute_import

import argparse  # pylint: disable=unused-import
import logging
import sys
import tempfile
from typing import List, Optional  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam import pvalue  # pylint: disable=unused-import
from apache_beam.io import filesystems
from apache_beam.options import pipeline_options

from gcp_variant_transforms import pipeline_common
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import metrics_util
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.libs import variant_partition
from gcp_variant_transforms.libs.annotation.vep import vep_runner_util
from gcp_variant_transforms.libs.variant_merge import merge_with_non_variants_strategy
from gcp_variant_transforms.libs.variant_merge import move_to_calls_strategy
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy  # pylint: disable=unused-import
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import annotate_files
from gcp_variant_transforms.transforms import combine_call_names
from gcp_variant_transforms.transforms import densify_variants
from gcp_variant_transforms.transforms import filter_variants
from gcp_variant_transforms.transforms import infer_headers
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.transforms import merge_variants
from gcp_variant_transforms.transforms import partition_variants
from gcp_variant_transforms.transforms import variant_to_avro
from gcp_variant_transforms.transforms import variant_to_bigquery
from gcp_variant_transforms.transforms import write_variants_to_shards


_COMMAND_LINE_OPTIONS = [
    variant_transform_options.VcfReadOptions,
    variant_transform_options.AvroWriteOptions,
    variant_transform_options.BigQueryWriteOptions,
    variant_transform_options.AnnotationOptions,
    variant_transform_options.FilterOptions,
    variant_transform_options.MergeOptions,
    variant_transform_options.PartitionOptions,
]

_MERGE_HEADERS_FILE_NAME = 'merged_headers.vcf'
_MERGE_HEADERS_JOB_NAME = 'merge-vcf-headers'
_ANNOTATE_FILES_JOB_NAME = 'annotate-files'
_SHARD_VCF_FILES_JOB_NAME = 'shard-files'
_SHARDS_FOLDER = 'shards'


def _read_variants(pipeline, known_args, pipeline_mode):
  # type: (beam.Pipeline, argparse.Namespace, bool) -> pvalue.PCollection
  """Helper method for returning a PCollection of Variants from VCFs."""
  representative_header_lines = None
  if known_args.representative_header_file:
    representative_header_lines = vcf_header_parser.get_metadata_header_lines(
        known_args.representative_header_file)

  if pipeline_mode:
    variants = (pipeline
                | 'InputFilePattern' >> beam.Create([known_args.input_pattern])
                | 'ReadAllFromVcf' >> vcfio.ReadAllFromVcf(
                    representative_header_lines=representative_header_lines,
                    allow_malformed_records=(
                        known_args.allow_malformed_records)))
  else:
    variants = pipeline | 'ReadFromVcf' >> vcfio.ReadFromVcf(
        known_args.input_pattern,
        representative_header_lines=representative_header_lines,
        allow_malformed_records=known_args.allow_malformed_records,
        vcf_parser_type=vcfio.VcfParserType[known_args.vcf_parser])
  return variants


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


def _add_inferred_headers(pipeline,  # type: beam.Pipeline
                          known_args,  # type: argparse.Namespace
                          merged_header,  # type: pvalue.PCollection
                          pipeline_mode  # type: bool
                         ):
  # type: (...) -> pvalue.PCollection
  annotation_fields_to_infer = (known_args.annotation_fields if
                                known_args.infer_annotation_types else [])
  inferred_headers = (
      _read_variants(pipeline, known_args, pipeline_mode)
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


def _shard_vcf_files(known_args, pipeline_args, pipeline_mode):
  # type: (argparse.Namespace, beam.Pipeline, bool) -> None
  """Shards the VCF files."""
  options = pipeline_options.PipelineOptions(pipeline_args)
  google_cloud_options = options.view_as(pipeline_options.GoogleCloudOptions)
  shard_files_job_name = pipeline_common.generate_unique_name(
      _SHARD_VCF_FILES_JOB_NAME)
  _update_google_cloud_job_name(google_cloud_options, shard_files_job_name)
  vcf_shards_output_dir = filesystems.FileSystems.join(
      known_args.annotation_output_dir, _SHARDS_FOLDER)
  with beam.Pipeline(options=options) as p:
    variants = _read_variants(p, known_args, pipeline_mode)
    call_names = (variants
                  | 'CombineCallNames' >>
                  combine_call_names.CallNamesCombiner())
    _ = (variants
         | 'DensifyVariants' >> densify_variants.DensifyVariants(
             beam.pvalue.AsSingleton(call_names))
         | 'WriteToShards' >> write_variants_to_shards.WriteToShards(
             vcf_shards_output_dir,
             beam.pvalue.AsSingleton(call_names),
             known_args.number_of_variants_per_shard))

  known_args.input_pattern = vep_runner_util.format_dir_path(
      vcf_shards_output_dir)


def _annotate_vcf_files(known_args, pipeline_args):
  # type: (argparse.Namespace, beam.Pipeline) -> None
  """Annotates the VCF files using VEP."""
  options = pipeline_options.PipelineOptions(pipeline_args)
  google_cloud_options = options.view_as(pipeline_options.GoogleCloudOptions)
  annotate_files_job_name = pipeline_common.generate_unique_name(
      _ANNOTATE_FILES_JOB_NAME)
  _update_google_cloud_job_name(google_cloud_options, annotate_files_job_name)

  with beam.Pipeline(options=options) as p:
    _ = (p
         | beam.Create([known_args.input_pattern])
         | 'AnnotateShards' >> beam.ParDo(
             annotate_files.AnnotateFile(known_args, pipeline_args)))
  known_args.input_pattern = vep_runner_util.get_output_pattern(
      known_args.annotation_output_dir)
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


def _update_google_cloud_job_name(google_cloud_options, job_name):
  if google_cloud_options.job_name:
    google_cloud_options.job_name += '-' + job_name
  else:
    google_cloud_options.job_name = job_name


def _merge_headers(known_args, pipeline_args, pipeline_mode,
                   original_input_pattern=None):
  # type: (argparse.Namespace, List[str], int, str) -> None
  """Merges VCF headers using beam based on pipeline_mode."""
  if known_args.representative_header_file:
    return

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
    headers = pipeline_common.read_headers(p, pipeline_mode, known_args)
    merged_header = pipeline_common.get_merged_headers(
        headers,
        known_args.split_alternate_allele_info_fields,
        known_args.allow_incompatible_records)
    if original_input_pattern:
      merged_header = pipeline_common.add_original_headers(
          p, known_args, pipeline_mode, merged_header, original_input_pattern)
    if known_args.infer_headers or known_args.infer_annotation_types:
      merged_header = _add_inferred_headers(p, known_args, merged_header,
                                            pipeline_mode)
    pipeline_common.write_headers(merged_header, temp_merged_headers_file_path)
    known_args.representative_header_file = temp_merged_headers_file_path


def run(argv=None):
  # type: (List[str]) -> None
  """Runs VCF to BigQuery pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  known_args, pipeline_args = pipeline_common.parse_args(argv,
                                                         _COMMAND_LINE_OPTIONS)
  pipeline_mode = pipeline_common.get_pipeline_mode(
      known_args.input_pattern, known_args.optimize_for_large_inputs)
  # Note VepRunner creates new input files, so it should be run before any
  # other access to known_args.input_pattern.
  original_input_pattern = None
  if known_args.run_annotation_pipeline:
    match_results = filesystems.FileSystems.match(
        ['{}*'.format(known_args.annotation_output_dir)])
    if match_results and match_results[0].metadata_list:
      raise ValueError('Output directory {} already exists.'.format(
          known_args.annotation_output_dir))
    known_args.omit_empty_sample_calls = True
    original_input_pattern = known_args.input_pattern

    if known_args.shard_input_files:
      _shard_vcf_files(known_args, pipeline_args, pipeline_mode)
    _annotate_vcf_files(known_args, pipeline_args)

  variant_merger = _get_variant_merge_strategy(known_args)
  # Starts a pipeline to merge VCF headers in beam if the total files that
  # match the input pattern exceeds _SMALL_DATA_THRESHOLD
  pipeline_mode = pipeline_common.get_pipeline_mode(
      known_args.input_pattern, known_args.optimize_for_large_inputs)
  _merge_headers(known_args, pipeline_args, pipeline_mode,
                 original_input_pattern)

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

  partitioner = None
  if ((known_args.optimize_for_large_inputs and variant_merger) or
      known_args.partition_config_path):
    partitioner = variant_partition.VariantPartition(
        known_args.partition_config_path)

  beam_pipeline_options = pipeline_options.PipelineOptions(pipeline_args)
  pipeline = beam.Pipeline(options=beam_pipeline_options)
  variants = _read_variants(pipeline, known_args, pipeline_mode)
  variants |= 'FilterVariants' >> filter_variants.FilterVariants(
      reference_names=known_args.reference_names)
  if partitioner:
    num_partitions = partitioner.get_num_partitions()
    partitioned_variants = variants | 'PartitionVariants' >> beam.Partition(
        partition_variants.PartitionVariants(partitioner), num_partitions)
    variants = []
    for i in range(num_partitions):
      if partitioner.should_keep_partition(i):
        variants.append(partitioned_variants[i])
      else:
        num_partitions -= 1
  else:
    # By default we don't partition the data, so we have only 1 partition.
    num_partitions = 1
    variants = [variants]

  for i in range(num_partitions):
    if variant_merger:
      variants[i] |= ('MergeVariants' + str(i) >>
                      merge_variants.MergeVariants(variant_merger))
    variants[i] |= (
        'ProcessVariants' + str(i) >>
        beam.Map(processed_variant_factory.create_processed_variant).\
            with_output_types(processed_variant.ProcessedVariant))
  if partitioner and partitioner.should_flatten():
    variants = [variants | 'FlattenPartitions' >> beam.Flatten()]
    num_partitions = 1

  if known_args.output_table:
    for i in range(num_partitions):
      table_suffix = ''
      if partitioner and partitioner.get_partition_name(i):
        table_suffix = '_' + partitioner.get_partition_name(i)
      table_name = known_args.output_table + table_suffix
      _ = (variants[i] | 'VariantToBigQuery' + table_suffix >>
           variant_to_bigquery.VariantToBigQuery(
               table_name,
               header_fields,
               variant_merger,
               processed_variant_factory,
               append=known_args.append,
               update_schema_on_append=known_args.update_schema_on_append,
               allow_incompatible_records=known_args.allow_incompatible_records,
               omit_empty_sample_calls=known_args.omit_empty_sample_calls,
               num_bigquery_write_shards=known_args.num_bigquery_write_shards,
               null_numeric_value_replacement=(
                   known_args.null_numeric_value_replacement)))

  if known_args.output_avro_path:
    # TODO(bashir2): Add an integration test that outputs to Avro files and
    # also imports to BigQuery. Then import those Avro outputs using the bq
    # tool and verify that the two tables are identical.
    _ = (
        variants | 'FlattenToOnePCollection' >> beam.Flatten()
        | 'VariantToAvro' >>
        variant_to_avro.VariantToAvroFiles(
            known_args.output_avro_path,
            header_fields,
            processed_variant_factory,
            variant_merger=variant_merger,
            allow_incompatible_records=known_args.allow_incompatible_records,
            omit_empty_sample_calls=known_args.omit_empty_sample_calls,
            null_numeric_value_replacement=(
                known_args.null_numeric_value_replacement))
    )

  result = pipeline.run()
  result.wait_until_finish()

  metrics_util.log_all_counters(result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
