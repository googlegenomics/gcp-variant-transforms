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
import datetime
import logging
import sys
import tempfile
from typing import List, Optional  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam import pvalue  # pylint: disable=unused-import
from apache_beam.io import filesystems
from apache_beam.options import pipeline_options

from gcp_variant_transforms import vcf_to_bq_common
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import metrics_util
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.libs import variant_partition
from gcp_variant_transforms.libs.annotation.vep import vep_runner
from gcp_variant_transforms.libs.variant_merge import merge_with_non_variants_strategy
from gcp_variant_transforms.libs.variant_merge import move_to_calls_strategy
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy  # pylint: disable=unused-import
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import filter_variants
from gcp_variant_transforms.transforms import infer_headers
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.transforms import merge_variants
from gcp_variant_transforms.transforms import partition_variants
from gcp_variant_transforms.transforms import variant_to_bigquery

_COMMAND_LINE_OPTIONS = [
    variant_transform_options.VcfReadOptions,
    variant_transform_options.BigQueryWriteOptions,
    variant_transform_options.AnnotationOptions,
    variant_transform_options.FilterOptions,
    variant_transform_options.MergeOptions,
    variant_transform_options.PartitionOptions,
]

_MERGE_HEADERS_FILE_NAME = 'merged_headers.vcf'
_MERGE_HEADERS_JOB_NAME = 'merge-vcf-headers'


def _read_variants(pipeline, known_args):
  # type: (beam.Pipeline, argparse.Namespace) -> pvalue.PCollection
  """Helper method for returning a PCollection of Variants from VCFs."""
  representative_header_lines = None
  if known_args.representative_header_file:
    representative_header_lines = vcf_header_parser.get_metadata_header_lines(
        known_args.representative_header_file)

  if known_args.optimize_for_large_inputs:
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
        allow_malformed_records=known_args.allow_malformed_records)
  return variants


def _get_variant_merge_strategy(known_args  # type: argparse.Namespace
                               ):
  # type: (...) -> Optional(variant_merge_strategy.VariantMergeStrategy)
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


def _do_inferred_merge(inferred_headers, merged_header, known_args, label):
  flatten_label = 'Flatten{}'.format(label)
  merge_label = 'Merge{}FromVcfAndVariants'.format(label)
  merged_header = (
      (inferred_headers, merged_header)
      | flatten_label >> beam.Flatten()
      | merge_label >> merge_headers.MergeHeaders(
          known_args.split_alternate_allele_info_fields,
          known_args.allow_incompatible_records))
  return merged_header


def _add_inferred_headers(pipeline,  # type: beam.Pipeline
                          known_args,  # type: argparse.Namespace
                          merged_header  # type: pvalue.PCollection
                         ):
  # type: (...) -> pvalue.PCollection
  filtered_variants = (_read_variants(pipeline, known_args)
                       | 'FilterVariants' >> filter_variants.FilterVariants(
                           reference_names=known_args.reference_names))
  if known_args.infer_headers:
    inferred_headers = (filtered_variants | 'InferHeaderFields' >>
                        infer_headers.InferHeaderFields(
                            pvalue.AsSingleton(merged_header),
                            known_args.allow_incompatible_records))
    merged_header = _do_inferred_merge(inferred_headers,
                                       merged_header,
                                       known_args,
                                       'Headers')
  if known_args.infer_annotation_types:
    inferred_headers = (filtered_variants | 'InferAnnotationTypes' >>
                        infer_headers.InferAnnotationTypes(
                            pvalue.AsSingleton(merged_header),
                            known_args.annotation_fields))
    merged_header = _do_inferred_merge(inferred_headers,
                                       merged_header,
                                       known_args,
                                       'Types')
  return merged_header


def _merge_headers(known_args, pipeline_args, pipeline_mode):
  # type: (argparse.Namespace, List[str], int) -> None
  """Merges VCF headers using beam based on pipeline_mode."""
  if known_args.representative_header_file:
    return

  options = pipeline_options.PipelineOptions(pipeline_args)

  # Always run pipeline locally if data is small.
  if (pipeline_mode == vcf_to_bq_common.PipelineModes.SMALL and
      not known_args.infer_headers):
    options.view_as(pipeline_options.StandardOptions).runner = 'DirectRunner'

  google_cloud_options = options.view_as(pipeline_options.GoogleCloudOptions)
  # Add a time suffix to ensure the job names and the merged headers files are
  # unique in case multiple pipelines are run at the same time.
  merge_headers_job_name = '-'.join([
      _MERGE_HEADERS_JOB_NAME,
      datetime.datetime.now().strftime('%Y%m%d-%H%M%S')])
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
    headers = vcf_to_bq_common.read_headers(p, pipeline_mode, known_args)
    merged_header = vcf_to_bq_common.get_merged_headers(
        headers,
        known_args.split_alternate_allele_info_fields,
        known_args.allow_incompatible_records)
    if known_args.infer_headers or known_args.infer_annotation_types:
      merged_header = _add_inferred_headers(p, known_args, merged_header)
    vcf_to_bq_common.write_headers(merged_header, temp_merged_headers_file_path)
    known_args.representative_header_file = temp_merged_headers_file_path


def run(argv=None):
  # type: (List[str]) -> None
  """Runs VCF to BigQuery pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  known_args, pipeline_args = vcf_to_bq_common.parse_args(argv,
                                                          _COMMAND_LINE_OPTIONS)
  # Note VepRunner creates new input files, so it should be run before any
  # other access to known_args.input_pattern.
  if known_args.run_annotation_pipeline:
    runner = vep_runner.create_runner_and_update_args(known_args, pipeline_args)
    runner.run_on_all_files()
    runner.wait_until_done()
    logging.info('Using VEP processed files: %s', known_args.input_pattern)

  variant_merger = _get_variant_merge_strategy(known_args)
  pipeline_mode = vcf_to_bq_common.get_pipeline_mode(
      known_args.input_pattern, known_args.optimize_for_large_inputs)

  # Starts a pipeline to merge VCF headers in beam if the total files that
  # match the input pattern exceeds _SMALL_DATA_THRESHOLD
  _merge_headers(known_args, pipeline_args, pipeline_mode)

  # Retrieve merged headers prior to launching the pipeline. This is needed
  # since the BigQuery schema cannot yet be dynamically created based on input.
  # See https://issues.apache.org/jira/browse/BEAM-2801.
  header_fields = vcf_header_parser.get_vcf_headers(
      known_args.representative_header_file)
  counter_factory = metrics_util.CounterFactory()
  processed_variant_factory = processed_variant.ProcessedVariantFactory(
      header_fields,
      known_args.split_alternate_allele_info_fields,
      known_args.annotation_fields,
      known_args.use_allele_num,
      known_args.minimal_vep_alt_matching,
      counter_factory)

  partitioner = None
  if ((known_args.optimize_for_large_inputs and variant_merger) or
      known_args.partition_config_path):
    partitioner = variant_partition.VariantPartition(
        known_args.partition_config_path)

  beam_pipeline_options = pipeline_options.PipelineOptions(pipeline_args)
  pipeline = beam.Pipeline(options=beam_pipeline_options)
  variants = _read_variants(pipeline, known_args)
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
        'ProcessVaraints' + str(i) >>
        beam.Map(processed_variant_factory.create_processed_variant).\
            with_output_types(processed_variant.ProcessedVariant))
  if partitioner and partitioner.should_flatten():
    variants = [variants | 'FlattenPartitions' >> beam.Flatten()]
    num_partitions = 1

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
             num_bigquery_write_shards=known_args.num_bigquery_write_shards))

  result = pipeline.run()
  result.wait_until_finish()

  metrics_util.log_all_counters(result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
