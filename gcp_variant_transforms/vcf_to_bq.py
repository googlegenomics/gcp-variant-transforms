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

import argparse
import datetime
import enum
import logging
import sys
import tempfile

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import metrics_util
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.libs.annotation.vep import vep_runner
from gcp_variant_transforms.libs.variant_merge import merge_with_non_variants_strategy
from gcp_variant_transforms.libs.variant_merge import move_to_calls_strategy
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import filter_variants
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.transforms import merge_variants
from gcp_variant_transforms.transforms import variant_to_bigquery
from gcp_variant_transforms.transforms import infer_undefined_headers

_COMMAND_LINE_OPTIONS = [
    variant_transform_options.VcfReadOptions,
    variant_transform_options.BigQueryWriteOptions,
    variant_transform_options.AnnotationOptions,
    variant_transform_options.FilterOptions,
    variant_transform_options.MergeOptions,
]

# If the # of files matching the input file_pattern exceeds this value, then
# headers will be merged in beam.
_SMALL_DATA_THRESHOLD = 100
_LARGE_DATA_THRESHOLD = 50000
_MERGE_HEADERS_FILE_NAME = 'merged_headers.vcf'
_MERGE_HEADERS_JOB_NAME = 'merge-vcf-headers'


class PipelineModes(enum.Enum):
  """An Enum specifying the mode of the pipeline based on the data size."""
  SMALL = 0
  MEDIUM = 1
  LARGE = 2


def _get_variant_merge_strategy(known_args):
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


def _read_variants(pipeline, known_args):
  """Helper method for returning a ``PCollection`` of Variants from VCFs."""
  if known_args.optimize_for_large_inputs:
    variants = (pipeline
                | 'InputFilePattern' >> beam.Create(
                    [known_args.input_pattern])
                | 'ReadAllFromVcf' >> vcfio.ReadAllFromVcf(
                    allow_malformed_records=(
                        known_args.allow_malformed_records)))
  else:
    variants = pipeline | 'ReadFromVcf' >> vcfio.ReadFromVcf(
        known_args.input_pattern,
        allow_malformed_records=known_args.allow_malformed_records)
  return variants


def _get_pipeline_mode(known_args):
  """Returns the mode the pipeline should operate in based on input size."""
  if known_args.optimize_for_large_inputs:
    return PipelineModes.LARGE

  match_results = FileSystems.match([known_args.input_pattern])
  if not match_results:
    raise ValueError('No files matched input_pattern: {}'.format(
        known_args.input_pattern))

  total_files = len(match_results[0].metadata_list)
  if total_files > _LARGE_DATA_THRESHOLD:
    return PipelineModes.LARGE
  elif total_files > _SMALL_DATA_THRESHOLD:
    return PipelineModes.MEDIUM

  return PipelineModes.SMALL

def _add_inferred_headers(pipeline, known_args, merged_header):
  inferred_headers = (
      _read_variants(pipeline, known_args)
      | 'FilterVariants' >> filter_variants.FilterVariants(
          reference_names=known_args.reference_names)
      | ' InferUndefinedHeaderFields' >>
      infer_undefined_headers.InferUndefinedHeaderFields(
          beam.pvalue.AsSingleton(merged_header)))
  merged_header = (
      (inferred_headers, merged_header)
      | beam.Flatten()
      | 'MergeHeadersFromVcfAndVariants' >> merge_headers.MergeHeaders(
          known_args.split_alternate_allele_info_fields))
  return merged_header


def _merge_headers(known_args, pipeline_args, pipeline_mode):
  """Merges VCF headers using beam based on pipeline_mode."""
  if known_args.representative_header_file:
    return

  options = PipelineOptions(pipeline_args)

  # Always run pipeline locally if data is small.
  if (pipeline_mode == PipelineModes.SMALL and
      not known_args.infer_undefined_headers):
    options.view_as(StandardOptions).runner = 'DirectRunner'


  google_cloud_options = options.view_as(GoogleCloudOptions)
  if google_cloud_options.job_name:
    google_cloud_options.job_name += '-' + _MERGE_HEADERS_JOB_NAME
  else:
    google_cloud_options.job_name = _MERGE_HEADERS_JOB_NAME

  temp_directory = google_cloud_options.temp_location or tempfile.mkdtemp()
  # Add a time prefix to ensure files are unique in case multiple
  # pipelines are run at the same time.
  temp_merged_headers_file_name = '-'.join([
      datetime.datetime.now().strftime('%Y%m%d-%H%M%S'),
      google_cloud_options.job_name,
      _MERGE_HEADERS_FILE_NAME])
  known_args.representative_header_file = FileSystems.join(
      temp_directory, temp_merged_headers_file_name)

  with beam.Pipeline(options=options) as p:
    headers = p
    if pipeline_mode == PipelineModes.LARGE:
      headers |= (beam.Create([known_args.input_pattern])
                  | vcf_header_io.ReadAllVcfHeaders())
    else:
      headers |= vcf_header_io.ReadVcfHeaders(known_args.input_pattern)

    merged_header = (headers
                     | 'MergeHeaders' >> merge_headers.MergeHeaders(
                         known_args.split_alternate_allele_info_fields,
                         known_args.allow_incompatible_records))

    if known_args.infer_undefined_headers:
      merged_header = _add_inferred_headers(p, known_args, merged_header)

    _ = (merged_header | 'WriteHeaders' >> vcf_header_io.WriteVcfHeaders(
        known_args.representative_header_file))

def _add_parser_arguments(options, parser):
  for transform_options in options:
    transform_options.add_arguments(parser)


def _validate_args(options, parsed_args):
  for transform_options in options:
    transform_options.validate(parsed_args)


def run(argv=None):
  """Runs VCF to BigQuery pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  parser = argparse.ArgumentParser()
  parser.register('type', 'bool', lambda v: v.lower() == 'true')
  command_line_options = [option() for option in _COMMAND_LINE_OPTIONS]
  _add_parser_arguments(command_line_options, parser)
  known_args, pipeline_args = parser.parse_known_args(argv)
  _validate_args(command_line_options, known_args)

  # Note VepRunner cretes new input files, so it should be run before any
  # other access to known_args.input_pattern.
  if known_args.run_vep:
    runner = vep_runner.create_runner_and_update_args(known_args, pipeline_args)
    runner.run_on_all_files()
    runner.wait_until_done()
    logging.info('Using VEP processed files: %s', known_args.input_pattern)

  variant_merger = _get_variant_merge_strategy(known_args)
  pipeline_mode = _get_pipeline_mode(known_args)

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

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline = beam.Pipeline(options=pipeline_options)
  variants = _read_variants(pipeline, known_args)
  variants |= 'FilterVariants' >> filter_variants.FilterVariants(
      reference_names=known_args.reference_names)
  if variant_merger:
    variants |= (
        'MergeVariants' >> merge_variants.MergeVariants(variant_merger))
  proc_variants = variants | 'ProcessVaraints' >> beam.Map(
      processed_variant_factory.create_processed_variant).\
    with_output_types(processed_variant.ProcessedVariant)
  _ = (proc_variants |
       'VariantToBigQuery' >> variant_to_bigquery.VariantToBigQuery(
           known_args.output_table,
           header_fields,
           variant_merger,
           processed_variant_factory,
           append=known_args.append,
           allow_incompatible_records=known_args.allow_incompatible_records,
           omit_empty_sample_calls=known_args.omit_empty_sample_calls))
  result = pipeline.run()
  result.wait_until_finish()

  metrics_util.log_all_counters(result)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
