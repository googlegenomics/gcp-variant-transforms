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

import datetime
import logging
import sys
import tempfile
from argparse import Namespace  # pylint: disable=unused-import
from typing import List  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam import Pipeline  # pylint: disable=unused-import
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.pvalue import PCollection  # pylint: disable=unused-import

from gcp_variant_transforms import general_process
from gcp_variant_transforms.libs import metrics_util
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.libs.variant_merge import merge_with_non_variants_strategy
from gcp_variant_transforms.libs.variant_merge import move_to_calls_strategy
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.options.variant_transform_options import MergeOptions
from gcp_variant_transforms.transforms import filter_variants
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.transforms import merge_variants
from gcp_variant_transforms.transforms import variant_to_bigquery

_COMMAND_LINE_OPTIONS = [
    variant_transform_options.VcfReadOptions,
    variant_transform_options.BigQueryWriteOptions,
    variant_transform_options.AnnotationOptions,
    variant_transform_options.FilterOptions,
    variant_transform_options.MergeOptions,
]

_MERGE_HEADERS_FILE_NAME = 'merged_headers.vcf'
_MERGE_HEADERS_JOB_NAME = 'merge-vcf-headers'


def _get_variant_merge_strategy(known_args):
  # type: (Namespace) -> MergeOptions.VARIANT_MERGE_STRATEGIES
  if (not known_args.variant_merge_strategy or
      known_args.variant_merge_strategy == MergeOptions.NONE):
    return None
  elif known_args.variant_merge_strategy == MergeOptions.MOVE_TO_CALLS:
    return move_to_calls_strategy.MoveToCallsStrategy(
        known_args.info_keys_to_move_to_calls_regex,
        known_args.copy_quality_to_calls,
        known_args.copy_filter_to_calls)
  elif (known_args.variant_merge_strategy ==
        MergeOptions.MERGE_WITH_NON_VARIANTS):
    return merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        known_args.info_keys_to_move_to_calls_regex,
        known_args.copy_quality_to_calls,
        known_args.copy_filter_to_calls)
  else:
    raise ValueError('Merge strategy is not supported.')


def _add_inferred_headers(pipeline, known_args, merged_header):
  # type: (Pipeline, Namespace, PCollection) -> PCollection
  inferred_headers = general_process.get_inferred_headers(pipeline, known_args,
                                                          merged_header)
  merged_header = (
      (inferred_headers, merged_header)
      | beam.Flatten()
      | 'MergeHeadersFromVcfAndVariants' >> merge_headers.MergeHeaders(
          known_args.split_alternate_allele_info_fields))
  return merged_header


def _merge_headers(known_args, pipeline_args, pipeline_mode):
  # type: (Namespace, List[str], int) -> None
  """Merges VCF headers using beam based on pipeline_mode."""
  if known_args.representative_header_file:
    return

  options = PipelineOptions(pipeline_args)

  # Always run pipeline locally if data is small.
  if (pipeline_mode == general_process.PipelineModes.SMALL and
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
    headers = general_process.read_headers(p, pipeline_mode, known_args)
    merged_header = general_process.get_merged_headers(headers, known_args)
    if known_args.infer_undefined_headers:
      merged_header = _add_inferred_headers(p, known_args, merged_header)
    general_process.write_headers(merged_header,
                                  known_args.representative_header_file)


def run(argv=None):
  # type: (List[str]) -> None
  """Runs VCF to BigQuery pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  known_args, pipeline_args = general_process.parse_args(argv,
                                                         _COMMAND_LINE_OPTIONS)
  variant_merger = _get_variant_merge_strategy(known_args)
  pipeline_mode = general_process.get_pipeline_mode(known_args)

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
  variants = general_process.read_variants(pipeline, known_args)
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
