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
import enum
import logging
import re
import tempfile

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apitools.base.py import exceptions
from oauth2client.client import GoogleCredentials

# TODO: Replace with the version from Beam SDK once that is released.
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.libs.variant_merge import move_to_calls_strategy
from gcp_variant_transforms.libs.variant_merge import merge_with_non_variants_strategy
from gcp_variant_transforms.transforms import filter_variants
from gcp_variant_transforms.transforms import merge_variants
from gcp_variant_transforms.transforms import variant_to_bigquery
from gcp_variant_transforms.transforms import merge_headers

_NONE_STRING = 'NONE'
_MOVE_TO_CALLS_STRING = 'MOVE_TO_CALLS'
_MERGE_WITH_NON_VARIANTS_STRING = 'MERGE_WITH_NON_VARIANTS'
# List of supported merge strategies for variants.
# - NONE: Variants will not be merged across files.
# - MOVE_TO_CALLS: uses libs.variant_merge.move_to_calls_strategy
#   for merging. Please see the documentation in that file for details.
_VARIANT_MERGE_STRATEGIES = [
    _NONE_STRING,
    _MOVE_TO_CALLS_STRING,
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
  if (not known_args.variant_merge_strategy or
      known_args.variant_merge_strategy == _NONE_STRING):
    return None
  elif known_args.variant_merge_strategy == _MOVE_TO_CALLS_STRING:
    return move_to_calls_strategy.MoveToCallsStrategy(
        known_args.info_keys_to_move_to_calls_regex,
        known_args.copy_quality_to_calls,
        known_args.copy_filter_to_calls)
  elif known_args.variant_merge_strategy == _MERGE_WITH_NON_VARIANTS_STRING:
    return merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        known_args.info_keys_to_move_to_calls_regex,
        known_args.copy_quality_to_calls,
        known_args.copy_filter_to_calls)
  else:
    raise ValueError('Merge strategy is not supported.')


def _validate_bq_path(output_table, client=None):
  output_table_re_match = re.match(
      r'^((?P<project>.+):)(?P<dataset>\w+)\.(?P<table>[\w\$]+)$',
      output_table)
  if not output_table_re_match:
    raise ValueError(
        'Expected a table reference (PROJECT:DATASET.TABLE) instead of %s.' % (
            output_table))
  try:
    if not client:
      credentials = GoogleCredentials.get_application_default().create_scoped(
          ['https://www.googleapis.com/auth/bigquery'])
      client = bigquery.BigqueryV2(credentials=credentials)
    client.datasets.Get(bigquery.BigqueryDatasetsGetRequest(
        projectId=output_table_re_match.group('project'),
        datasetId=output_table_re_match.group('dataset')))
  except exceptions.HttpError as e:
    if e.status_code == 404:
      raise ValueError('Dataset %s:%s does not exist.' %
                       (output_table_re_match.group('project'),
                        output_table_re_match.group('dataset')))
    else:
      # For the rest of the errors, use BigQuery error message.
      raise


def _validate_args(known_args):
  _validate_bq_path(known_args.output_table)

  if (known_args.variant_merge_strategy != _MOVE_TO_CALLS_STRING
      and known_args.variant_merge_strategy != _MERGE_WITH_NON_VARIANTS_STRING):
    if known_args.info_keys_to_move_to_calls_regex:
      raise ValueError(
          '--info_keys_to_move_to_calls_regex requires '
          '--variant_merge_strategy {}|{}'.format(
              _MOVE_TO_CALLS_STRING, _MERGE_WITH_NON_VARIANTS_STRING))
    if known_args.copy_quality_to_calls:
      raise ValueError(
          '--copy_quality_to_calls requires '
          '--variant_merge_strategy {}|{}'.format(
              _MOVE_TO_CALLS_STRING, _MERGE_WITH_NON_VARIANTS_STRING))
    if known_args.copy_filter_to_calls:
      raise ValueError(
          '--copy_filter_to_calls requires '
          '--variant_merge_strategy {}|{}'.format(
              _MOVE_TO_CALLS_STRING, _MERGE_WITH_NON_VARIANTS_STRING))


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


def _merge_headers(known_args, pipeline_args, pipeline_mode):
  """Merges VCF headers using beam based on pipeline_mode."""
  if (known_args.representative_header_file or
      pipeline_mode == PipelineModes.SMALL):
    return

  pipeline_options = GoogleCloudOptions(pipeline_args)
  if pipeline_options.job_name:
    pipeline_options.job_name += '-' + _MERGE_HEADERS_JOB_NAME
  else:
    pipeline_options.job_name = _MERGE_HEADERS_FILE_NAME

  temp_directory = pipeline_options.temp_location or tempfile.mkdtemp()
  known_args.representative_header_file = FileSystems.join(
      temp_directory, _MERGE_HEADERS_FILE_NAME)

  with beam.Pipeline(options=pipeline_options) as p:
    headers = p
    if pipeline_mode == PipelineModes.LARGE:
      headers |= (beam.Create([known_args.input_pattern])
                  | vcf_header_io.ReadAllVcfHeaders())
    else:
      headers |= vcf_header_io.ReadVcfHeaders(known_args.input_pattern)

    _ = (headers
         | 'MergeHeaders' >> merge_headers.MergeHeaders()
         | 'WriteHeaders' >> vcf_header_io.WriteVcfHeaders(
             known_args.representative_header_file))


def run(argv=None):
  """Runs VCF to BigQuery pipeline."""

  parser = argparse.ArgumentParser()
  parser.register('type', 'bool', lambda v: v.lower() == 'true')

  # I/O options.
  parser.add_argument('--input_pattern',
                      required=True,
                      help='Input pattern for VCF files to process.')
  parser.add_argument('--output_table',
                      required=True,
                      help='BigQuery table to store the results.')
  parser.add_argument(
      '--representative_header_file',
      default='',
      help=('If provided, header values from the provided file will be used as '
            'representative for all files matching input_pattern. '
            'In particular, this will be used to generate the BigQuery schema. '
            'If not provided, header values from all files matching '
            'input_pattern will be merged by key. Only one value will be '
            'chosen (in no particular order) in cases where multiple files use '
            'the same key. Providing this file improves performance if a '
            'large number of files are specified by input_pattern. '
            'Note that each VCF file must still contain valid header files '
            'even if this is provided.'))
  parser.add_argument(
      '--allow_malformed_records',
      type='bool', default=False, nargs='?', const=True,
      help=('If true, failed VCF record reads will not raise errors. '
            'Failed reads will be logged as warnings and returned as '
            'MalformedVcfRecord objects.'))
  parser.add_argument(
      '--optimize_for_large_inputs',
      type='bool', default=False, nargs='?', const=True,
      help=('If true, the pipeline runs in optimized way for handling large '
            'inputs. Set this to true if you are loading more than 50,000 '
            'files.'))

  # Output schema options.
  parser.add_argument(
      '--split_alternate_allele_info_fields',
      type='bool', default=True, nargs='?', const=True,
      help=('If true, all INFO fields with Number=A (i.e. one value for each '
            'alternate allele) will be stored under the alternate_bases '
            'record. If false, they will be stored with the rest of the INFO '
            'fields. Setting this option to true makes querying the data '
            'easier, because it avoids having to map each field with the '
            'corresponding alternate record while querying.'))
  parser.add_argument(
      '--append',
      type='bool', default=False, nargs='?', const=True,
      help=('If true, existing records in output_table will not be '
            'overwritten. New records will be appended to those that already '
            'exist.'))
  # Merging logic.
  parser.add_argument(
      '--variant_merge_strategy',
      default='NONE',
      choices=_VARIANT_MERGE_STRATEGIES,
      help=('Variant merge strategy to use. Set to NONE if variants should '
            'not be merged across files.'))
  # Configs for MOVE_TO_CALLS strategy.
  parser.add_argument(
      '--info_keys_to_move_to_calls_regex',
      default='',
      help=('Regular expression specifying the INFO keys to move to the '
            'associated calls in each VCF file. '
            'Requires variant_merge_strategy={}|{}.'.format(
                _MOVE_TO_CALLS_STRING, _MERGE_WITH_NON_VARIANTS_STRING)))
  parser.add_argument(
      '--copy_quality_to_calls',
      type='bool', default=False, nargs='?', const=True,
      help=('If true, the QUAL field for each record will be copied to '
            'the associated calls in each VCF file. '
            'Requires variant_merge_strategy={}|{}.'.format(
                _MOVE_TO_CALLS_STRING, _MERGE_WITH_NON_VARIANTS_STRING)))
  parser.add_argument(
      '--copy_filter_to_calls',
      type='bool', default=False, nargs='?', const=True,
      help=('If true, the FILTER field for each record will be copied to '
            'the associated calls in each VCF file. '
            'Requires variant_merge_strategy={}|{}.'.format(
                _MOVE_TO_CALLS_STRING, _MERGE_WITH_NON_VARIANTS_STRING)))

  parser.add_argument(
      '--reference_names',
      default=None, nargs='+',
      help=('A list of reference names (separated by a space) to load '
            'to BigQuery. If this parameter is not specified, all '
            'references will be kept.'))

  known_args, pipeline_args = parser.parse_known_args(argv)
  _validate_args(known_args)

  variant_merger = _get_variant_merge_strategy(known_args)
  pipeline_mode = _get_pipeline_mode(known_args)

  # Starts a pipeline to merge VCF headers in beam if the total files that
  # match the input pattern exceeds _SMALL_DATA_THRESHOLD
  _merge_headers(known_args, pipeline_args, pipeline_mode)

  # Retrieve merged headers prior to launching the pipeline. This is needed
  # since the BigQuery schema cannot yet be dynamically created based on input.
  # See https://issues.apache.org/jira/browse/BEAM-2801.
  header_fields = vcf_header_parser.get_merged_vcf_headers(
      known_args.representative_header_file or known_args.input_pattern)

  pipeline_options = PipelineOptions(pipeline_args)
  with beam.Pipeline(options=pipeline_options) as p:
    variants = _read_variants(p, known_args)
    variants |= 'FilterVariants' >> filter_variants.FilterVariants(
        reference_names=known_args.reference_names)
    if variant_merger:
      variants |= (
          'MergeVariants' >> merge_variants.MergeVariants(variant_merger))
    _ = (variants |
         'VariantToBigQuery' >> variant_to_bigquery.VariantToBigQuery(
             known_args.output_table,
             header_fields,
             variant_merger,
             known_args.split_alternate_allele_info_fields,
             append=known_args.append))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
