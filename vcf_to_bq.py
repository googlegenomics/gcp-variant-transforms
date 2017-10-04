r"""Pipeline from exporting VCF files to BigQuery.

Run locally:
python vcf_to_bq.py \
  --input_pattern <path to VCF files> \
  --output_table PROJECT_ID:BIGQUERY_DATASET.TABLE_NAME

Run on Dataflow:
python vcf_to_bq.py \
  --input_pattern <path to VCF files on GCS> \
  --output_table PROJECT_ID:BIGQUERY_DATASET.TABLE_NAME \
  --project PROJECT_ID \
  --staging_location gs://YOUR-BUCKET/staging \
  --temp_location gs://YOUR-BUCKET/temp \
  --job_name vcf-to-bq-export \
  --setup_file ./setup.py \
  --runner DataflowRunner
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# TODO(arostami): Replace with the version from Beam SDK once that is released.
from beam_io import vcfio
from libs.variant_merge import move_to_calls_strategy
from libs.vcf_header_parser import get_merged_vcf_headers
from transforms.merge_variants import MergeVariants
from transforms.variant_to_bigquery import VariantToBigQuery


# List of supported merge strategies for variants.
# - NONE: Variants will not be merged across files.
# - MOVE_TO_CALLS: uses libs.variant_merge.move_to_calls_strategy
#   for merging. Please see the documentation in that file for details.
_VARIANT_MERGE_STRATEGIES = ['NONE', 'MOVE_TO_CALLS']


def _get_variant_merge_strategy(known_args):
  if (not known_args.variant_merge_strategy or
      known_args.variant_merge_strategy == 'NONE'):
    return None
  elif known_args.variant_merge_strategy == 'MOVE_TO_CALLS':
    return move_to_calls_strategy.MoveToCallsStrategy(
        known_args.info_keys_to_move_to_calls_regex,
        known_args.copy_quality_to_calls,
        known_args.copy_filter_to_calls)
  else:
    raise ValueError('Merge strategy is not supported.')


def _validate_args(known_args):
  if known_args.variant_merge_strategy != 'MOVE_TO_CALLS':
    if known_args.info_keys_to_move_to_calls_regex:
      raise ValueError(
          '--info_keys_to_move_to_calls_regex requires '
          '--variant_merge_strategy MOVE_TO_CALLS.')
    if known_args.copy_quality_to_calls:
      raise ValueError(
          '--copy_quality_to_calls requires '
          '--variant_merge_strategy MOVE_TO_CALLS.')
    if known_args.copy_filter_to_calls:
      raise ValueError(
          '--copy_filter_to_calls requires '
          '--variant_merge_strategy MOVE_TO_CALLS.')


def run(argv=None):
  """Runs VCF to BigQuery export pipeline."""

  parser = argparse.ArgumentParser()
  parser.register('type', 'bool', lambda v: v.lower() == 'true')

  # I/O options.
  parser.add_argument('--input_pattern',
                      dest='input_pattern',
                      required=True,
                      help='Input pattern for VCF files to process.')
  parser.add_argument('--output_table',
                      dest='output_table',
                      required=True,
                      help='BigQuery table to store the results.')
  parser.add_argument(
      '--representative_header_file',
      dest='representative_header_file',
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

  # Output schema options.
  parser.add_argument(
      '--split_alternate_allele_info_fields',
      dest='split_alternate_allele_info_fields',
      type='bool', default=True, nargs='?', const=True,
      help=('If true, all INFO fields with Number=A (i.e. one value for each '
            'alternate allele) will be stored under the alternate_bases '
            'record. If false, they will be stored with the rest of the INFO '
            'fields. Setting this option to true makes querying the data '
            'easier, because it avoids having to map each field with the '
            'corresponding alternate record while querying.'))

  # Merging logic.
  parser.add_argument(
      '--variant_merge_strategy',
      dest='variant_merge_strategy',
      default='NONE',
      choices=_VARIANT_MERGE_STRATEGIES,
      help=('Variant merge strategy to use. Set to NONE if variants should '
            'not be merged across files.'))
  # Configs for MOVE_TO_CALLS strategy.
  parser.add_argument(
      '--info_keys_to_move_to_calls_regex',
      dest='info_keys_to_move_to_calls_regex',
      default='',
      help=('Regular expression specifying the INFO keys to move to the '
            'associated calls in each VCF file. '
            'Requires variant_merge_strategy=MOVE_TO_CALLS.'))
  parser.add_argument(
      '--copy_quality_to_calls',
      dest='copy_quality_to_calls',
      type='bool', default=False, nargs='?', const=True,
      help=('If true, the QUAL field for each record will be copied to '
            'the associated calls in each VCF file. '
            'Requires variant_merge_strategy=MOVE_TO_CALLS.'))
  parser.add_argument(
      '--copy_filter_to_calls',
      dest='copy_filter_to_calls',
      type='bool', default=False, nargs='?', const=True,
      help=('If true, the FILTER field for each record will be copied to '
            'the associated calls in each VCF file. '
            'Requires variant_merge_strategy=MOVE_TO_CALLS.'))

  known_args, pipeline_args = parser.parse_known_args(argv)
  _validate_args(known_args)

  variant_merger = _get_variant_merge_strategy(known_args)
  # Retrieve merged headers prior to launching the pipeline. This is needed
  # since the BigQUery shcmea cannot yet be dynamically created based on input.
  # See https://issues.apache.org/jira/browse/BEAM-2801.
  header_fields = get_merged_vcf_headers(
      known_args.representative_header_file or known_args.input_pattern)

  pipeline_options = PipelineOptions(pipeline_args)
  with beam.Pipeline(options=pipeline_options) as p:
    variants = p | 'ReadFromVcf' >> vcfio.ReadFromVcf(known_args.input_pattern)
    if variant_merger:
      variants |= 'MergeVariants' >> MergeVariants(variant_merger)
    _ = (variants |
         'VariantToBigQuery' >> VariantToBigQuery(
             known_args.output_table,
             header_fields,
             variant_merger,
             known_args.split_alternate_allele_info_fields))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
