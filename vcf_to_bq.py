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
from libs.vcf_header_parser import get_merged_vcf_headers
from transforms.variant_to_bigquery import VariantToBigQuery


def run(argv=None):
  """Runs VCF to BigQuery export pipeline."""

  parser = argparse.ArgumentParser()
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
  parser.add_argument(
      '--split_alternate_allele_info_fields',
      dest='split_alternate_allele_info_fields',
      default=True,
      help=('If true, all INFO fields with `Number=A (i.e. one value for each '
            'alternate allele) will be stored under the alternate_bases '
            'record. If false, they will be stored with the rest of the INFO '
            'fields. Setting this option to true makes querying the data '
            'easier, because it avoids having to map each field with the '
            'corresponding alternate record while querying.'))

  known_args, pipeline_args = parser.parse_known_args(argv)

  # Retrieve merged headers prior to launching the pipeline. This is needed
  # since the BigQUery shcmea cannot yet be dynamically created based on input.
  # See https://issues.apache.org/jira/browse/BEAM-2801.
  header_fields = get_merged_vcf_headers(
      known_args.representative_header_file or known_args.input_pattern)

  pipeline_options = PipelineOptions(pipeline_args)
  with beam.Pipeline(options=pipeline_options) as p:
    _ = (p
         | 'ReadFromVcf' >> vcfio.ReadFromVcf(known_args.input_pattern)
         | 'VariantToBigQuery' >> VariantToBigQuery(
             known_args.output_table,
             header_fields,
             known_args.split_alternate_allele_info_fields))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
