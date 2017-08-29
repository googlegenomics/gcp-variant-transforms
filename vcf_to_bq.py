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
  --setup_file ./setup.py
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_io import vcfio


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

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  with beam.Pipeline(options=pipeline_options) as p:
    _ = p | 'ReadFromVcf' >> vcfio.ReadFromText(known_args.input_pattern)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
