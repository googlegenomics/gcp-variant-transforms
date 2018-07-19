# Copyright 2018 Google Inc.  All Rights Reserved.
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

r"""Pipeline for downloading BigQuery table to one VCF file. [EXPERIMENTAL]

Run locally:
python -m gcp_variant_transforms.bq_to_vcf \
  --output_file <path to VCF file> \
  --input_table projectname:bigquerydataset.tablename

Run on Dataflow:
python -m gcp_variant_transforms.bq_to_vcf \
  --output_file <path to VCF file> \
  --input_table projectname:bigquerydataset.tablename \
  --project projectname \
  --staging_location gs://bucket/staging \
  --temp_location gs://bucket/temp \
  --job_name bq-to-vcf \
  --setup_file ./setup.py \
  --runner DataflowRunner
"""

import logging
import sys
from typing import List  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.options import pipeline_options

from gcp_variant_transforms import vcf_to_bq_common
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import bigquery_to_variant

_BASE_QUERY_TEMPLATE = 'SELECT * FROM `{INPUT_TABLE}`;'
_COMMAND_LINE_OPTIONS = [variant_transform_options.BigQueryToVcfOptions]


def run(argv=None):
  # type: (List[str]) -> None
  """Runs BigQuery to VCF pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  known_args, pipeline_args = vcf_to_bq_common.parse_args(argv,
                                                          _COMMAND_LINE_OPTIONS)
  bq_source = bigquery.BigQuerySource(
      query=_BASE_QUERY_TEMPLATE.format(
          INPUT_TABLE='.'.join(bigquery_util.parse_table_reference(
              known_args.input_table))),
      validate=True,
      use_standard_sql=True)

  options = pipeline_options.PipelineOptions(pipeline_args)
  with beam.Pipeline(options=options) as p:
    _ = (p | 'ReadFromBigQuery ' >> beam.io.Read(bq_source)
         | bigquery_to_variant.BigQueryToVariant()
         | vcfio.WriteToVcf(known_args.output_file))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
