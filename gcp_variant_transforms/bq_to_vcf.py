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

r"""Pipeline for loading BigQuery table to one VCF file. [EXPERIMENTAL]

The pipeline reads the variants from BigQuery table, groups a collection of
variants within a contiguous region of the genome (the size of the collection is
adjustable through flag `--number_of_bases_per_shard`), sorts them, and then
writes to one VCF shard. At the end, it consolidates VCF shards into one.

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
from datetime import datetime
from typing import Iterable, List, Tuple  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam.io import filesystems
from apache_beam.io.gcp import bigquery
from apache_beam.options import pipeline_options

from gcp_variant_transforms import vcf_to_bq_common
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import vcf_file_composer
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import bigquery_to_variant
from gcp_variant_transforms.transforms import densify_variants

_BASE_QUERY_TEMPLATE = 'SELECT * FROM `{INPUT_TABLE}`;'
_COMMAND_LINE_OPTIONS = [variant_transform_options.BigQueryToVcfOptions]


def run(argv=None):
  # type: (List[str]) -> None
  """Runs BigQuery to VCF pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  known_args, pipeline_args = vcf_to_bq_common.parse_args(argv,
                                                          _COMMAND_LINE_OPTIONS)
  options = pipeline_options.PipelineOptions(pipeline_args)
  google_cloud_options = options.view_as(pipeline_options.GoogleCloudOptions)
  # TODO(allieychen): Add support for local location.
  if not google_cloud_options.temp_location or not google_cloud_options.project:
    raise ValueError('temp_location and project must be set.')

  shards_folder = 'bq_to_vcf_temp_files_{}'.format(
      datetime.now().strftime('%Y%m%d_%H%M%S'))
  bq_to_vcf_temp_folder = filesystems.FileSystems.join(
      google_cloud_options.temp_location, shards_folder)

  _bigquery_to_vcf_shards(known_args, options, bq_to_vcf_temp_folder)
  vcf_file_composer.compose_vcf_data_files(google_cloud_options.project,
                                           bq_to_vcf_temp_folder,
                                           known_args.output_file)

  # TODO(allieychen): Eventually, it further consolidates the meta information,
  # data header line, and the composed VCF data file into the `output_file`.


def _bigquery_to_vcf_shards(
    known_args,  # type: argparse.Namespace
    beam_pipeline_options,  # type: pipeline_options.PipelineOptions
    bq_to_vcf_temp_folder  # type: str
    ):
  # type: (...) -> None
  """Runs BigQuery to VCF shards pipelines.

  It reads the variants from BigQuery table, groups a collection of variants
  within a contiguous region of the genome (the size of the collection is
  adjustable through flag `--number_of_bases_per_shard`), sorts them, and then
  writes to one VCF shard.

  TODO(allieychen): Eventually, it also generates the meta information file and
  data header file.
  """
  bq_source = bigquery.BigQuerySource(
      query=_BASE_QUERY_TEMPLATE.format(
          INPUT_TABLE='.'.join(bigquery_util.parse_table_reference(
              known_args.input_table))),
      validate=True,
      use_standard_sql=True)

  with beam.Pipeline(options=beam_pipeline_options) as p:
    _ = (p | 'ReadFromBigQuery ' >> beam.io.Read(bq_source)
         | bigquery_to_variant.BigQueryToVariant()
         | densify_variants.DensifyVariants()
         | 'PairVariantWithKey' >>
         beam.Map(_pair_variant_with_key, known_args.number_of_bases_per_shard)
         | 'GroupVariantsByKey' >> beam.GroupByKey()
         | beam.ParDo(_get_file_path_and_sorted_variants, bq_to_vcf_temp_folder)
         | vcfio.WriteVcfDataLines())


def _get_file_path_and_sorted_variants((file_name, variants), file_path_prefix):
  # type: (Tuple[str, List], str) -> Iterable[Tuple[str, List]]
  """Returns the file path and the sorted variants.

  Args:
    file_name: The file name that associated with the `variants`, which is used
      to form the file path where the `variants` are written to.
    variants: A collection of variants within a contiguous region of the genome.
    file_path_prefix: The common file prefix for all VCF files generated in the
      pipeline. The files written will begin with this prefix, followed by the
      `file_name`.
  """
  # pylint: disable=redefined-outer-name,reimported
  from apache_beam.io import filesystems
  file_path = filesystems.FileSystems.join(file_path_prefix, file_name)
  yield (file_path, sorted(variants))


def _pair_variant_with_key(variant, number_of_variants_per_shard):
  return ('%s_%011d' % (variant.reference_name,
                        variant.start / number_of_variants_per_shard *
                        number_of_variants_per_shard),
          variant)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
