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
import os
import sys
from datetime import datetime
from typing import Iterable, List, Tuple  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam.io import filesystems
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp import gcsio
from apache_beam.options import pipeline_options

from google.cloud import storage

from gcp_variant_transforms import vcf_to_bq_common
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import bigquery_to_variant
from gcp_variant_transforms.transforms import densify_variants

_BASE_QUERY_TEMPLATE = 'SELECT * FROM `{INPUT_TABLE}`;'
_COMMAND_LINE_OPTIONS = [variant_transform_options.BigQueryToVcfOptions]
_MAX_NUM_OF_BLOBS_PER_COMPOSE = 32


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
  _compose_temp_files(google_cloud_options.project,
                      known_args.output_file,
                      bq_to_vcf_temp_folder)


def _bigquery_to_vcf_shards(known_args, options, bq_to_vcf_temp_folder):
  # type: (argparse.Namespace, pipeline_options.PipelineOptions, str) -> None
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

  with beam.Pipeline(options=options) as p:
    _ = (p | 'ReadFromBigQuery ' >> beam.io.Read(bq_source)
         | bigquery_to_variant.BigQueryToVariant()
         | densify_variants.DensifyVariants()
         | 'PairVariantWithKey' >>
         beam.Map(_pair_variant_with_key, known_args.number_of_bases_per_shard)
         | 'GroupVariantsByKey' >> beam.GroupByKey()
         | beam.ParDo(_get_file_path_and_sorted_variants, bq_to_vcf_temp_folder)
         | vcfio.WriteVcfDataLines())


def _compose_temp_files(project, output_file, bq_to_vcf_temp_folder):
  # type: (str, str, str) -> None
  """Composes intermediate files to one VCF file.

  It composes VCF data shards to one VCF data file and deletes the intermediate
  VCF shards.
  TODO(allieychen): Eventually, it further consolidates the meta information,
  data header line, and the composed VCF data file into the `output_file`.
  TODO(allieychen): Move the composing logic into a separate library.
  """
  bucket_name, blob_prefix = gcsio.parse_gcs_path(bq_to_vcf_temp_folder)
  client = storage.Client(project)
  bucket = client.get_bucket(bucket_name)
  composed_vcf_data_file = _compose_vcf_shards_to_one(bucket, blob_prefix)
  output_file_blob = _create_blob(client, output_file)
  output_file_blob.rewrite(composed_vcf_data_file)
  bucket.delete_blobs(bucket.list_blobs(prefix=blob_prefix))


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
  file_path = '/'.join([file_path_prefix, file_name])
  yield (file_path, sorted(variants))


def _pair_variant_with_key(variant, number_of_variants_per_shard):
  return ('%s_%011d' % (variant.reference_name,
                        variant.start / number_of_variants_per_shard *
                        number_of_variants_per_shard),
          variant)


def _compose_vcf_shards_to_one(bucket, blob_prefix):
  # type: (storage.Bucket, str) -> storage.Blob
  """Composes multiple VCF shards in GCS to one.

  Note that Cloud Storage allows to compose up to 32 objects. This method
  composes the VCF files recursively until there is only one file.

  Args:
    bucket: the bucket in which the VCF files will be composed.
    blob_prefix: the prefix used to filter blobs. Only the VCF files with this
      prefix will be composed.

  Returns:
    The final blob that all VCFs composed to.
  """
  blobs_to_be_composed = list(bucket.list_blobs(prefix=blob_prefix))
  if len(blobs_to_be_composed) == 1:
    return blobs_to_be_composed[0]
  new_blob_prefix = filesystems.FileSystems.join(blob_prefix, 'composed_')
  for blobs_chunk in (_break_list_in_chunks(blobs_to_be_composed,
                                            _MAX_NUM_OF_BLOBS_PER_COMPOSE)):
    _, file_name = os.path.split(blobs_chunk[0].name)
    composed_file_name = ''.join([new_blob_prefix + file_name])
    output_file_blob = bucket.blob(composed_file_name)
    output_file_blob.content_type = 'text/plain'
    output_file_blob.compose(blobs_chunk)
  return _compose_vcf_shards_to_one(bucket, new_blob_prefix)


def _break_list_in_chunks(blob_list, chunk_size):
  # type: (List, int) -> Iterable[List]
  """Breaks blob_list into n-size chunks."""
  for i in range(0, len(blob_list), chunk_size):
    yield blob_list[i:i + chunk_size]


def _create_blob(client, file_path):
  # type: (storage.Client, str) -> storage.Blob
  bucket_name, blob_name = gcsio.parse_gcs_path(file_path)
  file_blob = client.get_bucket(bucket_name).blob(blob_name)
  file_blob.content_type = 'text/plain'
  return file_blob


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
