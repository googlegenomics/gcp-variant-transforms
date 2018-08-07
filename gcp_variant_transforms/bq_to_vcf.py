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
adjustable through flag --number_of_bases_per_shard), sorts them, and then
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
from typing import List  # pylint: disable=unused-import

import apache_beam as beam
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
  if not google_cloud_options.temp_location:
    raise ValueError('temp_location must be set.')

  shards_folder = 'intermediate_files_{}/'.format(
      datetime.now().strftime('%Y%m%d_%H%M%S'))
  file_path_prefix = '/'.join([google_cloud_options.temp_location,
                               shards_folder])

  _bigquery_to_shards(known_args, options, file_path_prefix)
  _compose_intermediate_files(known_args,
                              google_cloud_options.temp_location,
                              shards_folder)


def _bigquery_to_shards(known_args, options, file_path_prefix):
  # type: (argparse.Namespace, pipeline_options.PipelineOptions, str) -> None
  """Runs BigQuery to VCF shards pipelines.

  It reads the variants from BigQuery table, groups a collection of variants
  within a contiguous region of the genome (the size of the collection is
  adjustable through flag --number_of_bases_per_shard), sorts them, and then
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
         | beam.ParDo(vcfio.get_file_path_and_sorted_variants, file_path_prefix)
         | vcfio.WriteVcfDataLines())


def _compose_intermediate_files(known_args, temp_location, shards_folder):
  # type: (argparse.Namespace, str, str) -> str
  """Composes intermediate files to one VCF file.

  It composes VCF data shards to one VCF data file and deletes the intermediate
  VCF shards.
  TODO(allieychen): Eventually, it further consolidates the meta information,
  data header line, and the composed VCF data file into the `output_file`.
  """
  project, _, _ = bigquery_util.parse_table_reference(known_args.input_table)
  bucket_name, object_name = gcsio.parse_gcs_path(temp_location)
  blob_prefix = '/'.join([object_name, shards_folder])
  client = storage.Client(project)
  bucket = client.get_bucket(bucket_name)
  composed_vcf_data_file = _compose_vcf_shards_to_one(bucket, blob_prefix)
  _, file_object_name = gcsio.parse_gcs_path(known_args.output_file)
  output_file_blob = bucket.blob(file_object_name)
  output_file_blob.content_type = 'text/plain'
  output_file_blob.rewrite(composed_vcf_data_file)
  bucket.delete_blobs(bucket.list_blobs(prefix=blob_prefix))


def _pair_variant_with_key(variant, number_of_variants_per_shard):
  return ('%s_%011d' % (variant.reference_name,
                        variant.start / number_of_variants_per_shard *
                        number_of_variants_per_shard),
          variant)


def _compose_vcf_shards_to_one(bucket, blob_prefix):
  # type: (storage.Bucket, str) -> storage.Bucket
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
  new_blob_prefix = blob_prefix + 'composed_'
  for blobs_chunk in (_break_list_in_chunks(blobs_to_be_composed,
                                            _MAX_NUM_OF_BLOBS_PER_COMPOSE)):
    directory, file_name = os.path.split(blobs_chunk[0].name)
    composed_file_name = '/'.join([directory, 'composed_' + file_name])
    output_file_blob = bucket.blob(composed_file_name)
    output_file_blob.content_type = 'text/plain'
    output_file_blob.compose(blobs_chunk)
  return _compose_vcf_shards_to_one(bucket, new_blob_prefix)


def _break_list_in_chunks(blob_list, chunk_size):
  # type: (List, int) -> Iterable[List]
  """Breaks blob_list into n-size chunks."""
  for i in range(0, len(blob_list), chunk_size):
    yield blob_list[i:i + chunk_size]


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
