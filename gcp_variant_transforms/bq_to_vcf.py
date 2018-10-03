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
writes to one VCF shard. At the end, it consolidates VCF header and VCF shards
into one.

Run locally:
python -m gcp_variant_transforms.bq_to_vcf \
  --output_file <local path to VCF file> \
  --input_table projectname:bigquerydataset.tablename \
  --project projectname \
  --setup_file ./setup.py

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

from __future__ import absolute_import
from __future__ import division

import logging
import sys
import tempfile
from datetime import datetime
from typing import Dict, Iterable, List, Tuple  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam import transforms
from apache_beam.io import filesystems
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.internal.clients import bigquery as bigquery_v2
from apache_beam.options import pipeline_options
from apache_beam.runners.direct import direct_runner

from oauth2client import client

from gcp_variant_transforms import vcf_to_bq_common
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import bigquery_vcf_schema_converter
from gcp_variant_transforms.libs import genomic_region_parser
from gcp_variant_transforms.libs import vcf_file_composer
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import bigquery_to_variant
from gcp_variant_transforms.transforms import combine_call_names
from gcp_variant_transforms.transforms import densify_variants


_BASE_QUERY_TEMPLATE = 'SELECT * FROM `{INPUT_TABLE}`'
_GENOMIC_REGION_TEMPLATE = ('({REFERENCE_NAME_ID}="{REFERENCE_NAME_VALUE}" AND '
                            '{START_POSITION_ID}>={START_POSITION_VALUE} AND '
                            '{END_POSITION_ID}<={END_POSITION_VALUE})')
_COMMAND_LINE_OPTIONS = [variant_transform_options.BigQueryToVcfOptions]
_VCF_FIXED_COLUMNS = ['#CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER',
                      'INFO', 'FORMAT']


def run(argv=None):
  # type: (List[str]) -> None
  """Runs BigQuery to VCF pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  known_args, pipeline_args = vcf_to_bq_common.parse_args(argv,
                                                          _COMMAND_LINE_OPTIONS)
  options = pipeline_options.PipelineOptions(pipeline_args)
  is_direct_runner = _is_direct_runner(beam.Pipeline(options=options))
  google_cloud_options = options.view_as(pipeline_options.GoogleCloudOptions)
  if not google_cloud_options.project:
    raise ValueError('project must be set.')
  if not is_direct_runner and not known_args.output_file.startswith('gs://'):
    raise ValueError('Please set the output file {} to GCS when running with '
                     'DataflowRunner.'.format(known_args.output_file))
  if is_direct_runner:
    known_args.number_of_bases_per_shard = sys.maxsize

  temp_folder = google_cloud_options.temp_location or tempfile.mkdtemp()
  timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
  vcf_data_temp_folder = filesystems.FileSystems.join(
      temp_folder,
      'bq_to_vcf_data_temp_files_{}'.format(timestamp_str))
  # Create the directory manually. FileSystems cannot create a file if the
  # directory does not exist when using Direct Runner.
  filesystems.FileSystems.mkdirs(vcf_data_temp_folder)
  vcf_header_file_path = filesystems.FileSystems.join(
      temp_folder,
      'bq_to_vcf_header_with_call_names_{}'.format(timestamp_str))

  if not known_args.representative_header_file:
    known_args.representative_header_file = filesystems.FileSystems.join(
        temp_folder,
        'bq_to_vcf_meta_info_{}'.format(timestamp_str))
    _write_vcf_meta_info(known_args.input_table,
                         known_args.representative_header_file,
                         known_args.allow_incompatible_schema)

  _bigquery_to_vcf_shards(known_args,
                          options,
                          vcf_data_temp_folder,
                          vcf_header_file_path)
  if is_direct_runner:
    vcf_file_composer.compose_local_vcf_shards(vcf_header_file_path,
                                               vcf_data_temp_folder,
                                               known_args.output_file)
  else:
    vcf_file_composer.compose_gcs_vcf_shards(google_cloud_options.project,
                                             vcf_header_file_path,
                                             vcf_data_temp_folder,
                                             known_args.output_file)


def _write_vcf_meta_info(input_table,
                         representative_header_file,
                         allow_incompatible_schema):
  # type: (str, str, bool) -> None
  """Writes the meta information generated from BigQuery schema."""
  header_fields = (
      bigquery_vcf_schema_converter.generate_header_fields_from_schema(
          _get_schema(input_table), allow_incompatible_schema))
  write_header_fn = vcf_header_io.WriteVcfHeaderFn(representative_header_file)
  write_header_fn.process(header_fields)


def _bigquery_to_vcf_shards(
    known_args,  # type: argparse.Namespace
    beam_pipeline_options,  # type: pipeline_options.PipelineOptions
    vcf_data_temp_folder,  # type: str
    header_file_path,  # type: str
    ):
  # type: (...) -> None
  """Runs BigQuery to VCF shards pipelines.

  It reads the variants from BigQuery table, groups a collection of variants
  within a contiguous region of the genome (the size of the collection is
  adjustable through flag `--number_of_bases_per_shard`), sorts them, and then
  writes to one VCF file. All VCF data files are saved in
  `vcf_data_temp_folder`.

  Also, it writes the meta info and data header with the call names to
  `vcf_header_file_path`.
  """
  # TODO(allieychen): Modify the SQL query with the specified call_names.
  query = _get_bigquery_query(known_args)
  logging.info('Processing BigQuery query %s:', query)
  bq_source = bigquery.BigQuerySource(query=query,
                                      validate=True,
                                      use_standard_sql=True)
  schema = _get_schema(known_args.input_table)
  annotation_names = _extract_annotation_names(schema)
  with beam.Pipeline(options=beam_pipeline_options) as p:
    variants = (p
                | 'ReadFromBigQuery ' >> beam.io.Read(bq_source)
                | bigquery_to_variant.BigQueryToVariant(annotation_names))
    if known_args.call_names:
      call_names = (p
                    | transforms.Create(known_args.call_names)
                    | beam.combiners.ToList())
    else:
      call_names = (variants
                    | 'CombineCallNames' >>
                    combine_call_names.CallNamesCombiner())

    _ = (call_names
         | 'GenerateVcfDataHeader' >>
         beam.ParDo(_write_vcf_header_with_call_names,
                    _VCF_FIXED_COLUMNS,
                    known_args.representative_header_file,
                    header_file_path))

    _ = (variants
         | densify_variants.DensifyVariants(beam.pvalue.AsSingleton(call_names))
         | 'PairVariantWithKey' >>
         beam.Map(_pair_variant_with_key, known_args.number_of_bases_per_shard)
         | 'GroupVariantsByKey' >> beam.GroupByKey()
         | beam.ParDo(_get_file_path_and_sorted_variants, vcf_data_temp_folder)
         | vcfio.WriteVcfDataLines())


def _get_schema(input_table):
  # type: (str) -> bigqueryv2.TableSchema
  project_id, dataset_id, table_id = bigquery_util.parse_table_reference(
      input_table)
  credentials = (client.GoogleCredentials.get_application_default().
                 create_scoped(['https://www.googleapis.com/auth/bigquery']))
  bigquery_client = bigquery_v2.BigqueryV2(credentials=credentials)
  table = bigquery_client.tables.Get(bigquery_v2.BigqueryTablesGetRequest(
      projectId=project_id, datasetId=dataset_id, tableId=table_id))
  return table.schema


def _get_bigquery_query(known_args):
  # type: (argparse.Namespace) -> str
  """Returns a BigQuery query for the interested regions."""
  base_query = _BASE_QUERY_TEMPLATE.format(INPUT_TABLE='.'.join(
      bigquery_util.parse_table_reference(known_args.input_table)))
  conditions = []
  if known_args.genomic_regions:
    for region in known_args.genomic_regions:
      ref, start, end = genomic_region_parser.parse_genomic_region(region)
      conditions.append(_GENOMIC_REGION_TEMPLATE.format(
          REFERENCE_NAME_ID=bigquery_util.ColumnKeyConstants.REFERENCE_NAME,
          REFERENCE_NAME_VALUE=ref,
          START_POSITION_ID=bigquery_util.ColumnKeyConstants.START_POSITION,
          START_POSITION_VALUE=start,
          END_POSITION_ID=bigquery_util.ColumnKeyConstants.END_POSITION,
          END_POSITION_VALUE=end))

  if not conditions:
    return base_query
  return ' '.join([base_query, 'WHERE', ' OR '.join(conditions)])


def _extract_annotation_names(schema):
  # type: (bigquery_v2.TableSchema) -> Dict[str, List[str]]
  """Returns a mapping of annotation id to the corresponding annotation names.

  Any `Record` field within alternate bases is considered as an annotation
  field. The annotation names are useful for reconstructing the annotation str
  (e.g., 'A|upstream_gene_variant|MODIFIER|PSMF1|||||'). Sample returned map:
  {'CSQ': ['allele', 'Consequence', 'IMPACT', 'SYMBOL'],
   'CSQ_2': ['allele', 'Codons', 'STRAND']}
  """
  annotation_names = {}
  for table_field in schema.fields:
    if table_field.name == bigquery_util.ColumnKeyConstants.ALTERNATE_BASES:
      for sub_field in table_field.fields:
        if sub_field.type == bigquery_util.TableFieldConstants.TYPE_RECORD:
          annotation_names.update({
              sub_field.name: [field.name for field in sub_field.fields]
          })
  return annotation_names


def _write_vcf_header_with_call_names(sample_names,
                                      vcf_fixed_columns,
                                      representative_header_file,
                                      file_path):
  # type: (List[str], List[str], str, str) -> None
  """Writes VCF header containing meta info and header line with call names.

  It writes all meta-information starting with `##` extracted from
  `representative_header_file`, followed by one data header line with
  ` vcf_fixed_columns`, and sample names in `sample_names`. Example:
  ##INFO=<ID=CGA_SDO,Number=1,Type=Integer,Description="Number">
  ##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
  #CHROM  POS  ID  REF  ALT  QUAL  FILTER  INFO  SAMPLE1  SAMPLE2

  Args:
    sample_names: The sample names appended to `vcf_fixed_columns`.
    vcf_fixed_columns: The VCF fixed columns.
    representative_header_file: The location of the file that provides the
      meta-information.
    file_path: The location where the VCF headers is saved.
  """
  # pylint: disable=redefined-outer-name,reimported
  from apache_beam.io import filesystems
  from gcp_variant_transforms.libs import vcf_header_parser
  metadata_header_lines = vcf_header_parser.get_metadata_header_lines(
      representative_header_file)
  with filesystems.FileSystems.create(file_path) as file_to_write:
    file_to_write.write(''.join(metadata_header_lines))
    file_to_write.write(
        str('\t'.join(vcf_fixed_columns + sample_names)))
    file_to_write.write('\n')


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


# TODO(allieychen): Move this function to a general lib.
def _is_direct_runner(pipeline):
  return isinstance(pipeline.runner, direct_runner.DirectRunner)


def _pair_variant_with_key(variant, number_of_variants_per_shard):
  # type: (vcfio.Variant, int) -> Tuple[str, vcfio.Variant]
  """Pairs the variant with a key.

  If `number_of_variants_per_shard` is sys.maxsize, the variants are paired
  with the `reference_name`. Otherwise, the key is determined by the variant's
  reference name and start position.
  """
  if number_of_variants_per_shard == sys.maxsize:
    return (variant.reference_name, variant)
  return ('%s_%011d' % (variant.reference_name,
                        variant.start // number_of_variants_per_shard *
                        number_of_variants_per_shard),
          variant)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
