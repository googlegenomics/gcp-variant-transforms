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
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import genomic_region_parser
from gcp_variant_transforms.libs import vcf_file_composer
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import bigquery_to_variant
from gcp_variant_transforms.transforms import combine_call_names
from gcp_variant_transforms.transforms import densify_variants


_BASE_QUERY_TEMPLATE = 'SELECT * FROM `{INPUT_TABLE}`'
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
  google_cloud_options = options.view_as(pipeline_options.GoogleCloudOptions)
  # TODO(allieychen): Add support for local location.
  if not google_cloud_options.temp_location or not google_cloud_options.project:
    raise ValueError('temp_location and project must be set.')

  timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
  vcf_data_temp_folder = filesystems.FileSystems.join(
      google_cloud_options.temp_location,
      'bq_to_vcf_data_temp_files_{}'.format(timestamp_str))
  vcf_header_file_path = filesystems.FileSystems.join(
      google_cloud_options.temp_location,
      'bq_to_vcf_header_with_call_names_{}'.format(timestamp_str))

  if not known_args.representative_header_file:
    known_args.representative_header_file = filesystems.FileSystems.join(
        google_cloud_options.temp_location,
        'bq_to_vcf_meta_info_{}'.format(timestamp_str))
    _write_vcf_meta_info(known_args.representative_header_file)

  _bigquery_to_vcf_shards(known_args,
                          options,
                          vcf_data_temp_folder,
                          vcf_header_file_path)

  vcf_file_composer.compose_vcf_shards(google_cloud_options.project,
                                       vcf_header_file_path,
                                       vcf_data_temp_folder,
                                       known_args.output_file)


def _write_vcf_meta_info(representative_header_file):
  # TODO(allieychen): infer the meta information from the schema and replace the
  # empty `VcfHeader` below.
  header_fields = vcf_header_io.VcfHeader()
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
  bq_source = bigquery.BigQuerySource(query=_form_customized_query(known_args),
                                      validate=True,
                                      use_standard_sql=True)

  with beam.Pipeline(options=beam_pipeline_options) as p:
    variants = (p
                | 'ReadFromBigQuery ' >> beam.io.Read(bq_source)
                | bigquery_to_variant.BigQueryToVariant())
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


def _form_customized_query(known_args):
  # type: (argparse.Namespace) -> str
  """Returns a customized query for the interested regions."""
  base_query = _BASE_QUERY_TEMPLATE.format(INPUT_TABLE='.'.join(
      bigquery_util.parse_table_reference(known_args.input_table)))
  if not known_args.genomic_regions:
    return base_query

  conditions = []
  for region in known_args.genomic_regions:
    ref, start, end = genomic_region_parser.parse_genomic_region(region)

    conditions.append('{}=\'{}\' AND {}>={} AND {}<={}'.format(
        bigquery_util.ColumnKeyConstants.REFERENCE_NAME, ref,
        bigquery_util.ColumnKeyConstants.START_POSITION, start,
        bigquery_util.ColumnKeyConstants.END_POSITION, end))

  return ' '.join([base_query, 'WHERE', ' OR '.join(conditions)])


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


def _pair_variant_with_key(variant, number_of_variants_per_shard):
  return ('%s_%011d' % (variant.reference_name,
                        variant.start / number_of_variants_per_shard *
                        number_of_variants_per_shard),
          variant)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
