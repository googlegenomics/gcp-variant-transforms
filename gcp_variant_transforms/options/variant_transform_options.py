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

from __future__ import absolute_import

import argparse  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery
from oauth2client.client import GoogleCredentials

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_sanitizer
from gcp_variant_transforms.libs import bigquery_util


class VariantTransformsOptions(object):
  """Base class for defining groups of options for Variant Transforms.

  Transforms should create a derived class of ``VariantTransformsOptions``
  and override the ``add_arguments`` and ``validate`` methods. ``add_arguments``
  should create an argument group for the transform and add all necessary
  command line arguments necessary for the transform. ``validate`` should
  validate the resulting arguments after they are parsed.
  """

  def add_arguments(self, parser):
    # type: (argparse.ArgumentParser) -> None
    """Adds all options of this transform to parser."""
    raise NotImplementedError

  def validate(self, parsed_args):
    # type: (argparse.Namespace) -> None
    """Validates this group's options parsed from the command line."""
    pass


class VcfReadOptions(VariantTransformsOptions):
  """Options for reading VCF files."""

  def add_arguments(self, parser):
    """Adds all options of this transform to parser."""
    parser.add_argument(
        '--input_pattern',
        help=('Input pattern for VCF files to process. Either'
              'this or --input_file flag has to be provided, exclusively.'))
    parser.add_argument(
        '--input_file',
        help=('File that contains the list of VCF file names to input. Either '
              'this or --input_pattern flag has to be provided, exclusively.'
              'Note that using input_file rather than input_pattern is slower '
              'for inputs that contain less than 50k files.'))
    parser.add_argument(
        '--allow_malformed_records',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, failed VCF record reads will not raise errors. '
              'Failed reads will be logged as warnings and returned as '
              'MalformedVcfRecord objects.'))
    parser.add_argument(
        '--allow_incompatible_records',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, VCF records incompatible with BigQuery schema will not '
              'raise errors, and instead are casted to the schema.'))
    parser.add_argument(
        '--optimize_for_large_inputs',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, the pipeline runs in optimized way for handling large '
              'inputs. Set this to true if you are loading more than 50,000 '
              'files.'))
    parser.add_argument(
        '--representative_header_file',
        default='',
        help=('If provided, header values from the provided file will be used '
              'as representative for all files matching input_pattern. In '
              'particular, this will be used to generate the BigQuery schema. '
              'If not provided, header values from all files matching '
              'input_pattern will be merged by key. Only one value will be '
              'chosen (in no particular order) in cases where multiple files '
              'use the same key. Providing this file improves performance if a '
              'large number of files are specified by input_pattern. '
              'Note that each VCF file must still contain valid header files '
              'even if this is provided.'))
    parser.add_argument(
        '--infer_headers',
        '--infer_undefined_headers',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, header fields (e.g. FORMAT, INFO) are not only '
              'extracted from header section of VCF files, but also from '
              'variants. This is useful when there are header fields in '
              'variants not defined in the header sections, or the definition '
              'of the the header fields do not match the field values. Note: '
              'setting this flag or `--infer_annotation_types` incurs a '
              'performance penalty of an extra pass over all variants.'))
    parser.add_argument(
        '--vcf_parser',
        default=vcfio.VcfParserType.PYVCF.name,
        choices=[parser.name for parser in vcfio.VcfParserType],
        help=('Choose the underlying parser for reading VCF files. Currently '
              'we only support `{}` (default) and `{}`. Note: Nucleus parser '
              'is still in experimental stage so using it for production jobs '
              'is not recommended.'.format(vcfio.VcfParserType.PYVCF.name,
                                           vcfio.VcfParserType.NUCLEUS.name)))

  def validate(self, parsed_args):
    # type: (argparse.Namespace) -> None
    if parsed_args.infer_headers and parsed_args.representative_header_file:
      raise ValueError('Both --infer_headers and --representative_header_file '
                       'are passed! Please double check and choose at most one '
                       'of them.')
    _validate_inputs(parsed_args)


class AvroWriteOptions(VariantTransformsOptions):
  """Options for writing Variant records to Avro files."""

  def add_arguments(self, parser):
    # type: (argparse.ArgumentParser) -> None
    parser.add_argument('--output_avro_path',
                        default='',
                        help='The output path to write Avro files under.')

  def validate(self, parsed_args):
    # type: (argparse.Namespace) -> None
    if not parsed_args.output_table and not parsed_args.output_avro_path:
      raise ValueError('At least one of --output_table or --output_avro_path '
                       'options should be provided.')


class BigQueryWriteOptions(VariantTransformsOptions):
  """Options for writing Variant records to BigQuery."""

  def add_arguments(self, parser):
    # type: (argparse.ArgumentParser) -> None
    parser.add_argument('--output_table',
                        default='',
                        help='BigQuery table to store the results.')
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
              'overwritten. New records will be appended to those that '
              'already exist.'))
    parser.add_argument(
        '--update_schema_on_append',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, BigQuery schema will be updated by combining the '
              'existing schema and the new schema if they are compatible. '
              'Requires append=True.'))
    parser.add_argument(
        '--omit_empty_sample_calls',
        type='bool', default=False, nargs='?', const=True,
        help=("If true, samples that don't have a given call will be omitted."))
    parser.add_argument(
        '--num_bigquery_write_shards',
        type=int, default=1,
        help=('Before writing the final result to output BigQuery, the data is '
              'sharded to avoid a known failure for very large inputs (issue '
              '#199). Setting this flag to 1 will avoid this extra sharding.'
              'It is recommended to use 20 for loading large inputs without '
              'merging. Use a smaller value (2 or 3) if both merging and '
              'optimize_for_large_inputs are enabled.'))
    parser.add_argument(
        '--null_numeric_value_replacement',
        type=int,
        default=bigquery_sanitizer._DEFAULT_NULL_NUMERIC_VALUE_REPLACEMENT,
        help=('Value to use instead of null for numeric (float/int/long) lists.'
              'For instance, [0, None, 1] will become '
              '[0, `null_numeric_value_replacement`, 1].'))

  def validate(self, parsed_args, client=None):
    # type: (argparse.Namespace, bigquery.BigqueryV2) -> None
    if not parsed_args.output_table and parsed_args.output_avro_path:
      # Writing into BigQuery is not requested; no more BigQuery checks needed.
      return

    project_id, dataset_id, table_id = bigquery_util.parse_table_reference(
        parsed_args.output_table)

    if not client:
      credentials = GoogleCredentials.get_application_default().create_scoped(
          ['https://www.googleapis.com/auth/bigquery'])
      client = bigquery.BigqueryV2(credentials=credentials)

    bigquery_util.raise_error_if_dataset_not_exists(client, project_id,
                                                    dataset_id)
    # Ensuring given output table doesn't already exist to avoid overwriting it.
    if not parsed_args.append:
      if parsed_args.update_schema_on_append:
        raise ValueError('--update_schema_on_append requires --append to be '
                         'true.')
      bigquery_util.raise_error_if_table_exists(client,
                                                project_id,
                                                dataset_id,
                                                table_id)


class AnnotationOptions(VariantTransformsOptions):
  """Options for how to treat annotation fields."""

  _RUN_FLAG = 'run_annotation_pipeline'
  _OUTPUT_DIR_FLAG = 'annotation_output_dir'
  _VEP_IMAGE_FLAG = 'vep_image_uri'
  _VEP_CACHE_FLAG = 'vep_cache_path'

  def add_arguments(self, parser):
    # type: (argparse.ArgumentParser) -> None
    parser.add_argument(
        '--annotation_fields',
        default=None, nargs='+',
        help=('If set, it is a list of field names (separated by space) that '
              'should be treated as annotation fields. The content of these '
              'INFO fields will be broken into multiple columns in the output '
              'BigQuery table and stored as repeated fields with '
              'corresponding alternate alleles. [EXPERIMENTAL]'))
    parser.add_argument(
        '--use_allele_num',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, uses the "ALLELE_NUM" annotation to determine the ALT'
              'that matches each annotation set. Note this is the preferred way'
              'of ALT matching and should be used if available. In particular, '
              'if the annotation tool was VEP and it was used with --minimal, '
              'this option is preferred over --minimal_vep_alt_matching '
              'because it avoids ambiguity. When --use_allele_num is set '
              '--minimal_vep_alt_matching is ignored.'))
    parser.add_argument(
        '--minimal_vep_alt_matching',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, for ALT matching of annotation fields, the --minimal '
              'mode of VEP is simulated. Note that this can lead to ambiguous '
              'matches so by default this is False but if the VCF files are '
              'generated with VEP in --minimal mode, then this option should '
              'be turned on. The ambiguous cases are logged and counted.'
              'See the "Complex VCF Entries" of this doc for details:'
              'http://www.ensembl.org/info/docs/tools/vep/online/'
              'VEP_web_documentation.pdf'))
    parser.add_argument(
        '--' + AnnotationOptions._RUN_FLAG,
        type='bool', default=False, nargs='?', const=True,
        help=('If true, runs annotation tools (currently only VEP) on input '
              'VCFs before loading to BigQuery.'))
    parser.add_argument(
        '--shard_variants',
        type='bool', default=True, nargs='?', const=True,
        help=('By default, the input files are sharded into smaller temporary '
              'VCF files before running VEP annotation. If the input files are '
              'small, i.e., each VCF file contains less than 50,000 variants, '
              'setting this flag can be computationally wasteful.'))
    parser.add_argument(
        '--' + AnnotationOptions._OUTPUT_DIR_FLAG,
        default='',
        help=('The path on Google Cloud Storage to store annotated outputs. '
              'The output files are VCF and follow the same directory '
              'structure as input files with a suffix added to them. Note that '
              'this is expected not to exist and will be created in the '
              'process of running VEP pipelines.'))
    parser.add_argument(
        '--' + AnnotationOptions._VEP_IMAGE_FLAG,
        default='gcr.io/cloud-lifesciences/vep_91',
        help=('The URI of the docker image for VEP.'))
    parser.add_argument(
        '--' + AnnotationOptions._VEP_CACHE_FLAG,
        default='',
        help=('The path for VEP cache on Google Cloud Storage. By default, '
              'this will be set to gs://cloud-lifesciences/vep/'
              'vep_cache_homo_sapiens_GRCh38_91.tar.gz, assuming neither the '
              '`--vep_species` nor the `--vep_assembly` flags have been set. '
              'For convenience, if either of those flags are provided, this '
              'path will be automatically updated to reflect the new cache, '
              'given values are a species and/or assembly we maintain. For '
              'example, `--vep_assembly GRCh37` is satisfactory for specifying '
              'our gs://cloud-lifesciences/vep/'
              'vep_cache_homo_sapiens_GRCh37_91.tar.gz cache.'))
    parser.add_argument(
        '--vep_info_field',
        default='CSQ_VT',
        help=('The name of the new INFO field for annotations.'))
    parser.add_argument(
        '--vep_num_fork',
        type=int, default=2,
        help=('Number of local processes to use when running vep for a single '
              'file. The default is chosen to be 2 because even on a single '
              'core machine using two processes should help interleaving I/O '
              'vs CPU bound work.'))
    parser.add_argument(
        '--vep_assembly',
        default='GRCh38',
        help=('Genome assembly name to pass to vep. Setting this flag will be '
              'reflected in the cache file used if --{} is not set.').format(
                  AnnotationOptions._VEP_CACHE_FLAG))
    parser.add_argument(
        '--vep_species',
        default='homo_sapiens',
        help=('Species name to pass to vep. Setting this flag will be '
              'reflected in the cache file used if --{} is not set.').format(
                  AnnotationOptions._VEP_CACHE_FLAG))
    parser.add_argument(
        '--infer_annotation_types',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, all annotation fields will have BigQuery schema type '
              'information inferred from the contents of the variants. By '
              'default, annotation fields are STRING. Note: setting this flag '
              'or `infer_headers` incurs a performance penalty of an extra '
              'pass over all variants. Additionally, this flag will resolve '
              'conflicts for all headers as if `allow_incompatible_types` was '
              'true.'))
    parser.add_argument(
        '--run_with_garbage_collection',
        type='bool', default=True, nargs='?', const=True,
        help=('If set, in case of failure or cancellation, the VMs running '
              'VEP annotation will be cleaned up automatically.'))
    parser.add_argument(
        '--number_of_variants_per_shard',
        type=int, default=20000,
        help=('The maximum number of variants written to each shard if '
              '`shard_variants` is true. The default value should work '
              'for most cases. You may change this flag to a smaller value if '
              'you have a dataset with a lot of samples. Notice that the '
              'pipeline may take longer to finish for smaller value of this '
              'flag.'))

  def validate(self, parsed_args):
    # type: (argparse.Namespace) -> None
    args_dict = vars(parsed_args)
    if args_dict[AnnotationOptions._RUN_FLAG]:
      output_dir = args_dict[AnnotationOptions._OUTPUT_DIR_FLAG]  # type: str
      if not output_dir or not output_dir.startswith('gs://'):
        raise ValueError('Flag {} should start with gs://, got {}'.format(
            AnnotationOptions._OUTPUT_DIR_FLAG, output_dir))
      vep_image = args_dict[AnnotationOptions._VEP_IMAGE_FLAG]  # type: str
      if not vep_image:
        raise ValueError('Flag {} is not set.'.format(
            AnnotationOptions._VEP_IMAGE_FLAG))
      vep_cache = args_dict[AnnotationOptions._VEP_CACHE_FLAG]  # type: str
      if vep_cache and not vep_cache.startswith('gs://'):
        raise ValueError('Flag {} should start with gs://, got {}'.format(
            AnnotationOptions._VEP_CACHE_FLAG, vep_cache))


class FilterOptions(VariantTransformsOptions):
  """Options for filtering Variant records."""

  def add_arguments(self, parser):
    # type: (argparse.ArgumentParser) -> None
    parser.add_argument(
        '--reference_names',
        default=None, nargs='+',
        help=('A list of reference names (separated by a space) to load '
              'to BigQuery. If this parameter is not specified, all '
              'references will be kept.'))


class MergeOptions(VariantTransformsOptions):
  """Options for merging Variant records."""

  NONE = 'NONE'
  MOVE_TO_CALLS = 'MOVE_TO_CALLS'
  MERGE_WITH_NON_VARIANTS = 'MERGE_WITH_NON_VARIANTS'
  # List of supported merge strategies for variants.
  # - NONE: Variants will not be merged across files.
  # - MOVE_TO_CALLS: uses libs.variant_merge.move_to_calls_strategy
  #   for merging. Please see the documentation in that file for details.
  VARIANT_MERGE_STRATEGIES = [
      NONE,
      MOVE_TO_CALLS,
  ]

  def add_arguments(self, parser):
    # type: (argparse.ArgumentParser) -> None
    parser.add_argument(
        '--variant_merge_strategy',
        default='NONE',
        choices=MergeOptions.VARIANT_MERGE_STRATEGIES,
        help=('Variant merge strategy to use. Set to NONE if variants should '
              'not be merged across files.'))
    # Configs for MOVE_TO_CALLS strategy.
    parser.add_argument(
        '--info_keys_to_move_to_calls_regex',
        default='',
        help=('Regular expression specifying the INFO keys to move to the '
              'associated calls in each VCF file. '
              'Requires variant_merge_strategy={}|{}.'.format(
                  MergeOptions.MOVE_TO_CALLS,
                  MergeOptions.MERGE_WITH_NON_VARIANTS)))
    parser.add_argument(
        '--copy_quality_to_calls',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, the QUAL field for each record will be copied to '
              'the associated calls in each VCF file. '
              'Requires variant_merge_strategy={}|{}.'.format(
                  MergeOptions.MOVE_TO_CALLS,
                  MergeOptions.MERGE_WITH_NON_VARIANTS)))
    parser.add_argument(
        '--copy_filter_to_calls',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, the FILTER field for each record will be copied to '
              'the associated calls in each VCF file. '
              'Requires variant_merge_strategy={}|{}.'.format(
                  MergeOptions.MOVE_TO_CALLS,
                  MergeOptions.MERGE_WITH_NON_VARIANTS)))

  def validate(self, parsed_args):
    # type: (argparse.Namespace) -> None
    if (parsed_args.variant_merge_strategy !=
        MergeOptions.MOVE_TO_CALLS and
        parsed_args.variant_merge_strategy !=
        MergeOptions.MERGE_WITH_NON_VARIANTS):
      if parsed_args.info_keys_to_move_to_calls_regex:
        raise ValueError(
            '--info_keys_to_move_to_calls_regex requires '
            '--variant_merge_strategy {}|{}'.format(
                MergeOptions.MOVE_TO_CALLS,
                MergeOptions.MERGE_WITH_NON_VARIANTS))
      if parsed_args.copy_quality_to_calls:
        raise ValueError(
            '--copy_quality_to_calls requires '
            '--variant_merge_strategy {}|{}'.format(
                MergeOptions.MOVE_TO_CALLS,
                MergeOptions.MERGE_WITH_NON_VARIANTS))
      if parsed_args.copy_filter_to_calls:
        raise ValueError(
            '--copy_filter_to_calls requires '
            '--variant_merge_strategy {}|{}'.format(
                MergeOptions.MOVE_TO_CALLS,
                MergeOptions.MERGE_WITH_NON_VARIANTS))


class PreprocessOptions(VariantTransformsOptions):
  """Options for preprocess."""

  def add_arguments(self, parser):
    # type: (argparse.ArgumentParser) -> None
    parser.add_argument(
        '--input_pattern',
        help='Input pattern for VCF files to process. Either'
             'this or --input_file flag has to be provided, exclusively.')
    parser.add_argument(
        '--input_file',
        help=('File that contains the list of VCF file names to input. Either '
              'this or --input_pattern flag has to be provided, exlusively. '
              'Note that using input_file than input_pattern is slower for '
              'inputs that contain less than 50k files.'))
    parser.add_argument(
        '--report_all_conflicts',
        type='bool', default=False, nargs='?', const=True,
        help=('By default, only the incompatible VCF headers will be reported. '
              'If true, it also reports the undefined headers and malformed '
              'records.'))
    parser.add_argument(
        '--report_path',
        required=True,
        help=('The full path of the preprocessor report. If run locally, a '
              'local path must be provided. Otherwise, a cloud path is '
              'required. For best readability, please save the report as a tab '
              'delimited file (by appending the extension .tsv to the file '
              'name) and open with the spreadsheets.'))
    parser.add_argument(
        '--resolved_headers_path',
        default='',
        help=('The full path of the resolved headers. The file will not be'
              'generated if unspecified. Otherwise, please provide a local '
              'path if run locally, or a cloud path if run on Dataflow.'))
    parser.add_argument(
        '--estimate_disk_usage',
        type='bool', default=False, nargs='?', const=True,
        help=('By default, disk resource usage will not be estimated.'
              'If true, the preprocessor will estimate the maximum disk usage '
              'consumed at any step in the pipeline, which could lead to '
              'out-of-disk errors at a shuffle step e.g. MergeVariants.'))

  def validate(self, parsed_args):
    _validate_inputs(parsed_args)


class PartitionOptions(VariantTransformsOptions):
  """Options for partitioning Variant records."""

  def add_arguments(self, parser):
    parser.add_argument(
        '--partition_config_path',
        default='',
        help=('File containing list of partitions and output table names. You '
              'can use provided default partition_config file to split output '
              'by chromosome (one table per chromosome) which is located at: '
              'gcp_variant_transforms/data/partition_configs/'
              'homo_sapiens_default.yaml'))


class BigQueryToVcfOptions(VariantTransformsOptions):
  """Options for BigQuery to VCF pipelines."""

  def add_arguments(self, parser):
    # type: (argparse.ArgumentParser) -> None
    parser.add_argument(
        '--output_file',
        required=True,
        help=('The full path of the output VCF file. This can be a local path '
              'if you use `DirectRunner` (for very small VCF files) but must '
              'be a path in Google Cloud Storage if using `DataflowRunner`.'))
    parser.add_argument(
        '--input_table',
        required=True,
        help=('BigQuery table that will be loaded to VCF. It must be in the '
              'format of (PROJECT:DATASET.TABLE).'))
    parser.add_argument(
        '--number_of_bases_per_shard',
        type=int, default=1000000,
        help=('The maximum number of base pairs per chromosome to include in a '
              'shard. A shard is a collection of data within a contiguous '
              'region of the genome that can be efficiently sorted in memory. '
              'You may change this flag if you have a dataset that is very '
              'dense and variants in each shard cannot be sorted in memory.'))
    parser.add_argument(
        '--representative_header_file',
        help=('If provided, meta-information from the provided file (e.g., '
              'INFO, FORMAT, FILTER, etc) will be added into the '
              '`output_file`. Otherwise, the meta-information is inferred from '
              'the BigQuery schema on a "best effort" basis (e.g., any '
              'repeated INFO field will have `Number=.`). It is recommended to '
              'provide this file to specify the most accurate and complete '
              'meta-information in the VCF file.'))
    parser.add_argument(
        '--genomic_regions',
        default=None, nargs='+',
        help=('A list of genomic regions (separated by a space) to load from '
              'BigQuery. The format of each genomic region should be '
              'REFERENCE_NAME:START_POSITION-END_POSITION or REFERENCE_NAME if '
              'the full chromosome is requested. Only variants matching at '
              'least one of these regions will be loaded. For example, '
              '`--genomic_regions chr1 chr2:1000-2000` will load all variants '
              'in `chr1` and all variants in `chr2` with `start_position` in '
              '`[1000,2000)` from BigQuery. If this flag is not specified, all '
              'variants will be loaded.'))
    parser.add_argument(
        '--call_names',
        default=None, nargs='+',
        help=('A list of call names (separated by a space). Only variants for '
              'these calls will be loaded from BigQuery. If this parameter is '
              'not specified, all calls will be loaded.'))
    parser.add_argument(
        '--allow_incompatible_schema',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, the incompatibilities between BigQuery schema and the '
              'reserved fields based on VCF 4.3 spec (see '
              'http://samtools.github.io/hts-specs/VCFv4.3.pdf for more '
              'details) will not raise errors. Instead, the VCF '
              'meta-information are inferred from the schema without '
              'validation.'))
    parser.add_argument(
        '--preserve_call_names_order',
        type='bool', default=False, nargs='?', const=True,
        help=('By default, call names in the output VCF file are generated in '
              'ascending order. If set to true, the order of call names will '
              'be the same as the BigQuery table, but it requires all '
              'extracted variants to have the same call name ordering (usually '
              'true for tables from single VCF file import).'))


def _validate_inputs(parsed_args):
  if ((parsed_args.input_pattern and parsed_args.input_file) or
      (not parsed_args.input_pattern and not parsed_args.input_file)):
    raise ValueError('Exactly one of input_pattern and input_file has to be '
                     'provided.')


class ExperimentalOptions(VariantTransformsOptions):
  """Options for experimental features."""

  def add_arguments(self, parser):
    # type: (argparse.ArgumentParser) -> None
    parser.add_argument(
        '--auto_flags_experiment',
        default=False,
        help=('Set flag values automatically based on heuristics extracted '
              'from input files'))
