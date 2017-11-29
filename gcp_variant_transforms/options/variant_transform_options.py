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

import re

from apache_beam.io.gcp.internal.clients import bigquery
from apitools.base.py import exceptions
from oauth2client.client import GoogleCredentials


__all__ = ['VariantTransformsOptions', 'VcfReadOptions', 'BigQueryWriteOptions',
           'FilterOptions', 'MergeOptions']


class VariantTransformsOptions(object):
  """Base class for defining groups of options for Variant Transforms.

  Transforms should create a derived class of ``VariantTransformsOptions``
  and override the ``add_arguments`` and ``validate`` methods. ``add_arguments``
  should create an argument group for the transform and add all necessary
  command line arguments necessary for the transform. ``validate`` should
  validate the resulting arguments after they are parsed.
  """

  def add_arguments(self, parser):
    """Adds all options of this transform to parser."""
    raise NotImplementedError

  def validate(self, parsed_args):
    """Validates this group's options parsed from the command line."""
    pass


class VcfReadOptions(VariantTransformsOptions):
  """Options for reading VCF files."""

  def add_arguments(self, parser):
    """Adds all options of this transform to parser."""
    parser.add_argument('--input_pattern',
                        required=True,
                        help='Input pattern for VCF files to process.')
    parser.add_argument(
        '--allow_malformed_records',
        type='bool', default=False, nargs='?', const=True,
        help=('If true, failed VCF record reads will not raise errors. '
              'Failed reads will be logged as warnings and returned as '
              'MalformedVcfRecord objects.'))
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


class BigQueryWriteOptions(VariantTransformsOptions):
  """Options for writing Variant records to BigQuery."""

  def add_arguments(self, parser):
    parser.add_argument('--output_table',
                        required=True,
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

  def validate(self, parsed_args, client=None):
    output_table_re_match = re.match(
        r'^((?P<project>.+):)(?P<dataset>\w+)\.(?P<table>[\w\$]+)$',
        parsed_args.output_table)
    if not output_table_re_match:
      raise ValueError(
          'Expected a table reference (PROJECT:DATASET.TABLE) '
          'instead of {}.'.format(parsed_args.output_table))
    try:
      if not client:
        credentials = GoogleCredentials.get_application_default().create_scoped(
            ['https://www.googleapis.com/auth/bigquery'])
        client = bigquery.BigqueryV2(credentials=credentials)
      client.datasets.Get(bigquery.BigqueryDatasetsGetRequest(
          projectId=output_table_re_match.group('project'),
          datasetId=output_table_re_match.group('dataset')))
    except exceptions.HttpError as e:
      if e.status_code == 404:
        raise ValueError('Dataset %s:%s does not exist.' %
                         (output_table_re_match.group('project'),
                          output_table_re_match.group('dataset')))
      else:
        # For the rest of the errors, use BigQuery error message.
        raise


class FilterOptions(VariantTransformsOptions):
  """Options for filtering Variant records."""

  def add_arguments(self, parser):
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
  # - MERGE_WITH_NON_VARIANTS: uses
  #   libs.variant_merge.merge_with_non_varaitns_strategy for merging. Please
  #   see the documentation in that file for details.
  VARIANT_MERGE_STRATEGIES = [
      NONE,
      MOVE_TO_CALLS,
      MERGE_WITH_NON_VARIANTS,
  ]

  def add_arguments(self, parser):
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
