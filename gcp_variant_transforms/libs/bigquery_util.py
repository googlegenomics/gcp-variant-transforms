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

"""Constants and simple utility functions related to BigQuery."""

import re
from typing import Tuple  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcf_header_io

_VcfHeaderTypeConstants = vcf_header_io.VcfHeaderFieldTypeConstants


class ColumnKeyConstants(object):
  """Constants for column names in the BigQuery schema."""
  REFERENCE_NAME = 'reference_name'
  START_POSITION = 'start_position'
  END_POSITION = 'end_position'
  REFERENCE_BASES = 'reference_bases'
  ALTERNATE_BASES = 'alternate_bases'
  ALTERNATE_BASES_ALT = 'alt'
  NAMES = 'names'
  QUALITY = 'quality'
  FILTER = 'filter'
  CALLS = 'call'  # Column name is singular for consistency with Variants API.
  CALLS_NAME = 'name'
  CALLS_GENOTYPE = 'genotype'
  CALLS_PHASESET = 'phaseset'


class TableFieldConstants(object):
  """Constants for field modes/types in the BigQuery schema."""
  TYPE_STRING = 'STRING'
  TYPE_INTEGER = 'INTEGER'
  TYPE_RECORD = 'RECORD'
  TYPE_FLOAT = 'FLOAT'
  TYPE_BOOLEAN = 'BOOLEAN'
  MODE_NULLABLE = 'NULLABLE'
  MODE_REPEATED = 'REPEATED'


# A map to convert from VCF types to their equivalent BigQuery types.
_VCF_TYPE_TO_BIG_QUERY_TYPE_MAP = {
    'integer': TableFieldConstants.TYPE_INTEGER,
    'string': TableFieldConstants.TYPE_STRING,
    'character': TableFieldConstants.TYPE_STRING,
    'float': TableFieldConstants.TYPE_FLOAT,
    'flag': TableFieldConstants.TYPE_BOOLEAN,
}

# A map to convert from BigQuery types to their equivalent VCF types.
BIG_QUERY_TYPE_TO_VCF_TYPE_MAP = {
    TableFieldConstants.TYPE_INTEGER: _VcfHeaderTypeConstants.INTEGER,
    TableFieldConstants.TYPE_STRING: _VcfHeaderTypeConstants.STRING,
    TableFieldConstants.TYPE_FLOAT: _VcfHeaderTypeConstants.FLOAT,
    TableFieldConstants.TYPE_BOOLEAN: _VcfHeaderTypeConstants.FLAG
}

# A map to convert from BigQuery types to Python types.
_BIG_QUERY_TYPE_TO_PYTHON_TYPE_MAP = {
    TableFieldConstants.TYPE_INTEGER: int,
    # Bigquery accepts unicode for strings.
    TableFieldConstants.TYPE_STRING: unicode,
    TableFieldConstants.TYPE_FLOAT: float,
    TableFieldConstants.TYPE_BOOLEAN: bool,
}


def parse_table_reference(input_table):
  # type: (str) -> Tuple[str, str, str]
  """Parses a table reference.

  Args:
    input_table: a table reference in the format of PROJECT:DATASET.TABLE.

  Returns:
    A tuple (PROJECT, DATASET, TABLE).
  """
  table_re_match = re.match(
      r'^((?P<project>.+):)(?P<dataset>\w+)\.(?P<table>[\w\$]+)$', input_table)
  if not table_re_match:
    raise ValueError('Expected a table reference (PROJECT:DATASET.TABLE) ')
  return (table_re_match.group('project'),
          table_re_match.group('dataset'),
          table_re_match.group('table'))


def get_bigquery_type_from_vcf_type(vcf_type):
  # type: (str) -> str
  vcf_type = vcf_type.lower()
  if vcf_type not in _VCF_TYPE_TO_BIG_QUERY_TYPE_MAP:
    raise ValueError('Invalid VCF type: %s' % vcf_type)
  return _VCF_TYPE_TO_BIG_QUERY_TYPE_MAP[vcf_type]


def get_bigquery_mode_from_vcf_num(vcf_num):
  # type: (int) -> str
  """Returns mode (`repeated` or `nullable`) based on VCF field number."""
  if vcf_num in (0, 1):
    return TableFieldConstants.MODE_NULLABLE
  else:
    return TableFieldConstants.MODE_REPEATED


def get_python_type_from_bigquery_type(bigquery_type):
  # type: (str) -> Union[str, int, bool, float]
  if bigquery_type not in _BIG_QUERY_TYPE_TO_PYTHON_TYPE_MAP:
    raise ValueError('Invalid BigQuery type: %s' % bigquery_type)
  return _BIG_QUERY_TYPE_TO_PYTHON_TYPE_MAP[bigquery_type]
