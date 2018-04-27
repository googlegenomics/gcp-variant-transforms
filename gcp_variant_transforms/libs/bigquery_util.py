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
import math
import sys

from gcp_variant_transforms.beam_io import vcfio


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
  TYPE_STRING = 'string'
  TYPE_INTEGER = 'integer'
  TYPE_RECORD = 'record'
  TYPE_FLOAT = 'float'
  TYPE_BOOLEAN = 'boolean'
  MODE_NULLABLE = 'nullable'
  MODE_REPEATED = 'repeated'


# A map to convert from VCF types to their equivalent BigQuery types.
_VCF_TYPE_TO_BIG_QUERY_TYPE_MAP = {
    'integer': TableFieldConstants.TYPE_INTEGER,
    'string': TableFieldConstants.TYPE_STRING,
    'character': TableFieldConstants.TYPE_STRING,
    'float': TableFieldConstants.TYPE_FLOAT,
    'flag': TableFieldConstants.TYPE_BOOLEAN,
}

# A map to convert from BigQuery types to Python types.
_BIG_QUERY_TYPE_TO_PYTHON_TYPE_MAP = {
    TableFieldConstants.TYPE_INTEGER: int,
    # Bigquery accepts unicode for strings.
    TableFieldConstants.TYPE_STRING: unicode,
    TableFieldConstants.TYPE_FLOAT: float,
    TableFieldConstants.TYPE_BOOLEAN: bool,
}

# Prefix to use when the first character of the field name is not [a-zA-Z]
# as required by BigQuery.
_FALLBACK_FIELD_NAME_PREFIX = 'field_'


def get_bigquery_sanitized_field_name(field_name):
  # type: (str) -> str
  """Returns the sanitized field name according to BigQuery restrictions.

  BigQuery field names must follow `[a-zA-Z][a-zA-Z0-9_]*`. This method converts
  any unsupported characters to an underscore. Also, if the first character does
  not match `[a-zA-Z]`, it prepends ``_FALLBACK_FIELD_NAME_PREFIX`` to the name.

  Args:
    field_name (str): Name of the field to sanitize.
  Returns:
    Sanitized field name with unsupported characters replaced with an
    underscore. It also prepends the name with ``_FALLBACK_FIELD_NAME_PREFIX``
    if the first character does not match `[a-zA-Z]`.
  """
  assert field_name  # field_name must not be empty by this stage.
  if not re.match('[a-zA-Z]', field_name[0]):
    field_name = _FALLBACK_FIELD_NAME_PREFIX + field_name
  return re.sub('[^a-zA-Z0-9_]', '_', field_name)


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


def get_bigquery_sanitized_field(
    field, null_numeric_value_replacement=-sys.maxint):
  """Returns sanitized field according to BigQuery restrictions.

  This method only sanitizes lists and strings. It returns the same
  ``field`` for all other types (including None).

  For lists, null values are replaced with reasonable defaults since the
  BgiQuery API does not allow null values in lists (note that the entire
  list is allowed to be null). For instance, [0, None, 1] becomes
  [0, ``null_numeric_value_replacement``, 1].
  Null value replacements are:
    - `False` for bool.
    - `.` for string (null string values should not exist in Variants parsed
      using PyVCF though).
    - ``null_numeric_value_replacement`` for float/int/long.
  TODO: Expose ``null_numeric_value_replacement`` as a flag.

  For strings, it returns its unicode representation. The BigQuery API does not
  support strings that are UTF-8 encoded.

  Args:
    field: Field to sanitize. It can be of any type.
    null_numeric_value_replacement (int): Value to use instead of null for
      numeric (float/int/long) lists.
  Raises:
    ValueError: If the field could not be sanitized (e.g. unsupported types in
      lists).
  """
  if not field:
    return field
  if isinstance(field, basestring):
    return _get_bigquery_sanitized_string(field)
  elif isinstance(field, float):
    return _get_bigquery_sanitized_float(field)
  elif isinstance(field, list):
    return _get_bigquery_sanitized_list(field, null_numeric_value_replacement)
  else:
    return field


def _get_bigquery_sanitized_list(input_list, null_numeric_value_replacement):
  """Returns sanitized list according to BigQuery restrictions.

  Null values are replaced with reasonable defaults since the
  BgiQuery API does not allow null values in lists (note that the entire
  list is allowed to be null). For instance, [0, None, 1] becomes
  [0, ``null_numeric_value_replacement``, 1].
  Null value replacements are:
    - `False` for bool.
    - `.` for string (null string values should not exist in Variants parsed
      using PyVCF though).
    - ``null_numeric_value_replacement`` for float/int/long.
  Lists that contain strings are also sanitized according to the
  ``_get_bigquery_sanitized_string`` method.

  Args:
    input_list: List to sanitize.
    null_numeric_value_replacement (int): Value to use instead of null for
      numeric (float/int/long) lists.
  Raises:
    ValueError: If a list contains unsupported values. Supported types are
      basestring, bool, int, long, and float.
  """
  null_replacement_value = None
  for i in input_list:
    if i is None:
      continue
    if isinstance(i, basestring):
      null_replacement_value = vcfio.MISSING_FIELD_VALUE
    elif isinstance(i, bool):
      null_replacement_value = False
    elif isinstance(i, (int, long, float)):
      null_replacement_value = null_numeric_value_replacement
    else:
      raise ValueError('Unsupported value for input: %s' % str(i))
    break  # Assumption is that all fields have the same type.
  if null_replacement_value is None:  # Implies everything was None.
    return []
  sanitized_list = []
  for i in input_list:
    if i is None:
      i = null_replacement_value
    elif isinstance(i, basestring):
      i = _get_bigquery_sanitized_string(i)
    elif isinstance(i, float):
      sanitized_float = _get_bigquery_sanitized_float(i)
      i = (sanitized_float if sanitized_float is not None
           else null_replacement_value)
    sanitized_list.append(i)
  return sanitized_list


def _get_bigquery_sanitized_float(input_float):
  """Returns a sanitized float for BigQuery.

  This method replaces INF with sys.maxint, -INF with -sys.maxint, and NaN
  with None. It returns the same value for all other values.
  """
  if input_float == float('inf'):
    return sys.maxint
  elif input_float == float('-inf'):
    return -sys.maxint
  elif math.isnan(input_float):
    return None
  else:
    return input_float


def _get_bigquery_sanitized_string(input_str):
  """Returns a unicode string as BigQuery API does not support UTF-8 strings."""
  try:
    return (input_str if isinstance(input_str, unicode)
            else input_str.decode('utf-8'))
  except UnicodeDecodeError:
    raise ValueError('input_str is not UTF-8: %s ' % (input_str))
