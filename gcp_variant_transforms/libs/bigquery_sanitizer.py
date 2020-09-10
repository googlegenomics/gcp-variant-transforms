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

"""Sanitizes BigQuery schema and field according to BigQuery restrictions."""

import math
import re
import sys
from typing import List, Optional  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcfio

# Prefix to use when the first character of the field name is not [a-zA-Z]
# as required by BigQuery.
_FALLBACK_FIELD_NAME_PREFIX = 'field_'

# A big number to represent infinite float values. The division by 10 is to
# prevent unintentional overflows when doing subsequent operations.
_INF_FLOAT_VALUE = sys.float_info.max / 10
_DEFAULT_NULL_NUMERIC_VALUE_REPLACEMENT = -2 ^ 31


class SchemaSanitizer():
  """Class to sanitize BigQuery schema according to BigQuery restrictions."""

  @staticmethod
  def get_sanitized_string(input_str):
    # type: (str) -> unicode
    """Returns a unicode as BigQuery API does not support UTF-8 strings."""
    return _decode_utf8_string(input_str)

  @staticmethod
  def get_sanitized_field_name(field_name):
    # type: (str) -> str
    """Returns the sanitized field name according to BigQuery restrictions.

    BigQuery field names must follow `[a-zA-Z][a-zA-Z0-9_]*`. This method
    converts any unsupported characters to an underscore. Also, if the first
    character does not match `[a-zA-Z]`, it prepends
    `_FALLBACK_FIELD_NAME_PREFIX` to the name.

    Args:
      field_name: Name of the field to sanitize.
    Returns:
      Sanitized field name with unsupported characters replaced with an
        underscore. It also prepends the name with `_FALLBACK_FIELD_NAME_PREFIX`
        if the first character does not match `[a-zA-Z]`.
    """
    assert field_name  # field_name must not be empty by this stage.
    if not re.match('[a-zA-Z]', field_name[0]):
      field_name = _FALLBACK_FIELD_NAME_PREFIX + field_name
    return re.sub('[^a-zA-Z0-9_]', '_', field_name)


class FieldSanitizer():
  """Class to sanitize field values according to BigQuery restrictions."""

  def __init__(self, null_numeric_value_replacement):
    # type: (Optional[int]) -> None
    """Initializes a `BigQueryFieldSanitizer`.

    Args:
      null_numeric_value_replacement: Value to use instead of null for
        numeric (float/int/long) lists. For instance, [0, None, 1] will become
        [0, `null_numeric_value_replacement`, 1].
    """
    self._null_numeric_value_replacement = (
        null_numeric_value_replacement or
        _DEFAULT_NULL_NUMERIC_VALUE_REPLACEMENT)

  def get_sanitized_field(self, field):
    # type: (Any) ->  Any
    """Returns sanitized field according to BigQuery restrictions.

    This method only sanitizes lists and strings. It returns the same `field`
    for all other types (including None).

    For lists, null values are replaced with reasonable defaults since the
    BigQuery API does not allow null values in lists (note that the entire
    list is allowed to be null). For instance, [0, None, 1] becomes
    [0, `null_numeric_value_replacement`, 1].
    Null value replacements are:
      - `False` for bool.
      - `.` for string.
      - `null_numeric_value_replacement` for float/int/long.

    For strings, it returns its unicode representation. The BigQuery API does
    not support strings that are UTF-8 encoded.

    Args:
      field: Field to sanitize. It can be of any type.

    Raises:
      ValueError: If the field could not be sanitized (e.g. unsupported types in
        lists).
    """
    if not field:
      return field
    if isinstance(field, (str, bytes)):
      return self._get_sanitized_string(field)
    elif isinstance(field, float):
      return self._get_sanitized_float(field)
    elif isinstance(field, list):
      return self._get_sanitized_list(field)
    else:
      return field

  def _get_sanitized_list(self, input_list):
    # type: (List) -> List
    """Returns sanitized list according to BigQuery restrictions.

    Null values are replaced with reasonable defaults since the
    BigQuery API does not allow null values in lists (note that the entire
    list is allowed to be null). For instance, [0, None, 1] becomes
    [0, `null_numeric_value_replacement`, 1].
    Null value replacements are:
      - `False` for bool.
      - `.` for string.
      - `null_numeric_value_replacement` for float/int/long.
    Lists that contain strings are also sanitized according to the
    `_get_sanitized_string` method.

    Args:
      input_list: List to sanitize.

    Raises:
      ValueError: If a list contains unsupported values. Supported types are
        basestring, bool, int, long, and float.
    """
    null_replacement_value = None
    for i in input_list:
      if i is None:
        continue
      if isinstance(i, str):
        null_replacement_value = vcfio.MISSING_FIELD_VALUE
      elif isinstance(i, bool):
        null_replacement_value = False
      elif isinstance(i, (int, float)):
        null_replacement_value = self._null_numeric_value_replacement
      else:
        raise ValueError('Unsupported value for input: %s' % str(i))
      break  # Assumption is that all fields have the same type.
    if null_replacement_value is None:  # Implies everything was None.
      return []
    sanitized_list = []
    for i in input_list:
      if i is None:
        i = null_replacement_value
      elif isinstance(i, (str, bytes)):
        i = self._get_sanitized_string(i)
      elif isinstance(i, float):
        sanitized_float = self._get_sanitized_float(i)
        i = (sanitized_float if sanitized_float is not None
             else null_replacement_value)
      sanitized_list.append(i)
    return sanitized_list

  def _get_sanitized_float(self, input_float):
    """Returns a sanitized float for BigQuery.

    This method replaces INF and -INF with positive and negative numbers with
    huge absolute values, and replaces NaN with None. It returns the same value
    for all other values.
    """
    if input_float == float('inf'):
      return _INF_FLOAT_VALUE
    elif input_float == float('-inf'):
      return -_INF_FLOAT_VALUE
    elif math.isnan(input_float):
      return None
    else:
      return input_float

  def _get_sanitized_string(self, input_str):
    # type: (Any) -> str
    """Returns a unicode as BigQuery API does not support UTF-8 strings."""
    return _decode_utf8_string(input_str)


def _decode_utf8_string(input_str):
  # type: (Any) -> str
  try:
    return (input_str if isinstance(input_str, str)
            else input_str.decode('utf-8'))
  except UnicodeDecodeError as e:
    raise ValueError('input_str is not UTF-8: %s ' % (input_str)) from e
