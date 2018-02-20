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

"""Handles generation and processing of BigQuery schema for variants."""

from __future__ import absolute_import

import copy
import json
import math
import re
import sys
from typing import Dict, Any  #pylint: disable=unused-import

import vcf

from apache_beam.io.gcp.internal.clients import bigquery
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import processed_variant


__all__ = ['generate_schema_from_header_fields', 'get_rows_from_variant',
           'ColumnKeyConstants']


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


class _TableFieldConstants(object):
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
    'integer': _TableFieldConstants.TYPE_INTEGER,
    'string': _TableFieldConstants.TYPE_STRING,
    'character': _TableFieldConstants.TYPE_STRING,
    'float': _TableFieldConstants.TYPE_FLOAT,
    'flag': _TableFieldConstants.TYPE_BOOLEAN,
}
_FIELD_COUNT_ALTERNATE_ALLELE = 'A'
# Prefix to use when the first character of the field name is not [a-zA-Z]
# as required by BigQuery.
_FALLBACK_FIELD_NAME_PREFIX = 'field_'
# Maximum size of a BigQuery row is 10MB. See
# https://cloud.google.com/bigquery/quotas#import for details.
# We set it to 10MB - 10KB to leave a bit of room for error in case jsonifying
# the object is not exactly the same in different libraries.
_MAX_BIGQUERY_ROW_SIZE_BYTES = 10 * 1024 * 1024 - 10 * 1024
# Number of bytes to add to the object size when concatenating calls (i.e.
# to account for ", "). We use 5 bytes to be conservative.
_JSON_CONCATENATION_OVERHEAD_BYTES = 5


def generate_schema_from_header_fields(header_fields, variant_merger=None,
                                       split_alternate_allele_info_fields=True,
                                       annotation_fields=None):
  """Returns a ``TableSchema`` for the BigQuery table storing variants.

  Args:
    header_fields (``HeaderFields``): A ``namedtuple`` containing representative
      header fields for all ``Variant`` records. This specifies custom INFO and
      FORMAT fields in the VCF file(s).
    variant_merger (``VariantMergeStrategy``): The strategy used for merging
      variants (if any). Some strategies may change the schema, which is why
      this may be needed here.
    split_alternate_allele_info_fields (bool): If true, all INFO fields with
      `Number=A` (i.e. one value for each alternate allele) will be stored under
      the `alternate_bases` record. If false, they will be stored with the rest
      of the INFO fields.
    annotation_fields (List[str]): If provided, it is the list of annotation
      INFO fields.
  """
  schema = bigquery.TableSchema()
  schema.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.REFERENCE_NAME,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description='Reference name.'))
  schema.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.START_POSITION,
      type=_TableFieldConstants.TYPE_INTEGER,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description=('Start position (0-based). Corresponds to the first base '
                   'of the string of reference bases.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.END_POSITION,
      type=_TableFieldConstants.TYPE_INTEGER,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description=('End position (0-based). Corresponds to the first base '
                   'after the last base in the reference allele.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.REFERENCE_BASES,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description='Reference bases.'))

  # Add alternate bases.
  alternate_bases_record = bigquery.TableFieldSchema(
      name=ColumnKeyConstants.ALTERNATE_BASES,
      type=_TableFieldConstants.TYPE_RECORD,
      mode=_TableFieldConstants.MODE_REPEATED,
      description='One record for each alternate base (if any).')
  alternate_bases_record.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.ALTERNATE_BASES_ALT,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description='Alternate base.'))
  if split_alternate_allele_info_fields:
    for key, field in header_fields.infos.iteritems():
      if field.num == vcf.parser.field_counts[_FIELD_COUNT_ALTERNATE_ALLELE]:
        alternate_bases_record.fields.append(bigquery.TableFieldSchema(
            name=_get_bigquery_sanitized_field_name(key),
            type=_get_bigquery_type_from_vcf_type(field.type),
            mode=_TableFieldConstants.MODE_NULLABLE,
            description=_get_bigquery_sanitized_field(field.desc)))

  for annot_field in annotation_fields or []:
    if not annot_field in header_fields.infos:
      raise ValueError('Annotation field {} not found'.format(annot_field))
    annotation_names = processed_variant.extract_annotation_names(
        header_fields.infos[annot_field].desc)
    annotation_record = bigquery.TableFieldSchema(
        name=_get_bigquery_sanitized_field(annot_field),
        type=_TableFieldConstants.TYPE_RECORD,
        mode=_TableFieldConstants.MODE_REPEATED,
        description='List of {} annotations for this alternate.'.format(
            annot_field))
    for annotation_name in annotation_names:
      annotation_record.fields.append(bigquery.TableFieldSchema(
          name=_get_bigquery_sanitized_field(annotation_name),
          type=_TableFieldConstants.TYPE_STRING,
          mode=_TableFieldConstants.MODE_NULLABLE,
          # TODO(bashir2): Add descriptions of well known annotations, e.g.,
          # from VEP.
          description=''))
    alternate_bases_record.fields.append(annotation_record)
  schema.fields.append(alternate_bases_record)

  schema.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.NAMES,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_REPEATED,
      description='Variant names (e.g. RefSNP ID).'))
  schema.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.QUALITY,
      type=_TableFieldConstants.TYPE_FLOAT,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description=('Phred-scaled quality score (-10log10 prob(call is wrong)). '
                   'Higher values imply better quality.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.FILTER,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_REPEATED,
      description=('List of failed filters (if any) or "PASS" indicating the '
                   'variant has passed all filters.')))

  # Add calls.
  calls_record = bigquery.TableFieldSchema(
      name=ColumnKeyConstants.CALLS,
      type=_TableFieldConstants.TYPE_RECORD,
      mode=_TableFieldConstants.MODE_REPEATED,
      description='One record for each call.')
  calls_record.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.CALLS_NAME,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description='Name of the call.'))
  calls_record.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.CALLS_GENOTYPE,
      type=_TableFieldConstants.TYPE_INTEGER,
      mode=_TableFieldConstants.MODE_REPEATED,
      description=('Genotype of the call. "-1" is used in cases where the '
                   'genotype is not called.')))
  calls_record.fields.append(bigquery.TableFieldSchema(
      name=ColumnKeyConstants.CALLS_PHASESET,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description=('Phaseset of the call (if any). "*" is used in cases where '
                   'the genotype is phased, but no phase set ("PS" in FORMAT) '
                   'was specified.')))
  for key, field in header_fields.formats.iteritems():
    # GT and PS are already included in 'genotype' and 'phaseset' fields.
    if key in (vcfio.GENOTYPE_FORMAT_KEY, vcfio.PHASESET_FORMAT_KEY):
      continue
    calls_record.fields.append(bigquery.TableFieldSchema(
        name=_get_bigquery_sanitized_field_name(key),
        type=_get_bigquery_type_from_vcf_type(field.type),
        mode=_get_bigquery_mode_from_vcf_num(field.num),
        description=_get_bigquery_sanitized_field(field.desc)))
  schema.fields.append(calls_record)

  # Add info fields.
  info_keys = set()
  annotation_fields_set = set(annotation_fields or [])
  for key, field in header_fields.infos.iteritems():
    # END info is already included by modifying the end_position.
    if (key == vcfio.END_INFO_KEY or
        (split_alternate_allele_info_fields and
         field.num == vcf.parser.field_counts[_FIELD_COUNT_ALTERNATE_ALLELE]) or
        key in annotation_fields_set):
      continue
    schema.fields.append(bigquery.TableFieldSchema(
        name=_get_bigquery_sanitized_field_name(key),
        type=_get_bigquery_type_from_vcf_type(field.type),
        mode=_get_bigquery_mode_from_vcf_num(field.num),
        description=_get_bigquery_sanitized_field(field.desc)))
    info_keys.add(key)
  if variant_merger:
    variant_merger.modify_bigquery_schema(schema, info_keys)
  return schema


# TODO: refactor this to use a class instead.
def get_rows_from_variant(variant, omit_empty_sample_calls=False):
  # type: (processed_variant.ProcessedVariant, bool) -> Dict
  """Yields BigQuery rows according to the schema from the given variant.

  There is a 10MB limit for each BigQuery row, which can be exceeded by having
  a large number of calls. This method may split up a row into multiple rows if
  it exceeds 10MB.

  Args:
    variant (``ProcessedVariant``): Variant to convert to a row.
    omit_empty_sample_calls (bool): If true, samples that don't have a given
      call will be omitted.
  Yields:
    A dict representing a BigQuery row from the given variant. The row may have
    a subset of the calls if it exceeds the maximum allowed BigQuery row size.
  Raises:
    ValueError: If variant data is inconsistent or invalid.
  """
  # TODO: Add error checking here for cases where the schema defined
  # by the headers does not match actual records.
  base_row = _get_base_row_from_variant(variant)
  base_row_size_in_bytes = _get_json_object_size(base_row)
  row_size_in_bytes = base_row_size_in_bytes
  row = copy.deepcopy(base_row)  # Keep base_row intact.
  for call in variant.calls:
    call_record, empty = _get_call_record(call)
    if omit_empty_sample_calls and empty:
      continue

    # Add a few bytes to account for surrounding characters when concatenating.
    call_record_size_in_bytes = (
        _get_json_object_size(call_record) + _JSON_CONCATENATION_OVERHEAD_BYTES)
    if (row_size_in_bytes + call_record_size_in_bytes >=
        _MAX_BIGQUERY_ROW_SIZE_BYTES):
      yield row
      row = copy.deepcopy(base_row)
      row_size_in_bytes = base_row_size_in_bytes
    row[ColumnKeyConstants.CALLS].append(call_record)
    row_size_in_bytes += call_record_size_in_bytes
  yield row


def _get_call_record(call):
  """A helper method for ``get_rows_from_variant`` to get a call as JSON.

  Args:
     call (``VariantCall``): Variant call to convert.

  Returns:
    BigQuery call value (dict).
  """
  call_record = {
      ColumnKeyConstants.CALLS_NAME: _get_bigquery_sanitized_field(call.name),
      ColumnKeyConstants.CALLS_PHASESET: call.phaseset,
      ColumnKeyConstants.CALLS_GENOTYPE: call.genotype or []
  }
  is_empty = (not call.genotype or
              set(call.genotype) == set((vcfio.MISSING_GENOTYPE_VALUE,)))
  for key, field in call.info.iteritems():
    if field is not None:
      sanitized = _get_bigquery_sanitized_field(field)
      call_record[_get_bigquery_sanitized_field_name(key)] = sanitized
      is_empty = is_empty and _is_empty_field(sanitized)
  return call_record, is_empty


def _get_base_row_from_variant(variant):
  # type: (processed_variant.ProcessedVariant) -> Dict[str, Any]
  """A helper method for ``get_rows_from_variant`` to get row without calls."""
  row = {
      ColumnKeyConstants.REFERENCE_NAME: variant.reference_name,
      ColumnKeyConstants.START_POSITION: variant.start,
      ColumnKeyConstants.END_POSITION: variant.end,
      ColumnKeyConstants.REFERENCE_BASES: variant.reference_bases
  }  # type: Dict[str, Any]
  if variant.names:
    row[ColumnKeyConstants.NAMES] = _get_bigquery_sanitized_field(
        variant.names)
  if variant.quality is not None:
    row[ColumnKeyConstants.QUALITY] = variant.quality
  if variant.filters:
    row[ColumnKeyConstants.FILTER] = _get_bigquery_sanitized_field(
        variant.filters)
  # Add alternate bases.
  row[ColumnKeyConstants.ALTERNATE_BASES] = []
  for alt in variant.alternate_data_list:
    alt_record = {ColumnKeyConstants.ALTERNATE_BASES_ALT:
                  alt.alternate_bases}
    for key, data in alt.info.iteritems():
      alt_record[_get_bigquery_sanitized_field_name(key)] = data
    row[ColumnKeyConstants.ALTERNATE_BASES].append(alt_record)
  # Add info.
  for key, data in variant.non_alt_info.iteritems():
    if data is not None:
      row[_get_bigquery_sanitized_field_name(key)] = (
          _get_bigquery_sanitized_field(data))
  # Set calls to empty for now (will be filled later).
  row[ColumnKeyConstants.CALLS] = []
  return row


def _get_bigquery_sanitized_field_name(field_name):
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


def _get_bigquery_sanitized_field(
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


def _get_bigquery_type_from_vcf_type(vcf_type):
  vcf_type = vcf_type.lower()
  if vcf_type not in _VCF_TYPE_TO_BIG_QUERY_TYPE_MAP:
    raise ValueError('Invalid VCF type: %s' % vcf_type)
  return _VCF_TYPE_TO_BIG_QUERY_TYPE_MAP[vcf_type]


def _get_bigquery_mode_from_vcf_num(vcf_num):
  if vcf_num in (0, 1):
    return _TableFieldConstants.MODE_NULLABLE
  else:
    return _TableFieldConstants.MODE_REPEATED


def _is_alternate_allele_count(info_field):
  return info_field.field_count == _FIELD_COUNT_ALTERNATE_ALLELE


def _is_empty_field(value):
  return (value in (vcfio.MISSING_FIELD_VALUE, [vcfio.MISSING_FIELD_VALUE]) or
          (not value and value != 0))


def _get_json_object_size(obj):
  return len(json.dumps(obj))
