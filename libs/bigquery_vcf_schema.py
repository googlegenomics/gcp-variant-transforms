"""Handles generation and processing of BigQuery schema for variants."""

import sys
from apache_beam.io.gcp.internal.clients import bigquery
from beam_io.vcfio import END_INFO_KEY
from beam_io.vcfio import GENOTYPE_FORMAT_KEY
from beam_io.vcfio import MISSING_FIELD_VALUE
from beam_io.vcfio import PHASESET_FORMAT_KEY
from vcf.parser import field_counts

__all__ = ['generate_schema_from_header_fields', 'get_row_from_variant']


class _ColumnKeyConstants(object):
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


def generate_schema_from_header_fields(
    header_fields, split_alternate_allele_info_fields=True):
  """Returns a ``TableSchema`` for the BigQuery table storing variants.

  Args:
    header_fields (``libs.vcf_header_parser.HeaderFields``): A ``namedtuple``
      containing representative header fields for all ``Variant`` records. This
      specifies custom INFO and FORMAT fields in the VCF file(s).
    split_alternate_allele_info_fields (bool): If true, all INFO fields with
      `Number=A` (i.e. one value for each alternate allele) will be stored under
      the `alternate_bases` record. If false, they will be stored with the rest
      of the INFO fields.
  """
  schema = bigquery.TableSchema()
  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.REFERENCE_NAME,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description='Reference name.'))
  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.START_POSITION,
      type=_TableFieldConstants.TYPE_INTEGER,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description=('Start position (0-based). Corresponds to the first base '
                   'of the string of reference bases.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.END_POSITION,
      type=_TableFieldConstants.TYPE_INTEGER,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description=('End position (0-based). Corresponds to the first base '
                   'after the last base in the reference allele.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.REFERENCE_BASES,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description='Reference bases.'))

  # Add alternate bases.
  alternate_bases_record = bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.ALTERNATE_BASES,
      type=_TableFieldConstants.TYPE_RECORD,
      mode=_TableFieldConstants.MODE_REPEATED,
      description='One record for each alternate base (if any).')
  alternate_bases_record.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.ALTERNATE_BASES_ALT,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description='Alternate base.'))
  if split_alternate_allele_info_fields:
    for key, field in header_fields.infos.iteritems():
      if field.num == field_counts[_FIELD_COUNT_ALTERNATE_ALLELE]:
        alternate_bases_record.fields.append(bigquery.TableFieldSchema(
            name=key,
            type=_get_bigquery_type_from_vcf_type(field.type),
            mode=_TableFieldConstants.MODE_NULLABLE,
            description=field.desc))
  schema.fields.append(alternate_bases_record)

  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.NAMES,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_REPEATED,
      description='Variant names (e.g. RefSNP ID).'))
  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.QUALITY,
      type=_TableFieldConstants.TYPE_FLOAT,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description=('Phred-scaled quality score (-10log10 prob(call is wrong)). '
                   'Higher values imply better quality.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.FILTER,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_REPEATED,
      description=('List of failed filters (if any) or "PASS" indicating the '
                   'variant has passed all filters.')))

  # Add calls.
  calls_record = bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.CALLS,
      type=_TableFieldConstants.TYPE_RECORD,
      mode=_TableFieldConstants.MODE_REPEATED,
      description='One record for each call.')
  calls_record.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.CALLS_NAME,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description='Name of the call.'))
  calls_record.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.CALLS_GENOTYPE,
      type=_TableFieldConstants.TYPE_INTEGER,
      mode=_TableFieldConstants.MODE_REPEATED,
      description=('Genotype of the call. "-1" is used in cases where the '
                   'genotype is not called.')))
  calls_record.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.CALLS_PHASESET,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE,
      description=('Phaseset of the call (if any). "*" is used in cases where '
                   'the genotype is phased, but no phase set ("PS" in INFO) '
                   'was specified.')))
  for key, field in header_fields.formats.iteritems():
    # GT and PS are already included in 'genotype' and 'phaseset' fields.
    if key in (GENOTYPE_FORMAT_KEY, PHASESET_FORMAT_KEY):
      continue
    calls_record.fields.append(bigquery.TableFieldSchema(
        name=key,
        type=_get_bigquery_type_from_vcf_type(field.type),
        mode=_get_bigquery_mode_from_vcf_num(field.num),
        description=field.desc))
  schema.fields.append(calls_record)

  # Add info fields.
  for key, field in header_fields.infos.iteritems():
    # END info is already included by modifying the end_position.
    if (key == END_INFO_KEY or
        (split_alternate_allele_info_fields and
         field.num == field_counts[_FIELD_COUNT_ALTERNATE_ALLELE])):
      continue
    schema.fields.append(bigquery.TableFieldSchema(
        name=key,
        type=_get_bigquery_type_from_vcf_type(field.type),
        mode=_get_bigquery_mode_from_vcf_num(field.num),
        description=field.desc))

  return schema


def get_row_from_variant(variant, split_alternate_allele_info_fields=True):
  """Returns a BigQuery row according to the schema from the given variant.

  Args:
    variant (``Variant``): Variant to process.
    split_alternate_allele_info_fields (bool): If true, all INFO fields with
      `Number=A` (i.e. one value for each alternate allele) will be stored under
      the `alternate_bases` record. If false, they will be stored with the rest
      of the INFO fields.
  Returns:
    A dict representing BigQuery row from the given variant.
  Raises:
    ValueError: If variant data is inconsistent or invalid.
  """
  # TODO(arostami): Add error checking here for cases where the schema defined
  # by the headers does not match actual records.
  row = {
      _ColumnKeyConstants.REFERENCE_NAME: variant.reference_name,
      _ColumnKeyConstants.START_POSITION: variant.start,
      _ColumnKeyConstants.END_POSITION: variant.end,
      _ColumnKeyConstants.REFERENCE_BASES: variant.reference_bases,
      _ColumnKeyConstants.NAMES: variant.names,
      _ColumnKeyConstants.QUALITY: variant.quality,
      _ColumnKeyConstants.FILTER: variant.filters,
  }

  # Add alternate bases
  row[_ColumnKeyConstants.ALTERNATE_BASES] = []
  for alt_index, alt in enumerate(variant.alternate_bases):
    alt_record = {_ColumnKeyConstants.ALTERNATE_BASES_ALT: alt}
    if split_alternate_allele_info_fields:
      for info_key, info in variant.info.iteritems():
        if info.field_count == _FIELD_COUNT_ALTERNATE_ALLELE:
          if alt_index >= len(info.data):
            raise ValueError(
                'Invalid number of "A" fields for key %s in variant %s ' % (
                    info_key, variant))
          alt_record[info_key] = info.data[alt_index]
    row[_ColumnKeyConstants.ALTERNATE_BASES].append(alt_record)

  # Add calls.
  row[_ColumnKeyConstants.CALLS] = []
  for call in variant.calls:
    call_record = {
        _ColumnKeyConstants.CALLS_NAME: call.name,
        _ColumnKeyConstants.CALLS_PHASESET: call.phaseset,
        _ColumnKeyConstants.CALLS_GENOTYPE: [g for g in call.genotype or []]
    }
    for key, field in call.info.iteritems():
      if field is None:
        continue
      call_record[key] = _get_bigquery_sanitized_field(field)
    row[_ColumnKeyConstants.CALLS].append(call_record)

  # Add info.
  for key, info in variant.info.iteritems():
    if (info.data is None or
        (split_alternate_allele_info_fields and
         info.field_count == _FIELD_COUNT_ALTERNATE_ALLELE)):
      continue
    row[key] = _get_bigquery_sanitized_field(info.data)

  return row


def _get_bigquery_sanitized_field(
    field, null_numeric_value_replacement=-sys.maxint):
  """Returns sanitized field according to BigQuery restrictions.

  This method only sanitizes lists. It returns the same ``field`` for all other
  types (including None).

  For lists, null values are replaced with reasonable defaults since the
  BgiQuery API does not allow null values in lists (note that the entire
  list is allowed to be null). For instance, [0, None, 1] becomes
  [0, ``null_numeric_value_replacement``, 1].
  Null value replacements are:
    - `False` for bool.
    - `.` for string (null string values should not exist in Variants parsed
      using PyVCF though).
    - ``null_numeric_value_replacement`` for float/int/long.
  TODO(arostami): Expose ``null_numeric_value_replacement`` as a flag.

  Args:
    field: Field to sanitize. It can be of any type.
    null_numeric_value_replacement (int): Value to use instead of null for
      numeric (float/int/long) lists.
  Raises:
    ValueError: If a list contains unsupported values. Supported types are
      basestring, bool, int, long, and float.
  """
  if not field or not isinstance(field, list):
    return field
  null_replacement_value = None
  for i in field:
    if i is None:
      continue
    if isinstance(i, basestring):
      null_replacement_value = MISSING_FIELD_VALUE
    elif isinstance(i, bool):
      null_replacement_value = False
    elif isinstance(i, (int, long, float)):
      null_replacement_value = null_numeric_value_replacement
    else:
      raise ValueError('Unsupported value for input: %s' % str(i))
    break  # Assumption is that all fields have the same type.
  if null_replacement_value is None:  # Implies everything was None.
    return []
  return [(null_replacement_value if i is None else i) for i in field]


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
