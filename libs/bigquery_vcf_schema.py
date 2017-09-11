"""Handles generation and processing of BigQuery schema for variants."""

from apache_beam.io.gcp.internal.clients import bigquery

__all__ = ['generate_schema', 'get_row_from_variant']


class _ColumnKeyConstants(object):
  """Constants for column names in the BigQuery schema."""
  REFERENCE_NAME = 'reference_name'
  START_POSITION = 'start_position'
  END_POSITION = 'end_position'
  REFERENCE_BASES = 'reference_bases'
  ALTERNATE_BASES = 'alternate_bases'
  ALTERNATE_BASES_ALT = 'alt'


class _TableFieldConstants(object):
  """Constants for field modes/types in the BigQuery schema."""
  TYPE_STRING = 'string'
  TYPE_INTEGER = 'integer'
  TYPE_RECORD = 'record'
  TYPE_FLOAT = 'float'
  TYPE_BOOLEAN = 'boolean'
  MODE_NULLABLE = 'nullable'
  MODE_REPEATED = 'repeated'


def generate_schema():
  """Returns a ``TableSchema`` for the BigQuery table storing variants."""
  # TODO(arostami): Generate schema dynamically based on VCF headers.
  schema = bigquery.TableSchema()
  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.REFERENCE_NAME,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE))
  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.START_POSITION,
      type=_TableFieldConstants.TYPE_INTEGER,
      mode=_TableFieldConstants.MODE_NULLABLE))
  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.END_POSITION,
      type=_TableFieldConstants.TYPE_INTEGER,
      mode=_TableFieldConstants.MODE_NULLABLE))
  schema.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.REFERENCE_BASES,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE))
  alternate_bases_record = bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.ALTERNATE_BASES,
      type=_TableFieldConstants.TYPE_RECORD,
      mode=_TableFieldConstants.MODE_REPEATED)
  alternate_bases_record.fields.append(bigquery.TableFieldSchema(
      name=_ColumnKeyConstants.ALTERNATE_BASES_ALT,
      type=_TableFieldConstants.TYPE_STRING,
      mode=_TableFieldConstants.MODE_NULLABLE))
  schema.fields.append(alternate_bases_record)
  return schema


def get_row_from_variant(variant):
  """Returns a BigQuery row according to the schema from the given variant.

  Args:
    variant (``Variant``): Variant to process.
  Returns:
    A dict representing BigQuery row from the given variant.
  """
  row = {
      _ColumnKeyConstants.REFERENCE_NAME: variant.reference_name,
      _ColumnKeyConstants.START_POSITION: variant.start,
      _ColumnKeyConstants.END_POSITION: variant.end,
      _ColumnKeyConstants.REFERENCE_BASES: variant.reference_bases
  }

  # Add alternate bases
  row[_ColumnKeyConstants.ALTERNATE_BASES] = []
  for alt in variant.alternate_bases:
    alt_record = {_ColumnKeyConstants.ALTERNATE_BASES_ALT: alt}
    row[_ColumnKeyConstants.ALTERNATE_BASES].append(alt_record)
  return row
