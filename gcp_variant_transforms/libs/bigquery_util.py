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

import enum
import exceptions
import re
from typing import List, Tuple, Union  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery
from apitools.base.py import exceptions
from oauth2client.client import GoogleCredentials
from vcf import parser

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcfio

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


class AvroConstants(object):
  """Constants that are relevant to Avro schema."""
  TYPE = 'type'
  NAME = 'name'
  FIELDS = 'fields'
  ARRAY = 'array'
  ITEMS = 'items'
  RECORD = 'record'
  NULL = 'null'


class _SupportedTableFieldType(enum.Enum):
  """The supported BigQuery field types.

  Only schema fields with these types are interchangeable with VCF.
  """
  TYPE_STRING = TableFieldConstants.TYPE_STRING
  TYPE_INTEGER = TableFieldConstants.TYPE_INTEGER
  TYPE_RECORD = TableFieldConstants.TYPE_RECORD
  TYPE_FLOAT = TableFieldConstants.TYPE_FLOAT
  TYPE_BOOLEAN = TableFieldConstants.TYPE_BOOLEAN


# A map to convert from VCF types to their equivalent BigQuery types.
_VCF_TYPE_TO_BIG_QUERY_TYPE_MAP = {
    'integer': TableFieldConstants.TYPE_INTEGER,
    'string': TableFieldConstants.TYPE_STRING,
    'character': TableFieldConstants.TYPE_STRING,
    'float': TableFieldConstants.TYPE_FLOAT,
    'flag': TableFieldConstants.TYPE_BOOLEAN,
}

# A map to convert from BigQuery types to their equivalent VCF types.
_BIG_QUERY_TYPE_TO_VCF_TYPE_MAP = {
    TableFieldConstants.TYPE_INTEGER: _VcfHeaderTypeConstants.INTEGER,
    TableFieldConstants.TYPE_STRING: _VcfHeaderTypeConstants.STRING,
    TableFieldConstants.TYPE_FLOAT: _VcfHeaderTypeConstants.FLOAT,
    TableFieldConstants.TYPE_BOOLEAN: _VcfHeaderTypeConstants.FLAG
}

# A map to convert from BigQuery types to their equivalent Avro types.
_BIG_QUERY_TYPE_TO_AVRO_TYPE_MAP = {
    # This list is not exhaustive but covers all of the types we currently use.
    TableFieldConstants.TYPE_INTEGER: 'long',
    TableFieldConstants.TYPE_STRING: 'string',
    TableFieldConstants.TYPE_FLOAT: 'double',
    TableFieldConstants.TYPE_BOOLEAN: 'boolean',
    TableFieldConstants.TYPE_RECORD: 'record'
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
    raise ValueError('Expected a table reference (PROJECT:DATASET.TABLE) '
                     'instead of {}'.format(input_table))
  return (table_re_match.group('project'),
          table_re_match.group('dataset'),
          table_re_match.group('table'))


def raise_error_if_dataset_not_exists(client, project_id, dataset_id):
  # type: (bigquery.BigqueryV2, str, str) -> None
  try:
    client.datasets.Get(bigquery.BigqueryDatasetsGetRequest(
        projectId=project_id, datasetId=dataset_id))
  except exceptions.HttpError as e:
    if e.status_code == 404:
      raise ValueError('Dataset %s:%s does not exist.' %
                       (project_id, dataset_id))
    else:
      # For the rest of the errors, use BigQuery error message.
      raise


def raise_error_if_table_exists(client, project_id, dataset_id, table_id):
  # type: (bigquery.BigqueryV2, str, str, str) -> None
  try:
    client.tables.Get(bigquery.BigqueryTablesGetRequest(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id))
    raise ValueError('Table %s:%s.%s already exists, cannot overwrite it.' %
                     (project_id, dataset_id, table_id))
  except exceptions.HttpError as e:
    if e.status_code == 404:
      # This is expected, output table must not already exist
      pass
    else:
      # For the rest of the errors, use BigQuery error message.
      raise


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


def get_vcf_type_from_bigquery_type(bigquery_type):
  # type: (str) -> str
  """Returns VCF type based on BigQuery type."""
  if bigquery_type not in _BIG_QUERY_TYPE_TO_VCF_TYPE_MAP:
    raise ValueError('Invalid BigQuery type: %s' % bigquery_type)
  return _BIG_QUERY_TYPE_TO_VCF_TYPE_MAP[bigquery_type]


def get_vcf_num_from_bigquery_schema(bigquery_mode, bigquery_type):
  # type: (str, str) -> int
  """Returns VCF num based on BigQuery mode and type."""
  if bigquery_mode == TableFieldConstants.MODE_REPEATED:
    return parser.field_counts[vcfio.MISSING_FIELD_VALUE]
  else:
    return 0 if bigquery_type == TableFieldConstants.TYPE_BOOLEAN else 1


def get_supported_bigquery_schema_types():
  """Returns the supported BigQuery field types."""
  return [item.value for item in _SupportedTableFieldType]


def get_avro_type_from_bigquery_type_mode(bigquery_type, bigquery_mode):
  # type: (str, str) -> Union[str, List[str, str]]
  if not bigquery_type in _BIG_QUERY_TYPE_TO_AVRO_TYPE_MAP:
    raise ValueError('Unknown Avro equivalent for type {}'.format(
        bigquery_type))
  avro_type = _BIG_QUERY_TYPE_TO_AVRO_TYPE_MAP[bigquery_type]
  if bigquery_mode == TableFieldConstants.MODE_NULLABLE:
    # A nullable type in the Avro schema is represented by a Union which is
    # equivalent to an array in JSON format.
    return [avro_type, AvroConstants.NULL]
  else:
    return avro_type

def update_bigquery_schema_on_append(schema_fields, output_table):
  # type: (List[bigquery.TableFieldSchema], str) -> None
  """Update BQ schema by combining existing one with a new one, if possible.

  If table does not exist, do not need to update the schema.
  TODO (yifangchen): Move the logic into validate().
  """
  output_table_re_match = re.match(
      r'^((?P<project>.+):)(?P<dataset>\w+)\.(?P<table>[\w\$]+)$',
      output_table)
  credentials = GoogleCredentials.get_application_default().create_scoped(
      ['https://www.googleapis.com/auth/bigquery'])
  client = bigquery.BigqueryV2(credentials=credentials)
  try:
    project_id = output_table_re_match.group('project')
    dataset_id = output_table_re_match.group('dataset')
    table_id = output_table_re_match.group('table')
    existing_table = client.tables.Get(bigquery.BigqueryTablesGetRequest(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id))
  except exceptions.HttpError:
    return

  new_schema = bigquery.TableSchema()
  new_schema.fields = _get_merged_field_schemas(existing_table.schema.fields,
                                                schema_fields)
  existing_table.schema = new_schema
  try:
    client.tables.Update(bigquery.BigqueryTablesUpdateRequest(
        projectId=project_id,
        datasetId=dataset_id,
        table=existing_table,
        tableId=table_id))
  except exceptions.HttpError as e:
    raise RuntimeError('BigQuery schema update failed: %s' % str(e))


def _get_merged_field_schemas(
    field_schemas_1,  # type: List[bigquery.TableFieldSchema]
    field_schemas_2  # type: List[bigquery.TableFieldSchema]
    ):
  # type: (...) -> List[bigquery.TableFieldSchema]
  """Merges the `field_schemas_1` and `field_schemas_2`.

  Args:
    field_schemas_1: A list of `TableFieldSchema`.
    field_schemas_2: A list of `TableFieldSchema`.

  Returns:
    A new schema with new fields from `field_schemas_2` appended to
    `field_schemas_1`.

  Raises:
    ValueError: If there are fields with the same name, but different modes or
    different types.
  """
  existing_fields = {}  # type: Dict[str, bigquery.TableFieldSchema]
  merged_field_schemas = []  # type: List[bigquery.TableFieldSchema]
  for field_schema in field_schemas_1:
    existing_fields.update({field_schema.name: field_schema})
    merged_field_schemas.append(field_schema)

  for field_schema in field_schemas_2:
    if field_schema.name not in existing_fields.keys():
      merged_field_schemas.append(field_schema)
    else:
      existing_field_schema = existing_fields.get(field_schema.name)
      if field_schema.mode != existing_field_schema.mode:
        raise ValueError(
            'The mode of field {} is not compatible. The original mode is {}, '
            'and the new mode is {}.'.format(field_schema.name,
                                             existing_field_schema.mode,
                                             field_schema.mode))
      if field_schema.type != existing_field_schema.type:
        raise ValueError(
            'The type of field {} is not compatible. The original type is {}, '
            'and the new type is {}.'.format(field_schema.name,
                                             existing_field_schema.type,
                                             field_schema.type))
      if field_schema.type == TableFieldConstants.TYPE_RECORD:
        existing_field_schema.fields = _get_merged_field_schemas(
            existing_field_schema.fields, field_schema.fields)
  return merged_field_schemas
