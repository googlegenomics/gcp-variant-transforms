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

from concurrent.futures import TimeoutError
import enum
import logging
import os
import re
import time
from typing import List, Tuple, Union  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apitools.base.py import exceptions
from google.cloud import bigquery
from oauth2client.client import GoogleCredentials

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcfio

_VcfHeaderTypeConstants = vcf_header_io.VcfHeaderFieldTypeConstants

TABLE_SUFFIX_SEPARATOR = '__'

_BQ_DELETE_TABLE_COMMAND = 'bq rm -f -t {FULL_TABLE_ID}'
_GCS_DELETE_FILES_COMMAND = 'gsutil -m rm -f -R {ROOT_PATH}'
BQ_NUM_RETRIES = 5


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
  CALLS_SAMPLE_ID = 'sample_id'
  CALLS_GENOTYPE = 'genotype'
  CALLS_PHASESET = 'phaseset'


class TableFieldConstants(object):
  """Constants for field modes/types in the BigQuery schema."""
  TYPE_STRING = 'STRING'
  TYPE_INTEGER = 'INTEGER'
  TYPE_RECORD = 'RECORD'
  TYPE_FLOAT = 'FLOAT'
  TYPE_BOOLEAN = 'BOOLEAN'
  TYPE_TIMESTAMP = 'TIMESTAMP'
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
    TableFieldConstants.TYPE_RECORD: 'record',
    TableFieldConstants.TYPE_TIMESTAMP: 'long'
}

# A map to convert from BigQuery types to Python types.
_BIG_QUERY_TYPE_TO_PYTHON_TYPE_MAP = {
    TableFieldConstants.TYPE_INTEGER: int,
    # Bigquery accepts unicode for strings.
    TableFieldConstants.TYPE_STRING: str,
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
    raise ValueError('Expected a table reference (PROJECT:DATASET.TABLE), '
                     'got {}'.format(input_table))
  return (table_re_match.group('project'),
          table_re_match.group('dataset'),
          table_re_match.group('table'))


def raise_error_if_dataset_not_exists(client, project_id, dataset_id):
  # type: (beam_bigquery.BigqueryV2, str, str) -> None
  try:
    client.datasets.Get(beam_bigquery.BigqueryDatasetsGetRequest(
        projectId=project_id, datasetId=dataset_id))
  except exceptions.HttpError as e:
    if e.status_code == 404:
      raise ValueError('Dataset %s:%s does not exist.' %
                       (project_id, dataset_id))
    else:
      # For the rest of the errors, use BigQuery error message.
      raise


def table_exist(client, project_id, dataset_id, table_id):
  # type: (beam_bigquery.BigqueryV2, str, str, str) -> bool
  try:
    client.tables.Get(beam_bigquery.BigqueryTablesGetRequest(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id))
  except exceptions.HttpError as e:
    if e.status_code == 404:
      return False
    else:
      raise
  return True


def table_empty(project_id, dataset_id, table_id):
  client = bigquery.Client(project=project_id)
  num_rows = 'num_rows'
  query = 'SELECT count(0) AS {COL_NAME} FROM {DATASET_ID}.{TABLE_ID}'.format(
      COL_NAME=num_rows, DATASET_ID=dataset_id, TABLE_ID=table_id)
  query_job = client.query(query)
  num_retries = 0
  while True:
    try:
      results = query_job.result(timeout=300)
    except TimeoutError as e:
      logging.warning('Time out waiting for query: %s', query)
      if num_retries < BQ_NUM_RETRIES:
        num_retries += 1
        time.sleep(90)
      else:
        raise e
    else:
      if results.total_rows == 1:
        break
      else:
        logging.error('Query did not returned expected # of rows: %s', query)
        if num_retries < BQ_NUM_RETRIES:
          num_retries += 1
          time.sleep(90)
        else:
          raise ValueError('Expected 1 row in query result, got {}'.format(
              results.total_rows))

  row = list(results)[0]
  col_names = row.keys()
  if set(col_names) != {num_rows}:
    logging.error('Query `%s` did not return expected `%s` column.',
                  query, num_rows)
    raise ValueError(
        'Expected `{COL_NAME}` column is missing in the query result.'.format(
            COL_NAME=num_rows))
  return row.get(num_rows) == 0


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
    return vcfio.MISSING_FIELD_VALUE
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
  # type: (List[beam_bigquery.TableFieldSchema], str) -> None
  """Update BQ schema by combining existing one with a new one, if possible.

  If table does not exist, do not need to update the schema.
  TODO (yifangchen): Move the logic into validate().
  """
  output_table_re_match = re.match(
      r'^((?P<project>.+):)(?P<dataset>\w+)\.(?P<table>[\w\$]+)$',
      output_table)
  credentials = GoogleCredentials.get_application_default().create_scoped(
      ['https://www.googleapis.com/auth/bigquery'])
  client = beam_bigquery.BigqueryV2(credentials=credentials)
  try:
    project_id = output_table_re_match.group('project')
    dataset_id = output_table_re_match.group('dataset')
    table_id = output_table_re_match.group('table')
    existing_table = client.tables.Get(beam_bigquery.BigqueryTablesGetRequest(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_id))
  except exceptions.HttpError:
    return

  new_schema = beam_bigquery.TableSchema()
  new_schema.fields = _get_merged_field_schemas(existing_table.schema.fields,
                                                schema_fields)
  existing_table.schema = new_schema
  try:
    client.tables.Update(beam_bigquery.BigqueryTablesUpdateRequest(
        projectId=project_id,
        datasetId=dataset_id,
        table=existing_table,
        tableId=table_id))
  except exceptions.HttpError as e:
    raise RuntimeError('BigQuery schema update failed: %s' % str(e))


def _get_merged_field_schemas(
    field_schemas_1,  # type: List[beam_bigquery.TableFieldSchema]
    field_schemas_2  # type: List[beam_bigquery.TableFieldSchema]
    ):
  # type: (...) -> List[beam_bigquery.TableFieldSchema]
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
  existing_fields = {}  # type: Dict[str, beam_bigquery.TableFieldSchema]
  merged_field_schemas = []  # type: List[beam_bigquery.TableFieldSchema]
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


def compose_table_name(base_name, suffix):
  # type: (str, str) -> str
  return TABLE_SUFFIX_SEPARATOR.join([base_name, suffix])

def get_table_base_name(table_name):
  return table_name.split(TABLE_SUFFIX_SEPARATOR)[0]


def delete_table(full_table_id):
  bq_command = _BQ_DELETE_TABLE_COMMAND.format(FULL_TABLE_ID=full_table_id)
  return os.system(bq_command)


def rollback_newly_created_tables(newly_created_tables):
  for full_table_id in newly_created_tables:
    if delete_table(full_table_id) == 0:
      logging.info('Table was successfully deleted: %s', full_table_id)
    else:
      logging.error('Failed to delete table: %s', full_table_id)


def delete_gcs_files(root_path):
  gcs_command = _GCS_DELETE_FILES_COMMAND.format(ROOT_PATH=root_path)
  return os.system(gcs_command)
