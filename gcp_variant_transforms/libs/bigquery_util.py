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
import logging
import math
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

SAMPLE_INFO_TABLE_SUFFIX = 'sample_info'
TABLE_SUFFIX_SEPARATOR = '__'
SAMPLE_INFO_TABLE_SCHEMA_FILE_PATH = (
    'gcp_variant_transforms/data/schema/sample_info.json')

_MAX_BQ_NUM_PARTITIONS = 4000
_RANGE_END_SIG_DIGITS = 4
_RANGE_INTERVAL_SIG_DIGITS = 1

START_POSITION_COLUMN = 'start_position'
_BQ_CREATE_PARTITIONED_TABLE_COMMAND = (
    'bq mk --table --range_partitioning='
    '{PARTITION_COLUMN},0,{RANGE_END},{RANGE_INTERVAL} '
    '--clustering_fields=start_position,end_position '
    '{FULL_TABLE_ID} {SCHEMA_FILE_PATH}')
_BQ_CREATE_SAMPLE_INFO_TABLE_COMMAND = (
    'bq mk --table {FULL_TABLE_ID} {SCHEMA_FILE_PATH}')
_BQ_DELETE_TABLE_COMMAND = 'bq rm -f -t {FULL_TABLE_ID}'
_GCS_DELETE_FILES_COMMAND = 'gsutil -m rm -f -R {ROOT_PATH}'
_BQ_LOAD_JOB_NUM_RETRIES = 5
_MAX_NUM_CONCURRENT_BQ_LOAD_JOBS = 4


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
  TYPE_DATETIME = 'DATETIME'
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


def calculate_optimal_range_interval(range_end):
  # type: (int) -> Tuple[int, int]
  """Calculates the optimal range interval given range end value.

  BQ allows up to 4000 integer range partitions. This method divides
  [0, range_end] range into 3999 partitions. Every value outside of this
  range will fall into the 4000th partition. Note this partitioning method
  assumes variants are distributed uniformly.

  Since given range_end might be a lower estimate, we add a little extra
  buffer to the given value to avoid a situation where too many rows fall
  into the 4000th partition. The size of added buffer is controlled by the
  value of two consts:
    * _RANGE_END_SIG_DIGITS is set to 4 which adds [10^4, 2 * 10^4)
    * _RANGE_INTERVAL_SIG_DIGITS is set to 1 which adds [0, 10^1 * 3999)
  In total we add [10^4, 10 * 3999 + 2 * 10^4) buffer to range_end.

  Args:
    range_end: the maximum value of the column subject to partitioning

  Returns:
    A tuple (partition size, partition size * 3999).
  """
  # These two operations add [10^4, 2 * 10^4) buffer to range_end.
  range_end += math.pow(10, _RANGE_END_SIG_DIGITS)
  range_end = (
      math.ceil(range_end / math.pow(10, _RANGE_END_SIG_DIGITS)) *
      math.pow(10, _RANGE_END_SIG_DIGITS))
  # We use 4000 - 1 = 3999 partitions just to avoid hitting the BQ limits.
  range_interval = range_end / (_MAX_BQ_NUM_PARTITIONS - 1)
  # This operation adds another [0, 10 * 3999) buffer to the range_end.
  range_interval_round_up = int(
      math.ceil(range_interval / pow(10, _RANGE_INTERVAL_SIG_DIGITS)) *
      math.pow(10, _RANGE_INTERVAL_SIG_DIGITS))
  return (range_interval_round_up,
          range_interval_round_up * (_MAX_BQ_NUM_PARTITIONS - 1))


def compose_table_name(base_name, suffix):
  # type: (str, str) -> str
  return TABLE_SUFFIX_SEPARATOR.join([base_name, suffix])

def get_table_base_name(table_name):
  return table_name.split(TABLE_SUFFIX_SEPARATOR)[0]

class LoadAvro(object):
  def __init__(self,
               avro_root_path,  # type: str
               output_table,  # type: str
               suffixes  # type: List[str]
              ):
    self._avro_root_path = avro_root_path
    project_id, dataset_id, table_id = parse_table_reference(output_table)
    self._table_base_name = '{}.{}.{}'.format(project_id, dataset_id, table_id)

    self._num_load_jobs_retries = 0
    self._suffixes_to_load_jobs = {}  # type: Dict[str, bigquery.job.LoadJob]
    self._remaining_load_jobs = suffixes[:]

    self._client = bigquery.Client(project=project_id)

  def start_loading(self):
    # We run _MAX_NUM_CONCURRENT_BQ_LOAD_JOBS load jobs in parallel.
    for _ in range(_MAX_NUM_CONCURRENT_BQ_LOAD_JOBS):
      self._start_one_load_job(self._remaining_load_jobs.pop())

    self._monitor_load_jobs()

  def _start_one_load_job(self, suffix):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO)
    uri = self._avro_root_path + suffix + '-*'
    table_id = compose_table_name(self._table_base_name, suffix)
    load_job = self._client.load_table_from_uri(
        uri, table_id, job_config=job_config)
    self._suffixes_to_load_jobs.update({suffix: load_job})

  def _cancel_all_running_load_jobs(self):
    for load_job in self._suffixes_to_load_jobs.values():
      load_job.cancel()

  def _handle_failed_load_job(self, suffix, load_job):
    if self._num_load_jobs_retries < _BQ_LOAD_JOB_NUM_RETRIES:
      self._num_load_jobs_retries += 1
      # Retry the failed job after 5 minutes wait.
      time.sleep(300)
      self._start_one_load_job(suffix)
    else:
      # Jobs have failed more than _BQ_LOAD_JOB_NUM_RETRIES, cancel all jobs.
      self._cancel_all_running_load_jobs()
      table_id = compose_table_name(self._table_base_name, suffix)
      raise ValueError(
          'Failed to load AVRO to BigQuery table {} \n state: {} \n '
          'job_id: {} \n errors: {}.'.format(table_id, load_job.state,
                                             load_job.path,
                                             '\n'.join(load_job.errors)))
  def _monitor_load_jobs(self):
    # Waits until current jobs are done and then add remaining jobs one by one.
    while self._suffixes_to_load_jobs:
      time.sleep(60)
      processed_suffixes = self._suffixes_to_load_jobs.keys()
      for suffix in processed_suffixes:
        load_job = self._suffixes_to_load_jobs.get(suffix)
        if load_job.done():
          del self._suffixes_to_load_jobs[suffix]
          if load_job.state != 'DONE':
            self._handle_failed_load_job(suffix, load_job)
          else:
            if self._remaining_load_jobs:
              next_suffix = self._remaining_load_jobs.pop()
              self._start_one_load_job(next_suffix)

def _run_table_creation_command(bq_command):
  result = os.system(bq_command)
  if result != 0:
    time.sleep(30)  # In our integration tests sometime we overwhelm BQ server.
    result_second_attempt = os.system(bq_command)
    if result_second_attempt != 0:
      raise ValueError(
          'Failed to create a BigQuery table using "{}" command.'.format(
              bq_command))

def create_sample_info_table(output_table_id):
  bq_command = _BQ_CREATE_SAMPLE_INFO_TABLE_COMMAND.format(
      FULL_TABLE_ID=compose_table_name(output_table_id,
                                       SAMPLE_INFO_TABLE_SUFFIX),
      SCHEMA_FILE_PATH=SAMPLE_INFO_TABLE_SCHEMA_FILE_PATH)
  _run_table_creation_command(bq_command)

def create_output_table(full_table_id,  # type: str
                        partition_column,  # type: str
                        range_end,  # type: int
                        schema_file_path  # type: str
                       ):
  """Creates an integer range partitioned table using `bq mk table...` command.

  Since beam.io.BigQuerySink is unable to create an integer range partition
  we use `bq mk table...` to achieve this goal. Note that this command runs on
  the worker that monitors the Dataflow job.

  Args:
    full_table_id: for example: projet:dataset.table_base_name__chr1
    partition_column: name of the column intended for integer range partitioning
    range_end: the maximum value of the column subject to partitioning
    schema_file_path: a json file that contains the schema of the table
  """
  (range_interval, range_end_enlarged) = (
      calculate_optimal_range_interval(range_end))
  bq_command = _BQ_CREATE_PARTITIONED_TABLE_COMMAND.format(
      PARTITION_COLUMN=partition_column,
      RANGE_END=range_end_enlarged,
      RANGE_INTERVAL=range_interval,
      FULL_TABLE_ID=full_table_id,
      SCHEMA_FILE_PATH=schema_file_path)
  _run_table_creation_command(bq_command)


def _delete_table(full_table_id):
  bq_command = _BQ_DELETE_TABLE_COMMAND.format(FULL_TABLE_ID=full_table_id)
  return os.system(bq_command)


def rollback_newly_created_tables(append, base_table_name, suffixes=None):
  # Add sample_info table to the list of tables that need to be deleted.
  if suffixes:
    suffixes.append(SAMPLE_INFO_TABLE_SUFFIX)
  else:
    suffixes = [SAMPLE_INFO_TABLE_SUFFIX]

  if append:
    logging.warning(
        'Since tables were appended, added rows cannot be reverted. You can '
        'utilize BigQuery snapshot decorators to recover your table up to 7 '
        'days ago. For more information please refer to: '
        'https://cloud.google.com/bigquery/table-decorators '
        'Here is the list of tables that you need to manually rollback:')
    for suffix in suffixes:
      table_name = compose_table_name(base_table_name, suffix)
      logging.warning(table_name)
  else:
    logging.info('Trying to revert as much as possible...')
    for suffix in suffixes:
      table_name = compose_table_name(base_table_name, suffix)
      if _delete_table(table_name) == 0:
        logging.info('Table was successfully deleted: %s', table_name)
      else:
        logging.error('Failed to delete table: %s', table_name)


def delete_gcs_files(root_path):
  gcs_command = _GCS_DELETE_FILES_COMMAND.format(ROOT_PATH=root_path)
  return os.system(gcs_command)
