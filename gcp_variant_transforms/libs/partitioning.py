# Copyright 2020 Google Inc.  All Rights Reserved.
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

"""Utilities to create integer range partitioned BigQuery tables."""

from concurrent.futures import TimeoutError
import logging
import math
import os
import time

from google.cloud import bigquery

from gcp_variant_transforms.libs import bigquery_util

_GET_COLUMN_NAMES_QUERY = (
    'SELECT column_name '
    'FROM `{PROJECT_ID}`.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS '
    'WHERE table_name = "{TABLE_ID}"')
_GET_CALL_SUB_FIELDS_QUERY = (
    'SELECT field_path '
    'FROM `{PROJECT_ID}`.{DATASET_ID}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS '
    'WHERE table_name = "{TABLE_ID}" AND column_name="{CALL_COLUMN}"')
_MAIN_TABLE_ALIAS = 'main_table'
_CALL_TABLE_ALIAS = 'call_table'
_COLUMN_AS = '{TABLE_ALIAS}.{COL} AS `{COL_NAME}`'
_FLATTEN_CALL_QUERY = (
    'SELECT {SELECT_COLUMNS} '
    'FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` as {MAIN_TABLE_ALIAS}, '
    'UNNEST({CALL_COLUMN}) as {CALL_TABLE_ALIAS}')
_BQ_EXTRACT_SCHEMA_COMMAND = (
    'bq show --schema --format=prettyjson {FULL_TABLE_ID} > {SCHEMA_FILE_PATH}')

MAX_RANGE_END = pow(2, 63) - 1
_MAX_BQ_NUM_PARTITIONS = 4000
_RANGE_END_SIG_DIGITS = 4
_RANGE_INTERVAL_SIG_DIGITS = 1
_TOTAL_BASE_PAIRS_SIG_DIGITS = 4
_PARTITION_SIZE_SIG_DIGITS = 1

_BQ_CREATE_TABLE_COMMAND = (
    'bq mk --table {FULL_TABLE_ID} {SCHEMA_FILE_PATH}')
_BQ_CREATE_PARTITIONED_TABLE_COMMAND = (
    'bq mk --table --range_partitioning='
    '{PARTITION_COLUMN},0,{RANGE_END},{RANGE_INTERVAL} '
    '--clustering_fields=start_position,end_position '
    '{FULL_TABLE_ID} {SCHEMA_FILE_PATH}')

class FlattenCallColumn(object):
  """Flattens call column to convert variant opt tables to sample opt tables."""

  def __init__(self, base_table_id, suffixes):
    # type (str, List[str]) -> None
    """Initialize `FlattenCallColumn` object.

    In preparation to convert variant lookup optimized tables to sample lookup
    optimized tables, we initiate this class with the base table name of variant
    opt table (set using --output_table flag) and the list of suffixes (which
    are extracted from sharding config file).

    Args:
      base_table_id: Base name of variant opt outputs (set by --output_table).
      suffixes: List of suffixes (extracted from sharding config file).
    """
    (self._project_id,
     self._dataset_id,
     self._base_table) = bigquery_util.parse_table_reference(base_table_id)
    assert suffixes
    self._suffixes = suffixes[:]

    self._column_names = []
    self._sub_fields = []
    self._client = bigquery.Client(project=self._project_id)

    self._find_one_non_empty_table()

  def _find_one_non_empty_table(self):
    # Any non empty input table can be used as the source for schema extraction.
    for suffix in self._suffixes:
      table_id = bigquery_util.compose_table_name(self._base_table, suffix)
      if not bigquery_util.table_empty(
          self._project_id, self._dataset_id, table_id):
        self._schema_table_id = table_id
        return
    raise ValueError('All of the variant optimized tables are empty!')

  def _run_query(self, query):
    query_job = self._client.query(query)
    num_retries = 0
    while True:
      try:
        iterator = query_job.result(timeout=300)
      except TimeoutError as e:
        logging.warning('Time out waiting for query: %s', query)
        if num_retries < bigquery_util.BQ_NUM_RETRIES:
          num_retries += 1
          time.sleep(90)
        else:
          raise e
      else:
        break
    result = []
    for i in iterator:
      result.append(str(i.values()[0]))
    return result

  def _get_column_names(self):
    if not self._column_names:
      query = _GET_COLUMN_NAMES_QUERY.format(PROJECT_ID=self._project_id,
                                             DATASET_ID=self._dataset_id,
                                             TABLE_ID=self._schema_table_id)
      self._column_names = self._run_query(query)[:]
      assert self._column_names
    return self._column_names

  def _get_call_sub_fields(self):
    if not self._sub_fields:
      query = _GET_CALL_SUB_FIELDS_QUERY.format(
          PROJECT_ID=self._project_id,
          DATASET_ID=self._dataset_id,
          TABLE_ID=self._schema_table_id,
          CALL_COLUMN=bigquery_util.ColumnKeyConstants.CALLS)
      # returned list is [call, call.name, call.genotype, call.phaseset, ...]
      result = self._run_query(query)[1:]  # Drop the first element
      self._sub_fields = [sub_field.split('.')[1] for sub_field in result]
      assert self._sub_fields
    return self._sub_fields

  def _get_flatten_column_names(self):
    column_names = self._get_column_names()
    sub_fields = self._get_call_sub_fields()
    select_list = []
    for column in column_names:
      if column != bigquery_util.ColumnKeyConstants.CALLS:
        select_list.append(
            _COLUMN_AS.format(TABLE_ALIAS=_MAIN_TABLE_ALIAS, COL=column,
                              COL_NAME=column))
      else:
        for s_f in sub_fields:
          select_list.append(
              _COLUMN_AS.format(
                  TABLE_ALIAS=_CALL_TABLE_ALIAS, COL=s_f,
                  COL_NAME=bigquery_util.ColumnKeyConstants.CALLS + '_' + s_f))
    return ', '.join(select_list)

  def _copy_to_flatten_table(self, output_table_id, cp_query):
    job_config = bigquery.QueryJobConfig(destination=output_table_id)
    query_job = self._client.query(cp_query, job_config=job_config)
    num_retries = 0
    while True:
      try:
        _ = query_job.result(timeout=600)
      except TimeoutError as e:
        logging.warning('Time out waiting for query: %s', cp_query)
        if num_retries < bigquery_util.BQ_NUM_RETRIES:
          num_retries += 1
          time.sleep(90)
        else:
          logging.error('Copy to table query failed: %s', output_table_id)
          raise e
      else:
        break
    logging.info('Copy to table query was successful: %s', output_table_id)

  def _create_temp_flatten_table_with_1_row(self):
    temp_suffix = time.strftime('%Y%m%d_%H%M%S')
    temp_table_id = '{}{}'.format(self._schema_table_id, temp_suffix)
    full_output_table_id = '{}.{}.{}'.format(
        self._project_id, self._dataset_id, temp_table_id)

    select_columns = self._get_flatten_column_names()
    cp_query = _FLATTEN_CALL_QUERY.format(
        SELECT_COLUMNS=select_columns,
        PROJECT_ID=self._project_id,
        DATASET_ID=self._dataset_id,
        TABLE_ID=self._schema_table_id,
        MAIN_TABLE_ALIAS=_MAIN_TABLE_ALIAS,
        CALL_COLUMN=bigquery_util.ColumnKeyConstants.CALLS,
        CALL_TABLE_ALIAS=_CALL_TABLE_ALIAS)
    cp_query += ' LIMIT 1'  # We need this table only to extract its schema.
    self._copy_to_flatten_table(full_output_table_id, cp_query)
    logging.info('A new table with 1 row was created: %s', full_output_table_id)
    logging.info('This table is used to extract the schema of flatten table.')
    return temp_table_id

  def get_flatten_table_schema(self, schema_file_path):
    # type: (str) -> bool
    """Write the flatten table's schema to the given json file.

    This method basically performs the following tasks:
      * Composes a 'flattening query' based on _schema_table_id table's schema.
      * Runs the 'flattening query' to read 1 row and writes it to a temp table.
      * Extracts the schema of the temp table using _BQ_EXTRACT_SCHEMA_COMMAND.

    Args:
      schema_file_path: The json schema will be written to this file.

    Returns;
      A bool value indicating if the schema was successfully extracted.
    """
    temp_table_id = self._create_temp_flatten_table_with_1_row()
    full_table_id = '{}:{}.{}'.format(
        self._project_id, self._dataset_id, temp_table_id)
    bq_command = _BQ_EXTRACT_SCHEMA_COMMAND.format(
        FULL_TABLE_ID=full_table_id,
        SCHEMA_FILE_PATH=schema_file_path)
    result = os.system(bq_command)
    if result != 0:
      logging.error('Failed to extract flatten table schema using "%s" command',
                    bq_command)
    else:
      logging.info('Successfully extracted the schema of flatten table.')
    if bigquery_util.delete_table(full_table_id) == 0:
      logging.info('Successfully deleted temporary table: %s', full_table_id)
    else:
      logging.error('Was not able to delete temporary table: %s', full_table_id)
    return result == 0

  def copy_to_flatten_table(self, output_base_table_id):
    # type: (str) -> None
    """Copies data from variant lookup optimized tables to sample lookup tables.

    Copies rows from _base_table_id__* to output_base_table_id__* for each value
    in _suffixes. Here we assume destination tables are already created and are
    partitioned based on call_sample_id column. The copying process is done via
    a flattening query similar to the one used in get_flatten_table_schema().

    Note that if source tables have repeated sample_ids then output table will
    have more rows than input table. Essentially:
    Number of output rows = Number of input rows * Number of repeated sample_ids

    Args:
      output_base_table_id: Base table name of output tables.
    """
    # Here we assume all output_table_base + suffices[:] are already created.
    (output_project_id, output_dataset_id, output_base_table) = (
        bigquery_util.parse_table_reference(output_base_table_id))
    select_columns = self._get_flatten_column_names()
    for suffix in self._suffixes:
      input_table_id = bigquery_util.compose_table_name(self._base_table,
                                                        suffix)
      output_table_id = bigquery_util.compose_table_name(output_base_table,
                                                         suffix)

      full_output_table_id = '{}.{}.{}'.format(
          output_project_id, output_dataset_id, output_table_id)
      cp_query = _FLATTEN_CALL_QUERY.format(
          SELECT_COLUMNS=select_columns, PROJECT_ID=self._project_id,
          DATASET_ID=self._dataset_id, TABLE_ID=input_table_id,
          MAIN_TABLE_ALIAS=_MAIN_TABLE_ALIAS,
          CALL_COLUMN=bigquery_util.ColumnKeyConstants.CALLS,
          CALL_TABLE_ALIAS=_CALL_TABLE_ALIAS)

      self._copy_to_flatten_table(full_output_table_id, cp_query)
      logging.info('Flatten table is fully loaded: %s', full_output_table_id)



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

  range_end must be capped at MAX_RANGE_END = pow(2, 63) - 1 which is required
  by BigQuery integer range partitioning.

  Args:
    range_end: the maximum value of the column subject to partitioning

  Returns:
    A tuple (partition size, partition size * 3999).
  """
  if range_end >= MAX_RANGE_END:
    return(int(MAX_RANGE_END / float(_MAX_BQ_NUM_PARTITIONS)),
           MAX_RANGE_END)
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
  range_end_round_up = range_interval_round_up * (_MAX_BQ_NUM_PARTITIONS - 1)

  if  range_end_round_up < MAX_RANGE_END:
    return (range_interval_round_up, range_end_round_up)
  else:
    return(int(MAX_RANGE_END / float(_MAX_BQ_NUM_PARTITIONS)),
           MAX_RANGE_END)


def create_bq_table(full_table_id, schema_file_path, partition_column=None,
                    range_end=0):
  """Creates an integer range partitioned table using `bq mk table...` command.

  Since beam.io.BigQuerySink is unable to create an integer range partition
  we use `bq mk table...` to achieve this goal. Note that this command runs on
  the worker that monitors the Dataflow job.

  Args:
    full_table_id: for example: projet:dataset.table_base_name__chr1
    schema_file_path: a json file that contains the schema of the table
    partition_column: name of the column intended for integer range partitioning
    range_end: the maximum value of the column subject to partitioning
  """
  if not schema_file_path:
    raise ValueError('Missing `schema_file_path` while calling create_bq_table')
  if not partition_column and range_end != 0:
    raise ValueError(
        'When `partition_column` is set to None `range_end` must be 0')

  if partition_column:
    (range_interval, range_end_enlarged) = (
        calculate_optimal_range_interval(range_end))
    bq_command = _BQ_CREATE_PARTITIONED_TABLE_COMMAND.format(
        PARTITION_COLUMN=partition_column,
        RANGE_END=range_end_enlarged,
        RANGE_INTERVAL=range_interval,
        FULL_TABLE_ID=full_table_id,
        SCHEMA_FILE_PATH=schema_file_path)
  else:
    bq_command = _BQ_CREATE_TABLE_COMMAND.format(
        FULL_TABLE_ID=full_table_id,
        SCHEMA_FILE_PATH=schema_file_path)

  _run_table_creation_command(bq_command)


def _run_table_creation_command(bq_command):
  result = os.system(bq_command)
  if result != 0:
    time.sleep(30)  # In our integration tests sometime we overwhelm BQ server.
    result_second_attempt = os.system(bq_command)
    if result_second_attempt != 0:
      raise ValueError(
          'Failed to create a BigQuery table using "{}" command.'.format(
              bq_command))
