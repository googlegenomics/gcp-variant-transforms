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

r"""Integration testing runner for Variant Transforms' VCF to BigQuery pipeline.

You may run this test in any project (the test files are publicly accessible).
Execute the following command from the root source directory:
python gcp_variant_transforms/testing/integration/run_tests.py \
  --project myproject \
  --staging_location gs://mybucket/staging \
  --temp_location gs://mybucket/temp \
  --logging_location gs://mybucket/temp/integration_test_logs

By default, it runs all integration tests inside
`gcp_variant_transforms/testing/integration/`.

To keep the tables that this test creates, use the --keep_tables option.

To validate old tables, i.e., skip most of the test (including table creation
and populating) and only do the validation, use --revalidation_dataset_id, e.g.,
--revalidation_dataset_id integration_tests_20180117_151528
"""

import argparse
import json
import multiprocessing
import os
import time

from datetime import datetime
# TODO(bashir2): Figure out why pylint can't find this.
# pylint: disable=no-name-in-module,import-error
from google.cloud import bigquery

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


IMAGE_NAME = 'gcr.io/gcp-variant-transforms/gcp-variant-transforms'
PIPELINE_NAME = 'gcp-variant-transforms-integration-test'
SCOPES = ['https://www.googleapis.com/auth/bigquery']
DEFAULT_ZONES = ['us-west1-b']
SCRIPT_PATH = '/opt/gcp_variant_transforms/bin/vcf_to_bq'


class TestCaseFailure(Exception):
  """Exception for failed test cases."""
  pass


class TestCase(object):
  """Test case that holds information to run in pipelines API.

  To define a new integration test case, create a json file in
  gcp_variant_transforms/testing/integration directory and specify at least
  test_name, table_name, and input_pattern for the integration test.
  """

  def __init__(self,
               context,
               test_name,
               table_name,
               input_pattern,
               validation_query,
               expected_query_result,
               **kwargs):

    self._name = test_name
    dataset_id = context.dataset_id
    self._project = context.project
    self._table_name = '{}.{}'.format(dataset_id, table_name)
    output_table = '{}:{}'.format(context.project, self._table_name)
    self._validation_query = (" ").join(validation_query)
    self._expected_query_result = expected_query_result
    args = ['--input_pattern {}'.format(input_pattern),
            '--output_table {}'.format(output_table),
            '--project {}'.format(context.project),
            '--staging_location {}'.format(context.staging_location),
            '--temp_location {}'.format(context.temp_location),
            '--job_name {}-{}'.format(test_name, dataset_id.replace('_', '-'))]
    for k, v in kwargs.iteritems():
      args.append('--{} {}'.format(k, v))

    self._pipelines_api_request = {
        'pipelineArgs': {
            'projectId': context.project,
            'logging': {'gcsPath': context.logging_location},
            'serviceAccount': {'scopes': SCOPES}
        },
        'ephemeralPipeline': {
            'projectId': context.project,
            'name': PIPELINE_NAME,
            'resources': {'zones': DEFAULT_ZONES},
            'docker': {
                'imageName': IMAGE_NAME,
                'cmd': ' '.join([SCRIPT_PATH] + args)
            },
        }
    }

  def get_name(self):
    return self._name

  def run(self, context):
    service = discovery.build(
        'genomics', 'v1alpha2', credentials=context.credentials)
    # The following pylint hint is needed because `pipelines` is a method that
    # is dynamically added to the returned `service` object above. See
    # `googleapiclient.discovery.Resource._set_service_methods`.
    # pylint: disable=no-member
    request = service.pipelines().run(body=self._pipelines_api_request)

    operation_name = request.execute()['name']
    response = self._wait_for_operation_done(service, operation_name)
    self._handle_failure(response)

  def _wait_for_operation_done(self, service, operation_name):
    """Waits until the operation `operation_name` of `service` is done."""
    time.sleep(60)
    operations = service.operations()
    request = operations.get(name=operation_name)
    response = request.execute()
    while not response['done']:
      time.sleep(10)
      response = request.execute()
    return response

  def _handle_failure(self, response):
    """Raises errors if test case failed."""
    if 'error' in response:
      if 'message' in response['error']:
        raise TestCaseFailure(response['error']['message'])
      else:
        # This case should never happen.
        raise TestCaseFailure(
            'No traceback. See logs for more information on error.')

  def validate_table(self):
    """Runs a simple query against the output table and verifies aggregates."""
    client = bigquery.Client(project=self._project)
    # TODO(bashir2): Create macros for common queries and add the option for
    # having a list of queries instead of just one.
    query = self._validation_query.format(TABLE_NAME=self._table_name)
    query_job = client.query(query)
    assert query_job.state == 'RUNNING'
    iterator = query_job.result(timeout=60)
    rows = list(iterator)
    if len(rows) != 1:
      raise TestCaseFailure('Expected one row in query result, got {}'.format(
          len(rows)))
    row = rows[0]
    if len(self._expected_query_result) != len(row):
      raise TestCaseFailure(
          'Expected {} columns in the query result, got {}'.format(
              len(self._expected_query_result), len(row)))
    for key in self._expected_query_result.keys():
      if self._expected_query_result[key] != row.get(key):
        raise TestCaseFailure(
            'Column {} mismatch: expected {}, got {}'.format(
                key, self._expected_query_result[key], row.get(key)))


class TestContextManager(object):
  """Manages all resources for a given run of tests.

  Responsible for setting up tests (i.e. creating a unique dataset) and
  cleaning up all resources after tests have been completed.
  """

  def __init__(self, args):
    self.staging_location = args.staging_location
    self.temp_location = args.temp_location
    self.logging_location = args.logging_location
    self.project = args.project
    self.credentials = GoogleCredentials.get_application_default()
    self._keep_tables = args.keep_tables
    self.revalidation_dataset_id = args.revalidation_dataset_id
    if self.revalidation_dataset_id:
      self.dataset_id = self.revalidation_dataset_id
    else:
      self.dataset_id = 'integration_tests_{}'.format(
          datetime.now().strftime('%Y%m%d_%H%M%S'))

  def __enter__(self):
    if not self.revalidation_dataset_id:
      client = bigquery.Client(project=self.project)
      dataset_ref = client.dataset(self.dataset_id)
      dataset = bigquery.Dataset(dataset_ref)
      _ = client.create_dataset(dataset)
    return self

  def __exit__(self, *args):
    if not self._keep_tables:
      client = bigquery.Client(project=self.project)
      dataset_ref = client.dataset(self.dataset_id)
      dataset = bigquery.Dataset(dataset_ref)
      tables = client.list_tables(dataset)
      # Delete tables, otherwise dataset deletion will fail because it is still
      # "in use".
      for table in tables:
        # The returned tables are of type TableListItem which has similar
        # properties like Table with the exception of a few missing ones.
        # This seems to be the easiest (but not cleanest) way of creating a
        # Table instance from a TableListItem.
        client.delete_table(bigquery.Table(table))
      client.delete_dataset(dataset)


def _get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--project', required=True)
  parser.add_argument('--staging_location', required=True)
  parser.add_argument('--temp_location', required=True)
  parser.add_argument('--logging_location', required=True)
  parser.add_argument('--keep_tables',
                      help='If set, created tables are not deleted.',
                      action='store_true')
  parser.add_argument(
      '--revalidation_dataset_id',
      help=('If set, instead of running the full test, skips '
            'most of it and only validates the tables in the '
            'given dataset. This is useful when --keep_tables '
            'is used in a previous run. Example: '
            '--revalidation_dataset_id integration_tests_20180118_014812'))
  return parser.parse_args()


def _get_test_configs():
  """Gets all test configs in integration directory and subdirectories."""
  test_configs = []
  test_file_path = os.path.join(
      os.getcwd(), 'gcp_variant_transforms/testing/integration')
  for root, _, files in os.walk(test_file_path):
    for filename in files:
      if filename.endswith('.json'):
        test_configs.append(_load_test_config(os.path.join(root, filename)))
  if not test_configs:
    raise TestCaseFailure('Found no .json files in directory {}'.format(
        test_file_path))
  return test_configs


def _load_test_config(filename):
  """Loads an integration test JSON object from a file."""
  with open(filename, 'r') as f:
    test = json.loads(f.read())
    _validate_test(test, filename)
    return test


def _validate_test(test, filename):
  required_keys = ['test_name', 'table_name', 'input_pattern',
                   'validation_query', 'expected_query_result']
  for key in required_keys:
    if key not in test:
      raise ValueError('Test case in {} is missing required key: {}'.format(
          filename, key))


def _run_test(test, context):
  if not context.revalidation_dataset_id:
    test.run(context)
  test.validate_table()


def _print_errors(results):
  """Prints results of test cases and tracebacks for any errors."""
  errors = []
  for result, test in results:
    print '{} ...'.format(test.get_name()),
    try:
      _ = result.get()
      print 'ok'
    except TestCaseFailure as e:
      print 'FAIL'
      errors.append((test.get_name(), _get_traceback(str(e))))
  for test_name, error in errors:
    print _get_failure_message(test_name, error)


def _get_traceback(message):
  traceback_index = message.find('Traceback')
  if traceback_index == -1:
    # If error contains no traceback, provide the message for some context.
    return message
  return message[traceback_index:]


def _get_failure_message(test_name, message):
  """Prints a formatted message for failed tests."""
  lines = [
      '=' * 70,
      'FAIL: {}'.format(test_name),
      '-' * 70,
      message,
  ]
  return '\n' + '\n'.join(lines) + '\n'


def main():
  test_case_configs = _get_test_configs()
  with TestContextManager(_get_args()) as context:
    pool = multiprocessing.Pool(processes=len(test_case_configs))
    results = []
    for config in test_case_configs:
      test = TestCase(context, **config)
      results.append(
          (pool.apply_async(func=_run_test, args=(test, context)), test))

    pool.close()
    pool.join()

  _print_errors(results)


if __name__ == '__main__':
  main()
