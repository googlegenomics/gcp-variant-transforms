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

"""Integration testing runner."""

import argparse
import json
import multiprocessing
import os

from datetime import datetime
from google.cloud import bigquery

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import time

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
               **kwargs):

    self._name = test_name
    dataset_id = context.dataset_id
    output_table = '{}:{}.{}'.format(context.project, dataset_id, table_name)
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
    request = service.pipelines().run(body=self._pipelines_api_request)

    operation_name = request.execute()['name']
    response = self._wait_for_operation_done(service, operation_name)

    self._handle_failure(response)
    return response

  def _wait_for_operation_done(self, service, operation_name):
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

  def __enter__(self):
    self.credentials = GoogleCredentials.get_application_default()
    client = bigquery.Client(project=self.project)
    self.dataset_id = 'integration_tests_{}'.format(
        datetime.now().strftime('%Y%m%d_%H%M%S'))
    dataset_ref = client.dataset(self.dataset_id)
    self.dataset = bigquery.Dataset(dataset_ref, client)
    _ = client.create_dataset(self.dataset)
    return self

  def __exit__(self, *args):
    client = bigquery.Client(project=self.project)
    tables = client.list_dataset_tables(self.dataset)
    # Delete tables, otherwise dataset deletion will fail because it is still
    # "in use".
    for table in tables:
      client.delete_table(table)
    client.delete_dataset(self.dataset)


def _get_args():
  parser = argparse.ArgumentParser()
  parser.add_argument('--project', required=True)
  parser.add_argument('--staging_location', required=True)
  parser.add_argument('--temp_location', required=True)
  parser.add_argument('--logging_location', required=True)
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
  return test_configs


def _load_test_config(filename):
  """Loads an integration test JSON object from a file."""
  with open(filename, 'r') as f:
    test = json.loads(f.read())
    _validate_test(test, filename)
    return test


def _validate_test(test, filename):
  required_keys = ['test_name', 'table_name', 'input_pattern']
  for key in required_keys:
    if key not in test:
      raise ValueError('Test case in {} is missing required key: {}'.format(
          filename, key))


def _run_test(test, context):
  return test.run(context)


def _print_errors(results):
  """Prints results of test cases and tracebacks for any errors."""
  errors = []
  for result, test in results:
    print '{} ...'.format(test.get_name()),
    try:
      error = result.get()
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
      test = TestCase(context, **test_case_config)
      results.append(
          (pool.apply_async(func=_run_test, args=(test, context)), test))

    pool.close()
    pool.join()

  _print_errors(results)


if __name__ == '__main__':
  main()
