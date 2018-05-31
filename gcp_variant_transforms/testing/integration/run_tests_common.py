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

"""Common functions and classes that are used by integration tests.

It provides common functions and classes for both run_vcf_to_bq_tests
(integration test script for vcf_to_bq pipeline) and run_preprocessor_tests
(integration test script for vcf_to_bq_preprocess pipeline).
"""

import argparse  # pylint: disable=unused-import
import json
import os
import time
from typing import Dict, List  # pylint: disable=unused-import

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

_DEFAULT_IMAGE_NAME = 'gcr.io/gcp-variant-transforms/gcp-variant-transforms'
_DEFAULT_ZONES = ['us-east1-b']


class TestCaseFailure(Exception):
  """Exception for failed test cases."""
  pass


class TestRunner(object):
  """Runs the tests using pipelines API."""

  def __init__(self, tests):
    # type: (List[Union[PreprocessorTestCase, VcfToBQTestCase]]) -> None
    self._tests = tests
    self._service = discovery.build(
        'genomics',
        'v1alpha2',
        credentials=GoogleCredentials.get_application_default())
    self._operation_names = []  # type: List[str]
    self._responses = []  # List[Dict]

  def run(self):
    """Runs all tests."""
    for test in self._tests:
      # The following pylint hint is needed because `pipelines` is a method that
      # is dynamically added to the returned `service` object above. See
      # `googleapiclient.discovery.Resource._set_service_methods`.
      # pylint: disable=no-member
      request = self._service.pipelines().run(body=test.pipeline_api_request)
      operation_name = request.execute()['name']
      self._operation_names.append(operation_name)
    self._wait_for_all_operations_done()

  def _wait_for_all_operations_done(self):
    """Waits until all operations of `_operation_names` are done."""
    # pylint: disable=no-member
    operations = self._service.operations()
    running_operation_names = set(self._operation_names)
    while running_operation_names:
      time.sleep(10)
      for operation_name in self._operation_names:
        if operation_name in running_operation_names:
          request = operations.get(name=operation_name)
          response = request.execute()
          if response['done']:
            self._handle_failure(response)
            self._responses.append(response)
            running_operation_names.remove(operation_name)

  def _handle_failure(self, response):
    """Raises errors if test case failed."""
    if 'error' in response:
      if 'message' in response['error']:
        raise TestCaseFailure(response['error']['message'])
      else:
        # This case should never happen.
        raise TestCaseFailure(
            'No traceback. See logs for more information on error.')

  def print_results(self):
    """Prints results of test cases."""
    for test in self._tests:
      print '{} ...ok'.format(test.get_name())
    return 0


def form_pipeline_api_request(project,  # type: str
                              logging_location,  # type: str
                              image,  # type: str
                              scopes,  # type: List[str]
                              pipeline_name,  # type: str
                              script_path,  # type: str
                              args  # type: List[str]
                             ):
  # type: (...) -> Dict
  return {
      'pipelineArgs': {
          'projectId': project,
          'logging': {'gcsPath': logging_location},
          'serviceAccount': {'scopes': scopes}
      },
      'ephemeralPipeline': {
          'projectId': project,
          'name': pipeline_name,
          'resources': {'zones': _DEFAULT_ZONES},
          'docker': {
              'imageName': image,
              'cmd': ' '.join([script_path] + args)
          }
      }
  }


def add_args(parser):
  # type: (argparse.ArgumentParser) -> None
  """Adds common arguments."""
  parser.add_argument('--project', required=True)
  parser.add_argument('--staging_location', required=True)
  parser.add_argument('--temp_location', required=True)
  parser.add_argument('--logging_location', required=True)
  parser.add_argument(
      '--image',
      help=('The name of the container image to run the test against it, for '
            'example: gcr.io/test-gcp-variant-transforms/'
            'test_gcp-variant-transforms_2018-01-20-13-47-12. By default the '
            'production image {} is used.').format(_DEFAULT_IMAGE_NAME),
      default=_DEFAULT_IMAGE_NAME,
      required=False)


def get_configs(test_file_path, required_keys):
  # type: (str, List[str]) -> List[Dict]
  """Gets all test configs in integration directory and subdirectories."""
  test_configs = []
  for root, _, files in os.walk(test_file_path):
    for filename in files:
      if filename.endswith('.json'):
        test_configs.append(_load_test_config(os.path.join(root, filename),
                                              required_keys))
  if not test_configs:
    raise TestCaseFailure('Found no .json files in directory {}'.format(
        test_file_path))
  return test_configs


def _load_test_config(filename, required_keys):
  # type: (str, List[str]) -> str
  """Loads an integration test JSON object from a file."""
  with open(filename, 'r') as f:
    test = json.loads(f.read())
    _validate_test(test, filename, required_keys)
    return test


def _validate_test(test, filename, required_keys):
  for key in required_keys:
    if key not in test:
      raise ValueError('Test case in {} is missing required key: {}'.format(
          filename, key))
