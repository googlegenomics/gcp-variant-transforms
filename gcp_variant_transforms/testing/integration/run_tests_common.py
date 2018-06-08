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
from typing import Dict, List, Optional, Set, Union  # pylint: disable=unused-import

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
    # type: (List[List[Union[PreprocessorTestCase, VcfToBQTestCase]]]) -> None
    self._tests = tests
    self._service = discovery.build(
        'genomics',
        'v1alpha2',
        credentials=GoogleCredentials.get_application_default())
    self._operation_names_to_tests = {}  # type: Dict
    self._test_names_to_dependent_tests = {}  # type: Dict
    self._running_operation_names = set()  # type: Set[str]
    for test_cases in tests:
      if len(test_cases) > 2:
        raise NotImplementedError
      if len(test_cases) == 2:
        self._test_names_to_dependent_tests.update(
            {test_cases[0].get_name(): test_cases[1]})

  def run(self):
    """Runs all tests."""
    for test_cases in self._tests:
      self._run_test(test_cases[0])
    self._wait_for_all_operations_done()

  def _run_test(self, test_case):
    # type: (Union[PreprocessorTestCase, VcfToBQTestCase]) -> None
    # The following pylint hint is needed because `pipelines` is a method that
    # is dynamically added to the returned `service` object above. See
    # `googleapiclient.discovery.Resource._set_service_methods`.
    # pylint: disable=no-member
    request = self._service.pipelines().run(body=test_case.pipeline_api_request)
    operation_name = request.execute()['name']
    self._operation_names_to_tests.update({operation_name: test_case})
    self._running_operation_names.add(operation_name)

  def _wait_for_all_operations_done(self):
    """Waits until all operations of `_operation_names` are done."""
    # pylint: disable=no-member
    operations = self._service.operations()
    while self._running_operation_names:
      time.sleep(10)
      copy_of_running_operation_names = set(self._running_operation_names)
      for operation_name in copy_of_running_operation_names:
        request = operations.get(name=operation_name)
        response = request.execute()
        if response['done']:
          self._handle_failure(response)
          self._operation_names_to_tests.get(operation_name).validate_result()
          self._running_operation_names.remove(operation_name)
          test_name = (self._operation_names_to_tests.get(operation_name).
                       get_name())
          if test_name in self._test_names_to_dependent_tests:
            self._run_test(self._test_names_to_dependent_tests.get(test_name))

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
    for test_cases in self._tests:
      for test_case in test_cases:
        print '{} ...ok'.format(test_case.get_name())
    return 0


def form_pipeline_api_request(project,  # type: str
                              logging_location,  # type: str
                              image,  # type: str
                              scopes,  # type: List[str]
                              pipeline_name,  # type: str
                              script_path,  # type: str
                              zones,  # type: Optional[List[str]]
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
          'resources': {'zones': zones or _DEFAULT_ZONES},
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
  # type: (str, List[str]) -> List[List[Dict]]
  """Gets all test configs in integration directory and subdirectories."""
  test_configs = []
  for root, _, files in os.walk(test_file_path):
    for filename in files:
      if filename.endswith('.json'):
        test_configs.append(_load_test_configs(os.path.join(root, filename),
                                               required_keys))
  if not test_configs:
    raise TestCaseFailure('Found no .json files in directory {}'.format(
        test_file_path))
  return test_configs


def _load_test_configs(filename, required_keys):
  # type: (str, List[str]) -> List[Dict]
  """Loads an integration test JSON object from a file."""
  with open(filename, 'r') as f:
    tests = json.loads(f.read())
    # When there are multiple test cases in one JSON file, it is a list. Wrap
    # the dictionary in a list if there is only one test case in one JSON file.
    if not isinstance(tests, list):
      tests = [tests]
    _validate_test_configs(tests, filename, required_keys)
    return tests


def _validate_test_configs(test_configs, filename, required_keys):
  # type: (List[Dict], str, List[str]) -> None
  for key in required_keys:
    for test_config in test_configs:
      if key not in test_config:
        raise ValueError('Test case in {} is missing required key: {}'.format(
            filename, key))
