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
from collections import namedtuple
from typing import Dict, List, Optional  # pylint: disable=unused-import

from apache_beam.io import filesystems

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

_DEFAULT_IMAGE_NAME = 'gcr.io/gcp-variant-transforms/gcp-variant-transforms'
_DEFAULT_ZONES = ['us-east1-b']

# `TestCaseState` saves current running test and the remaining tests in the same
# test script (.json).
TestCaseState = namedtuple('TestCaseState',
                           ['running_test', 'remaining_tests'])
_BASE_SCRIPT_PATH = '/opt/gcp_variant_transforms/bin'
BQ_TO_VCF_SCRIPT = 'bq_to_vcf'
VCF_TO_BQ_SCRIPT = 'vcf_to_bq'


class TestCaseInterface(object):
  """Interface of an integration test case."""

  def validate_result(self):
    """Validates the result of the test case."""
    raise NotImplementedError


class TestCaseFailure(Exception):
  """Exception for failed test cases."""
  pass


class TestRunner(object):
  """Runs the tests using pipelines API."""

  def __init__(self, tests, revalidate=False):
    # type: (List[List[TestCaseInterface]], bool) -> None
    """Initializes the TestRunner.

    Args:
      tests: All test cases.
      revalidate: If True, only run the result validation part of the tests.
    """
    self._tests = tests
    self._service = discovery.build(
        'genomics',
        'v1alpha2',
        credentials=GoogleCredentials.get_application_default())
    self._revalidate = revalidate
    self._operation_names_to_test_states = {}  # type: Dict[str, TestCaseState]

  def run(self):
    """Runs all tests."""
    if self._revalidate:
      for test_cases in self._tests:
        # Only validates the last test case in one test script since the table
        # created by one test case might be altered by the following up ones.
        test_cases[-1].validate_result()
    else:
      for test_cases in self._tests:
        self._run_test(test_cases)
      self._wait_for_all_operations_done()

  def _run_test(self, test_cases):
    # type: (List[TestCaseInterface]) -> None
    """Runs the first test case in `test_cases`.

    The first test case and the remaining test cases form `TestCaseState` and
    are added into `_operation_names_to_test_states` for future usage.
    """
    if not test_cases:
      return
    # The following pylint hint is needed because `pipelines` is a method that
    # is dynamically added to the returned `service` object above. See
    # `googleapiclient.discovery.Resource._set_service_methods`.
    # pylint: disable=no-member
    request = self._service.pipelines().run(
        body=test_cases[0].pipeline_api_request)
    operation_name = request.execute()['name']
    self._operation_names_to_test_states.update(
        {operation_name: TestCaseState(test_cases[0], test_cases[1:])})

  def _wait_for_all_operations_done(self):
    """Waits until all operations are done."""
    # pylint: disable=no-member
    operations = self._service.operations()
    while self._operation_names_to_test_states:
      time.sleep(10)
      running_operation_names = self._operation_names_to_test_states.keys()
      for operation_name in running_operation_names:
        request = operations.get(name=operation_name)
        response = request.execute()
        if response['done']:
          self._handle_failure(response)
          test_case_state = self._operation_names_to_test_states.get(
              operation_name)
          del self._operation_names_to_test_states[operation_name]
          test_case_state.running_test.validate_result()
          self._run_test(test_case_state.remaining_tests)

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


def get_script_path(script):
  if script == BQ_TO_VCF_SCRIPT or VCF_TO_BQ_SCRIPT:
    return filesystems.FileSystems.join(_BASE_SCRIPT_PATH, script)
  else:
    raise TestCaseFailure('The script type {} is not supported'.format(script))


def _load_test_configs(filename, required_keys):
  # type: (str, List[str]) -> List[Dict]
  """Loads an integration test JSON object from a file."""
  with open(filename, 'r') as f:
    tests = json.loads(f.read())
    _validate_test_configs(tests, filename, required_keys)
    return tests


def _validate_test_configs(test_configs, filename, required_keys):
  # type: (List[Dict], str, List[str]) -> None
  for key in required_keys:
    for test_config in test_configs:
      if key not in test_config:
        raise ValueError('Test case in {} is missing required key: {}'.format(
            filename, key))
