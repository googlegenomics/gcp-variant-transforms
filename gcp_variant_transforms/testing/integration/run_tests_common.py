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
(integration test script for vcf_to_bq preprocessor pipeline).
"""

from abc import abstractmethod
from typing import Dict, List  # pylint: disable=unused-import
import argparse  # pylint: disable=unused-import
import json
import os
import time

from googleapiclient import discovery

DEFAULT_IMAGE_NAME = 'gcr.io/gcp-variant-transforms/gcp-variant-transforms'


class TestCaseFailure(Exception):
  """Exception for failed test cases."""
  pass


class TestCase(object):
  """Test case that holds information to run in pipelines API."""

  def __init__(self, test_name, project):
    # type: (str, str) -> None
    self._name = test_name
    self._project = project

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
    """Waits until the operation ``operation_name`` of ``service`` is done."""
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

  @abstractmethod
  def validate_result(self):
    raise NotImplementedError


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
            'production image {} is used.').format(DEFAULT_IMAGE_NAME),
      default=DEFAULT_IMAGE_NAME,
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


def print_errors(results):
  """Prints results of test cases and traceback for any errors."""
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
  if errors:
    return 1
  else:
    return 0


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
