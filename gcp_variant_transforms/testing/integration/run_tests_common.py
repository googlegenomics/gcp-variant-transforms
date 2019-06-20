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
import subprocess
import time
from collections import namedtuple
from typing import Dict, List, Optional  # pylint: disable=unused-import


_DEFAULT_IMAGE_NAME = 'gcr.io/cloud-lifesciences/gcp-variant-transforms'
_DEFAULT_ZONES = ['us-east1-b']

# `TestCaseState` saves current running test and the remaining tests in the same
# test script (.json).
TestCaseState = namedtuple('TestCaseState',
                           ['running_test', 'remaining_tests'])


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
    self._revalidate = revalidate
    self._test_names_to_test_states = {}  # type: Dict[str, TestCaseState]
    self._test_names_to_processes = {}  # type: Dict[str, subprocess.Popen]

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
    are added into `_test_names_to_test_states` for future usage.
    """
    if not test_cases:
      return
    self._test_names_to_test_states.update({
        test_cases[0].get_name(): TestCaseState(test_cases[0], test_cases[1:])})
    self._test_names_to_processes.update(
        {test_cases[0].get_name(): subprocess.Popen(
            test_cases[0].run_test_command, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)})

  def _wait_for_all_operations_done(self):
    """Waits until all operations are done."""
    while self._test_names_to_processes:
      time.sleep(10)
      running_test_names = self._test_names_to_processes.keys()
      for test_name in running_test_names:
        running_proc = self._test_names_to_processes.get(test_name)
        return_code = running_proc.poll()
        if return_code is not None:
          test_case_state = self._test_names_to_test_states.get(test_name)
          self._handle_failure(running_proc, test_case_state.running_test)
          del self._test_names_to_processes[test_name]
          test_case_state.running_test.validate_result()
          self._run_test(test_case_state.remaining_tests)

  def _handle_failure(self, proc, test_case):
    """Raises errors if test case failed."""
    if proc.returncode != 0:
      stdout, stderr = proc.communicate()
      raise TestCaseFailure('Test case {} failed. stdout: {}, stderr: {}, '
                            'return code: {}.'.format(test_case.get_name(),
                                                      stdout, stderr,
                                                      proc.returncode))

  def print_results(self):
    """Prints results of test cases."""
    for test_cases in self._tests:
      for test_case in test_cases:
        print '{} ...ok'.format(test_case.get_name())
    return 0


def form_command(project, temp_location, image, tool_name, zones, args):
  # type: (str, str, str, str, Optional[List[str]], List[str]) -> List[str]
  return ['/opt/gcp_variant_transforms/src/docker/pipelines_runner.sh',
          '--project', project,
          '--docker_image', image,
          '--temp_location', temp_location,
          '--zones', str(' '.join(zones or _DEFAULT_ZONES)),
          ' '.join([tool_name] + args)]


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


def get_configs(test_file_dir, required_keys, test_file_suffix=''):
  # type: (str, List[str], str) -> List[List[Dict]]
  """Gets test configs.

  Args:
    test_file_dir: The directory where the test cases are saved.
    required_keys: The keys that are required in each test case.
    test_file_suffix: If empty, all test cases in `test_file_path` are
      considered. Otherwise, only the test cases that end with this suffix will
      run.
  Raises:
    TestCaseFailure: If no test cases are found.
  """
  test_configs = []
  test_file_suffix = test_file_suffix or '.json'
  for root, _, files in os.walk(test_file_dir):
    for filename in files:
      if filename.endswith(test_file_suffix):
        test_configs.append(_load_test_configs(os.path.join(root, filename),
                                               required_keys))
  if not test_configs:
    raise TestCaseFailure('Found no {} file in directory {}'.format(
        test_file_suffix, test_file_dir))
  return test_configs


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
