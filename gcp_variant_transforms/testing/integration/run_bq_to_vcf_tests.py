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

"""Integration testing runner for BQ to VCF pipeline.

To define a new bq to vcf integration test case, create a json file in
`gcp_variant_transforms/testing/integration/bq_to_vcf_tests` directory and
specify at least test_name, input_table, output_file_name and
expected_output_file. `expected_output_file` saves the expected VCF contents.

Execute the following command from the root source directory:
python gcp_variant_transforms/testing/integration/run_bq_to_vcf_tests.py \
  --project gcp-variant-transforms-test \
  --region us-central1 \
  --staging_location gs://integration_test_runs/staging \
  --temp_location gs://integration_test_runs/temp \
  --logging_location gs://integration_test_runs/temp/integration_test_logs

It runs all integration tests inside
`gcp_variant_transforms/testing/integration/bq_to_vcf_tests`.
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Dict, List  # pylint: disable=unused-import

from apache_beam.io import filesystems

from gcp_variant_transforms.testing.integration import run_tests_common


_TEST_FOLDER = 'gcp_variant_transforms/testing/integration/bq_to_vcf_tests'
_TOOL_NAME = 'bq_to_vcf'


class BqToVcfTestCase(run_tests_common.TestCaseInterface):
  """Test case that holds information to run in Pipelines API."""

  def __init__(self,
               parsed_args,
               test_name,  # type: str
               input_table,  # type: str
               output_file_name,  # type: str
               expected_output_file,  # type: str
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    self._name = test_name
    self._expected_output_file = expected_output_file
    timestamp = 'integration_tests_{}'.format(
        datetime.now().strftime('%Y%m%d_%H%M%S'))
    self._output_file = filesystems.FileSystems.join(parsed_args.temp_location,
                                                     timestamp,
                                                     output_file_name)
    args = ['--input_table {}'.format(input_table),
            '--output_file {}'.format(self._output_file),
            '--staging_location {}'.format(parsed_args.staging_location),
            '--temp_location {}'.format(parsed_args.temp_location),
            '--job_name {}'.format(
                ''.join([test_name, timestamp]).replace('_', '-'))]
    for k, v in kwargs.iteritems():
      args.append('--{} {}'.format(k, v))

    self.run_test_command = run_tests_common.form_command(
        parsed_args.project,
        parsed_args.region,
        filesystems.FileSystems.join(parsed_args.logging_location,
                                     '_'.join([test_name, timestamp])),
        parsed_args.image, _TOOL_NAME, args)

  def validate_result(self):
    """Validates the results.

    - Checks that the VCF file is generated.
    - Validates VCF contents are the same as expected.
    """
    if not filesystems.FileSystems.exists(self._output_file):
      raise run_tests_common.TestCaseFailure(
          'The VCF is not generated in {} in test {}'.format(self._output_file,
                                                             self._name))
    lines = filesystems.FileSystems.open(self._output_file).readlines()
    expected_lines = filesystems.FileSystems.open(
        self._expected_output_file).readlines()
    if len(lines) != len(expected_lines):
      raise run_tests_common.TestCaseFailure(
          'Expected {} lines for file {} , got {} in test {}'.format(
              len(expected_lines), self._output_file, len(lines), self._name))
    for line, expected_line in zip(lines, expected_lines):
      if line != expected_line:
        raise run_tests_common.TestCaseFailure(
            'The contents of the VCF generated in {} in test {} is different '
            'from the expected: {} vs {}'.format(
                self._output_file, self._name, line, expected_line))

  def get_name(self):
    return self._name


def _get_args():
  parser = argparse.ArgumentParser()
  run_tests_common.add_args(parser)
  return parser.parse_args()


def _get_test_configs():
  # type: () -> List[List[Dict]]
  """Gets all test configs in bq_to_vcf_tests."""
  required_keys = ['input_table', 'output_file_name', 'expected_output_file']
  test_file_path = os.path.join(os.getcwd(), _TEST_FOLDER)
  return run_tests_common.get_configs(test_file_path, required_keys)


def main():
  """Runs the integration tests for bq to vcf."""
  args = _get_args()
  test_configs = _get_test_configs()
  tests = []

  for test_case_configs in test_configs:
    test_cases = []
    for config in test_case_configs:
      test_cases.append(BqToVcfTestCase(args, ** config))
    tests.append(test_cases)
  test_runner = run_tests_common.TestRunner(tests)
  test_runner.run()
  return test_runner.print_results()


if __name__ == '__main__':
  print('Starting bq_to_vcf tests...')
  ret_code = main()
  print('Finished all bq_to_vcf tests successfully.')
  sys.exit(ret_code)
