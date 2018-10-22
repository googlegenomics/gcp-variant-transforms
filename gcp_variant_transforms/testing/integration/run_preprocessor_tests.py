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

r"""Integration testing runner for Variant Transforms' Preprocessor pipeline.

To define a new preprocessor_tests integration test case, create a json file in
gcp_variant_transforms/testing/integration/preprocessor_tests directory and
specify at least test_name, input_pattern, blob_name and expected_contents
for the integration test.

Execute the following command from the root source directory:
python gcp_variant_transforms/testing/integration/run_preprocessor_tests.py \
  --project gcp-variant-transforms-test \
  --staging_location gs://integration_test_runs/staging \
  --temp_location gs://integration_test_runs/temp \
  --logging_location gs://integration_test_runs/temp/integration_test_logs

To keep the reports that this test creates, use the --keep_reports option.

It runs all integration tests inside
`gcp_variant_transforms/testing/integration/preprocessor_tests`.
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Dict, List  # pylint: disable=unused-import

from google.cloud import storage

from gcp_variant_transforms.testing.integration import run_tests_common

_BUCKET_NAME = 'integration_test_runs'
_PIPELINE_NAME = 'gcp-variant-transforms-preprocessor-integration-test'
_SCRIPT_PATH = '/opt/gcp_variant_transforms/bin/vcf_to_bq_preprocess'
_TEST_FOLDER = 'gcp_variant_transforms/testing/integration/preprocessor_tests'


class PreprocessorTestCase(run_tests_common.TestCaseInterface):
  """Test case that holds information to run in Pipelines API."""

  def __init__(self,
               parser_args,  # type: Namespace
               test_name,  # type: str
               input_pattern,  # type: str
               expected_contents,  # type: List[str]
               report_blob_name,  # type: str
               header_blob_name=None,  # type: str
               zones=None,  # type: List[str]
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    self._keep_reports = parser_args.keep_reports
    self._name = test_name
    self._expected_contents = expected_contents
    suffix = '_integration_tests_{}'.format(
        datetime.now().strftime('%Y%m%d_%H%M%S'))
    self._report_blob_name = self._append_suffix(report_blob_name, suffix)
    self._report_path = '/'.join(['gs:/', _BUCKET_NAME, self._report_blob_name])
    self._project = parser_args.project
    args = ['--input_pattern {}'.format(input_pattern),
            '--report_path {}'.format(self._report_path),
            '--project {}'.format(parser_args.project),
            '--staging_location {}'.format(parser_args.staging_location),
            '--temp_location {}'.format(parser_args.temp_location),
            '--job_name {}'.format(
                ''.join([test_name, suffix]).replace('_', '-'))]
    self._header_blob_name = None
    self._header_path = None
    if header_blob_name:
      self._header_blob_name = self._append_suffix(header_blob_name, suffix)
      self._header_path = '/'.join(['gs:/',
                                    _BUCKET_NAME,
                                    self._header_blob_name])
      args.append('--resolved_headers_path {}'.format(self._header_path))
    for k, v in kwargs.iteritems():
      args.append('--{} {}'.format(k, v))

    self.pipelines_api_request = run_tests_common.form_pipelines_api_request(
        parser_args.project,
        '/'.join([parser_args.logging_location, self._report_blob_name]),
        parser_args.image, _PIPELINE_NAME, _SCRIPT_PATH, zones, args)

  def validate_result(self):
    """Validates the results.

    - Checks that the report is generated.
    - Validates report's contents are the same as `expected_contents`.
    - Checks that the resolved headers are generated if `header_blob_name` is
      specified in the test.
    """
    client = storage.Client(self._project)
    bucket = client.get_bucket(_BUCKET_NAME)
    report_blob = bucket.get_blob(self._report_blob_name)
    if not report_blob:
      raise run_tests_common.TestCaseFailure(
          'Report is not generated in {} in test {}'.format(self._report_path,
                                                            self._name))
    contents = report_blob.download_as_string()
    expected_contents = '\n'.join(self._expected_contents)
    if expected_contents != contents:
      raise run_tests_common.TestCaseFailure(
          'Contents mismatch: expected {}, got {} in test {}'.format(
              expected_contents, contents, self._name))
    if not self._keep_reports:
      report_blob.delete()

    if self._header_blob_name:
      resolved_headers_blob = bucket.get_blob(self._header_blob_name)
      if not resolved_headers_blob:
        raise run_tests_common.TestCaseFailure(
            'The resolved header is not generated in {} in test {}'.format(
                self._header_path, self._name))
      if not self._keep_reports:
        resolved_headers_blob.delete()

  def get_name(self):
    return self._name

  def _append_suffix(self, file_path, suffix):
    # type: (str, str) -> str
    file_name, file_extension = os.path.splitext(file_path)
    return ''.join([file_name, suffix, file_extension])


def _get_args():
  parser = argparse.ArgumentParser()
  run_tests_common.add_args(parser)
  parser.add_argument('--keep_reports',
                      type=bool, default=False, nargs='?', const=True,
                      help=('If set, generated reports and resolved headers '
                            'are not deleted.'))
  return parser.parse_args()


def _get_test_configs():
  # type: () -> List[List[Dict]]
  """Gets all test configs in preprocessor_tests."""
  required_keys = ['test_name', 'report_blob_name', 'input_pattern',
                   'expected_contents']
  test_file_path = os.path.join(os.getcwd(), _TEST_FOLDER)
  return run_tests_common.get_configs(test_file_path, required_keys)


def main():
  """Runs the integration tests for preprocessor."""
  args = _get_args()
  test_configs = _get_test_configs()

  tests = []
  for test_case_configs in test_configs:
    test_cases = []
    for config in test_case_configs:
      test_cases.append(PreprocessorTestCase(args, **config))
    tests.append(test_cases)
  test_runner = run_tests_common.TestRunner(tests)
  test_runner.run()
  return test_runner.print_results()


if __name__ == '__main__':
  ret_code = main()
  sys.exit(ret_code)
