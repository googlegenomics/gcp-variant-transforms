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
from typing import Dict, List  # pylint: disable=unused-import

from google.cloud import storage

from gcp_variant_transforms.testing.integration import run_tests_common

_BUCKET_NAME = 'integration_test_runs'
_PIPELINE_NAME = 'gcp-variant-transforms-preprocessor-integration-test'
_SCOPES = []
_SCRIPT_PATH = '/opt/gcp_variant_transforms/bin/vcf_to_bq_preprocess'
_TEST_FOLDER = 'gcp_variant_transforms/testing/integration/preprocessor_tests'


class PreprocessorTestCase(object):
  """Test case that holds information to run in Pipelines API."""

  def __init__(self,
               parser_args,  # type: Namespace
               test_name,  # type: str
               input_pattern,  # type: str
               expected_contents,  # type: List[str]
               blob_name,  # type: str
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    self._keep_reports = parser_args.keep_reports
    self._name = test_name
    self._expected_contents = expected_contents
    self._report_path = '/'.join(['gs:/', _BUCKET_NAME, blob_name])
    self._blob_name = blob_name
    self._project = parser_args.project
    args = ['--input_pattern {}'.format(input_pattern),
            '--report_path {}'.format(self._report_path),
            '--project {}'.format(parser_args.project),
            '--staging_location {}'.format(parser_args.staging_location),
            '--temp_location {}'.format(parser_args.temp_location),
            '--job_name {}'.format(test_name)]
    for k, v in kwargs.iteritems():
      args.append('--{} {}'.format(k, v))

    self.pipeline_api_request = run_tests_common.form_pipeline_api_request(
        parser_args.project, parser_args.logging_location, parser_args.image,
        _SCOPES, _PIPELINE_NAME, _SCRIPT_PATH, args)

  def validate_result(self):
    """Validates the results.

    - Checks that the report is generated.
    - Validates report's contents are the same as `expected_contents`.
    """
    client = storage.Client(self._project)
    bucket = client.get_bucket(_BUCKET_NAME)
    blob = bucket.get_blob(self._blob_name)
    if not blob.exists(client):
      raise run_tests_common.TestCaseFailure(
          'Report is not generated in {}'.format(self._report_path))
    contents = blob.download_as_string()
    expected_contents = ''.join(self._expected_contents)
    if expected_contents != contents:
      raise run_tests_common.TestCaseFailure(
          'Contents mismatch: expected {}, got {}'.format(
              expected_contents, contents))
    if not self._keep_reports:
      blob.delete()

  def get_name(self):
    return self._name


def _get_args():
  parser = argparse.ArgumentParser()
  run_tests_common.add_args(parser)
  parser.add_argument('--keep_reports',
                      type=bool, default=False, nargs='?', const=True,
                      help='If set, generated reports are not deleted.')
  return parser.parse_args()


def _get_test_configs():
  # type: () -> List[Dict]
  """Gets all test configs in preprocessor_tests."""
  required_keys = ['test_name', 'blob_name', 'input_pattern',
                   'expected_contents']
  test_file_path = os.path.join(os.getcwd(), _TEST_FOLDER)
  return run_tests_common.get_configs(test_file_path, required_keys)


def main():
  """Runs the integration tests for preprocessor."""
  args = _get_args()
  test_case_configs = _get_test_configs()

  tests = []
  for config in test_case_configs:
    tests.append(PreprocessorTestCase(args, **config))
  test_runner = run_tests_common.TestRunner(tests)
  test_runner.run()
  for test in tests:
    test.validate_result()
  return test_runner.print_errors()


if __name__ == '__main__':
  ret_code = main()
  sys.exit(ret_code)
