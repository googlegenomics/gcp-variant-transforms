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

You may run this test in any project (the test files are publicly accessible).
Execute the following command from the root source directory:
python gcp_variant_transforms/testing/integration/run_preprocessor_tests.py \
  --project myproject \
  --staging_location gs://mybucket/staging \
  --temp_location gs://mybucket/temp \
  --logging_location gs://mybucket/temp/integration_test_logs

To keep the reports that this test creates, use the --keep_reports option.

It runs all integration tests inside
`gcp_variant_transforms/testing/integration/preprocessor_tests`.
"""

import argparse
import multiprocessing
import sys
import os
from typing import Dict, List  # pylint: disable=unused-import

from apache_beam.io import filesystems
from oauth2client.client import GoogleCredentials

from gcp_variant_transforms.testing.integration import run_tests_common

_PIPELINE_NAME = 'gcp-variant-transforms-preprocessor-integration-test'
_SCOPES = []
_DEFAULT_ZONES = ['us-west1-b']
_SCRIPT_PATH = '/opt/gcp_variant_transforms/bin/vcf_to_bq_preprocess'
_TEST_FOLDER = 'gcp_variant_transforms/testing/integration/preprocessor_tests'


class PreprocessorTestCase(run_tests_common.TestCase):
  """Test case that holds information to run in pipelines API.

  To define a new preprocessor_tests integration test case, create a json file
  in gcp_variant_transforms/testing/integration/preprocessor_tests directory and
  specify at least test_name, input_pattern, report_path and expected_contents
  for the integration test.
  """

  def __init__(self,
               context,
               test_name,
               input_pattern,
               expected_contents,
               report_path,
               **kwargs):
    # type: (TestContextManager, str, str, List[str], str, str) -> None
    super(PreprocessorTestCase, self).__init__(test_name, context.project)
    self._expected_contents = expected_contents
    self._report_path = report_path
    self._keep_reports = context.keep_reports
    args = ['--input_pattern {}'.format(input_pattern),
            '--report_path {}'.format(report_path),
            '--project {}'.format(context.project),
            '--staging_location {}'.format(context.staging_location),
            '--temp_location {}'.format(context.temp_location),
            '--job_name {}'.format(test_name)]
    for k, v in kwargs.iteritems():
      args.append('--{} {}'.format(k, v))

    self._pipelines_api_request = {
        'pipelineArgs': {
            'projectId': context.project,
            'logging': {'gcsPath': context.logging_location},
            'serviceAccount': {'scopes': _SCOPES}
        },
        'ephemeralPipeline': {
            'projectId': context.project,
            'name': _PIPELINE_NAME,
            'resources': {'zones': _DEFAULT_ZONES},
            'docker': {
                'imageName': context.image,
                'cmd': ' '.join([_SCRIPT_PATH] + args)
            }
        }
    }

  def validate_result(self):
    """Validates the results.

    - Checks that the report is generated.
    - Validates report's contents are the same as ``expected_contents``.
    """
    if not filesystems.FileSystems.exists(self._report_path):
      raise run_tests_common.TestCaseFailure(
          'Report is not generated in {}'.format(self._report_path))
    with filesystems.FileSystems.open(self._report_path) as f:
      for expected_content in self._expected_contents:
        line = f.readline()
        if expected_content != line:
          raise run_tests_common.TestCaseFailure(
              'Contents mismatch: expected {}, got {}'.format(
                  expected_content, line))
    if not self._keep_reports:
      filesystems.FileSystems.delete(self._report_path)


class TestContextManager(object):
  """Manages all resources for a given run of tests."""

  def __init__(self, args):
    # type: (argparse.ArgumentParser) -> None
    self.staging_location = args.staging_location
    self.temp_location = args.temp_location
    self.logging_location = args.logging_location
    self.project = args.project
    self.credentials = GoogleCredentials.get_application_default()
    self.image = args.image
    self.keep_reports = args.keep_reports

  def __enter__(self):
    return self

  def __exit__(self, *args):
    return


def _get_args():
  parser = argparse.ArgumentParser()
  run_tests_common.add_args(parser)
  parser.add_argument('--keep_reports',
                      type=bool, default=False, nargs='?', const=True,
                      help='If set, generated reports and resolved headers are '
                           'not deleted.')
  return parser.parse_args()


def _get_test_configs():
  # type: () -> List[Dict]
  """Gets all test configs in preprocessor_tests."""
  required_keys = ['test_name', 'report_path', 'input_pattern',
                   'expected_contents']
  test_file_path = os.path.join(os.getcwd(), _TEST_FOLDER)
  return run_tests_common.get_configs(test_file_path, required_keys)


def _run_test(test, context):
  test.run(context)


def main():
  """Runs the integration tests for preprocessor."""
  args = _get_args()
  test_case_configs = _get_test_configs()
  with TestContextManager(args) as context:
    pool = multiprocessing.Pool(processes=len(test_case_configs))
    results = []
    for config in test_case_configs:
      test = PreprocessorTestCase(context, **config)
      results.append(
          (pool.apply_async(func=_run_test, args=(test, context)), test))

    pool.close()
    pool.join()
  return run_tests_common.print_errors(results)


if __name__ == '__main__':
  ret_code = main()
  sys.exit(ret_code)
