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

"""Integration testing runner for BQ to VCF and VCF To BQ pipeline."""

import argparse
import os
import sys
from datetime import datetime
from typing import Dict, List  # pylint: disable=unused-import

from gcp_variant_transforms.testing.integration import run_tests_common
from gcp_variant_transforms.testing.integration import run_vcf_to_bq_tests

_BUCKET_NAME = 'integration_test_runs'
_PIPELINE_NAME = 'gcp-variant-transforms-bq-to-vcf-integration-test'
_SCOPES = ['https://www.googleapis.com/auth/bigquery']
_TEST_FOLDER = 'gcp_variant_transforms/testing/integration/bq_to_vcf_tests'


class BqToVcfTestCase(run_tests_common.TestCaseInterface):
  """Test case that holds information to run in Pipelines API."""

  def __init__(self,
               context,
               test_name,  # type: str
               table_name,  # type: str
               output_file,  # type: str
               script=run_tests_common.BQ_TO_VCF_SCRIPT,  # type: str
               zones=None,  # type: List[str]
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    self._name = test_name
    self._output_file = output_file
    input_table = '{}:{}.{}'.format(context.project,
                                    context.dataset_id,
                                    table_name)
    suffix = '_integration_tests_{}'.format(
        datetime.now().strftime('%Y%m%d_%H%M%S'))
    self._project = context.project

    args = ['--input_table {}'.format(input_table),
            '--output_file {}'.format(output_file),
            '--project {}'.format(context.project),
            '--staging_location {}'.format(context.staging_location),
            '--temp_location {}'.format(context.temp_location),
            '--job_name {}'.format(
                ''.join([test_name, suffix]).replace('_', '-'))]
    for k, v in kwargs.iteritems():
      args.append('--{} {}'.format(k, v))

    self.pipeline_api_request = run_tests_common.form_pipeline_api_request(
        context.project, context.logging_location, context.image,
        _SCOPES, _PIPELINE_NAME, run_tests_common.get_script_path(script),
        zones, args)

  def get_name(self):
    return self._name

  def validate_result(self):
    pass


def _get_args():
  parser = argparse.ArgumentParser()
  run_tests_common.add_args(parser)
  parser.add_argument('--run_presubmit_tests',
                      type=bool, default=False, nargs='?', const=True,
                      help='If set, runs the presubmit_tests.')
  parser.add_argument('--run_all_tests',
                      type=bool, default=False, nargs='?', const=True,
                      help='If set, runs all integration tests.')
  parser.add_argument('--keep_tables',
                      type=bool, default=False, nargs='?', const=True,
                      help='If set, created tables are not deleted.')
  parser.add_argument(
      '--revalidation_dataset_id',
      help=('If set, instead of running the full test, skips '
            'most of it and only validates the tables in the '
            'given dataset. This is useful when --keep_tables '
            'is used in a previous run. Example: '
            '--revalidation_dataset_id integration_tests_20180118_014812'))
  parser.add_argument(
      '--script',
      help=('The script current test case should use. `vcf_to_bq` and '
            '`bq_to_vcf` are supported.'))
  return run_vcf_to_bq_tests._get_args()


def _get_test_configs():
  # type: () -> List[List[Dict]]
  """Gets all test configs in preprocessor_tests."""
  # TODO (yifangchen): Refactor the test_config required key validation based on
  # The script typle.
  required_keys = []
  test_file_path = os.path.join(os.getcwd(), _TEST_FOLDER)
  return run_tests_common.get_configs(test_file_path, required_keys)


def main():
  """Runs the integration tests for bq to vcf."""
  args = _get_args()
  test_configs = _get_test_configs()
  tests = []
  with run_vcf_to_bq_tests.TestContextManager(args) as context:
    for test_case_configs in test_configs:
      test_cases = []
      for test_case_config in test_case_configs:
        script = test_case_config.get('script')
        if script == run_tests_common.BQ_TO_VCF_SCRIPT:
          test_cases.append(BqToVcfTestCase(context, ** test_case_config))
        elif script == run_tests_common.VCF_TO_BQ_SCRIPT:
          test_cases.append(run_vcf_to_bq_tests.VcfToBQTestCase(
              context, ** test_case_config))
        else:
          raise run_tests_common.TestCaseFailure(
              'The script type {} is not supported'.format(script))
      tests.append(test_cases)
    test_runner = run_tests_common.TestRunner(tests)
    test_runner.run()
  return test_runner.print_results()


if __name__ == '__main__':
  ret_code = main()
  sys.exit(ret_code)
