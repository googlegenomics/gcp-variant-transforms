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

from __future__ import absolute_import

import unittest

import logging
import mock

from mock import patch

from apache_beam.io import filesystems
from gcp_variant_transforms.libs.annotation.vep import test_util
from gcp_variant_transforms.libs.annotation.vep import vep_runner


_OUTPUT_DIR = 'gs://output/dir'
_VEP_INFO_FIELD = 'TEST_FIELD'
_IMAGE = 'gcr.io/image'
_CACHE = 'path/to/cache'
_NUM_FORK = 8
_PROJECT = 'test-project'
_REGION = 'test-region'


class _MockFileSystems(filesystems.FileSystems):
  """This inherits from FileSystems such that most functions behave the same."""

  @staticmethod
  def match(patterns, limits=None):
    if len(patterns) == 1 and patterns[0] == test_util.INPUT_PATTERN:
      return [mock.Mock(
          metadata_list=[test_util.FileMetadataStub(path, size) for
                         (path, size) in test_util.INPUT_FILES_WITH_SIZE])]
    return []

  @staticmethod
  def create(path, mime_type=None, compression_type=None):
    """Overriding `create` to remove any interaction with real file systems."""
    return mock.Mock()


class VepRunnerTest(unittest.TestCase):

  def _create_test_instance(self, pipeline_args=None):
    self._mock_service = mock.Mock()  # pylint: disable=attribute-defined-outside-init
    self._mock_pipelines = mock.Mock()  # pylint: disable=attribute-defined-outside-init
    self._mock_request = mock.Mock()  # pylint: disable=attribute-defined-outside-init
    self._mock_service.pipelines = mock.Mock(return_value=self._mock_pipelines)
    self._mock_pipelines.run = mock.Mock(return_value=self._mock_request)
    self._mock_request.execute = mock.Mock(return_value={'name': 'operation'})
    test_object = vep_runner.VepRunner(
        self._mock_service, test_util.INPUT_PATTERN, _OUTPUT_DIR,
        _VEP_INFO_FIELD, _IMAGE, _CACHE, _NUM_FORK,
        pipeline_args or self._get_pipeline_args())
    return test_object

  def _get_pipeline_args(self, num_workers=1):
    return ['--project', _PROJECT,
            '--region', _REGION,
            '--max_num_workers', str(num_workers),
            '--worker_machine_type', 'n1-standard-8',
           ]

  def test_instantiation(self):
    """This is just to test object construction."""
    self._create_test_instance()

  def test_get_output_pattern(self):
    output_pattern = self._create_test_instance().get_output_pattern()
    self.assertEqual(output_pattern, _OUTPUT_DIR + '/**_vep_output.vcf')

  def _validate_run_for_all_files(self):
    matcher = _PartialCommandMatcher(
        [f[0] for f in test_util.INPUT_FILES_WITH_SIZE])
    for args_list in self._mock_pipelines.run.call_args_list:
      self.assertEqual(args_list, mock.call(body=matcher))
    self.assertEqual(len(matcher.input_file_set), 0)

  def test_run_on_all_files(self):
    num_workers = len(test_util.INPUT_FILES_WITH_SIZE) / 2 + 1
    test_instance = self._create_test_instance(
        self._get_pipeline_args(num_workers))
    with patch('apache_beam.io.filesystems.FileSystems', _MockFileSystems):
      test_instance.run_on_all_files()
    all_call_args = self._mock_pipelines.run.call_args_list
    self.assertEqual(len(all_call_args), num_workers)
    self._validate_run_for_all_files()

  def test_run_on_all_files_with_more_workers(self):
    num_workers = len(test_util.INPUT_FILES_WITH_SIZE) + 5
    test_instance = self._create_test_instance(
        self._get_pipeline_args(num_workers))
    with patch('apache_beam.io.filesystems.FileSystems', _MockFileSystems):
      test_instance.run_on_all_files()
    all_call_args = self._mock_pipelines.run.call_args_list
    self.assertEqual(len(all_call_args), len(test_util.INPUT_FILES_WITH_SIZE))
    self._validate_run_for_all_files()


class _PartialCommandMatcher(object):
  """This is used for checking that calls to Pipelines API cover all inputs.

  We need this matcher to avoid duplicating the whole JSON object created in
  the production code. Instead we just do a simple heuristic match by going
  through all `commands` and check if at least in one of them one of the
  input files appear.
  """

  def __init__(self, input_file_list):
    self.input_file_set = set(input_file_list)

  def __eq__(self, other):
    if not isinstance(other, dict):
      return False
    start_len = len(self.input_file_set)
    action_list = other['pipeline']['actions']
    for action in action_list:
      for command_part in action['commands']:
        if command_part in self.input_file_set:
          self.input_file_set.remove(command_part)
    # Making sure that each action list convers at least one file.
    if start_len == len(self.input_file_set):
      logging.error('None of the input files appeared in %s or it was repeated',
                    str(action_list))
      logging.error('List of remaining files: %s', str(self.input_file_set))
      return False
    return True
