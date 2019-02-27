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

from typing import List  # pylint: disable=unused-import

import mock
from mock import patch

from apache_beam.io import filesystems
from gcp_variant_transforms.libs.annotation.vep import file_metadata_stub
from gcp_variant_transforms.libs.annotation.vep import vep_runner


_INPUT_PATTERN = 'some/input/pattern*'
_INPUT_FILES_WITH_SIZE = [
    ('some/input/pattern/a', 100),
    ('some/input/pattern/b', 100),
    ('some/input/pattern/c', 100),
    ('some/input/pattern/dir1/a', 100),
    ('some/input/pattern/dir1/dir2/b', 100),
    ('some/input/pattern/dir2/b', 100),
    ('some/input/pattern/dir2/c', 100),
]
_SPECIES = 'homo_sapiens'
_ASSEMBLY = 'GRCh38'
_OUTPUT_DIR = 'gs://output/dir'
_VEP_INFO_FIELD = 'TEST_FIELD'
_IMAGE = 'gcr.io/image'
_CACHE = 'gs://path/to/cache'
_NUM_FORK = 8
_PROJECT = 'test-project'
_REGION = 'test-region'


class _MockFileSystems(filesystems.FileSystems):
  """This inherits from FileSystems such that most functions behave the same."""

  @staticmethod
  def match(patterns, limits=None):
    if len(patterns) == 1 and patterns[0] == _INPUT_PATTERN:
      return [mock.Mock(
          metadata_list=[file_metadata_stub.FileMetadataStub(path, size) for
                         (path, size) in _INPUT_FILES_WITH_SIZE])]
    return []

  @staticmethod
  def create(path, mime_type=None, compression_type=None):
    """Overriding `create` to remove any interaction with real file systems."""
    return mock.Mock()


class VepRunnerTest(unittest.TestCase):

  def setUp(self):
    self._mock_service = mock.Mock()
    self._mock_request = mock.Mock()
    self._pipelines_spy = PipelinesSpy(self._mock_request)
    self._mock_service.pipelines = mock.Mock(return_value=self._pipelines_spy)
    self._mock_request.execute = mock.Mock(return_value={'name': 'operation'})

  def _create_test_instance(self, pipeline_args=None):
    # type: (List[str]) -> vep_runner.VepRunner
    test_object = vep_runner.VepRunner(
        self._mock_service, _ASSEMBLY, _SPECIES,
        _INPUT_PATTERN, _OUTPUT_DIR,
        _VEP_INFO_FIELD, _IMAGE, _CACHE, _NUM_FORK,
        pipeline_args or self._get_pipeline_args(), None, 30)
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

  def test_instantiation_bad_pipeline_options(self):
    """This is to test object construction fails with incomplete arguments."""
    with self.assertRaisesRegexp(ValueError, '.*project.*'):
      self._create_test_instance(pipeline_args=['no_arguments'])

  def test_make_vep_cache_path(self):
    test_instance = self._create_test_instance()
    self.assertEqual(test_instance._vep_cache_path, _CACHE)
    test_instance = vep_runner.VepRunner(
        self._mock_service, _SPECIES, _ASSEMBLY, _INPUT_PATTERN, _OUTPUT_DIR,
        _VEP_INFO_FIELD, _IMAGE, '', _NUM_FORK, self._get_pipeline_args(),
        None, 30)
    self.assertEqual(test_instance._vep_cache_path,
                     ('gs://gcp-variant-annotation-vep-cache/'
                      'vep_cache_homo_sapiens_GRCh38_91.tar.gz'))
    test_instance = vep_runner.VepRunner(
        self._mock_service, 'mouse', 'mm9', _INPUT_PATTERN, _OUTPUT_DIR,
        _VEP_INFO_FIELD, _IMAGE, '', _NUM_FORK, self._get_pipeline_args(),
        None, 30)
    self.assertEqual(test_instance._vep_cache_path,
                     ('gs://gcp-variant-annotation-vep-cache/'
                      'vep_cache_mouse_mm9_91.tar.gz'))

  def test_get_output_pattern(self):
    output_pattern = self._create_test_instance().get_output_pattern()
    self.assertEqual(output_pattern, _OUTPUT_DIR + '/**_vep_output.vcf')

  def test_get_output_log_path(self):
    test_instance = self._create_test_instance()
    log_path = test_instance._get_output_log_path(_OUTPUT_DIR, 0)
    self.assertEqual(log_path, _OUTPUT_DIR + '/logs/output_VM_0.log')
    log_path = test_instance._get_output_log_path(_OUTPUT_DIR + '/', 0)
    self.assertEqual(log_path, _OUTPUT_DIR + '/logs/output_VM_0.log')

  def _validate_run_for_all_files(self):
    self._pipelines_spy.validate_calls([f[0] for f in _INPUT_FILES_WITH_SIZE])

  def test_run_on_all_files(self):
    num_workers = len(_INPUT_FILES_WITH_SIZE) / 2 + 1
    test_instance = self._create_test_instance(
        self._get_pipeline_args(num_workers))
    with patch('apache_beam.io.filesystems.FileSystems', _MockFileSystems):
      test_instance.run_on_all_files()
    self.assertEqual(self._pipelines_spy.num_run_calls(), num_workers)
    self._validate_run_for_all_files()

  def test_run_on_all_files_with_more_workers(self):
    num_workers = len(_INPUT_FILES_WITH_SIZE) + 5
    test_instance = self._create_test_instance(
        self._get_pipeline_args(num_workers))
    with patch('apache_beam.io.filesystems.FileSystems', _MockFileSystems):
      test_instance.run_on_all_files()
    self.assertEqual(self._pipelines_spy.num_run_calls(),
                     len(_INPUT_FILES_WITH_SIZE))
    self._validate_run_for_all_files()

  def test_wait_until_done(self):
    mock_projects = mock.Mock()
    mock_opearations = mock.Mock()
    mock_request = mock.Mock()
    self._mock_service.projects = mock.Mock(return_value=mock_projects)
    mock_projects.operations = mock.Mock(return_value=mock_opearations)
    mock_opearations.get = mock.Mock(return_value=mock_request)
    mock_request.execute = mock.Mock(return_value={'done': True, 'error': {}})
    test_instance = self._create_test_instance()
    with patch('apache_beam.io.filesystems.FileSystems', _MockFileSystems):
      test_instance.run_on_all_files()
      with self.assertRaisesRegexp(AssertionError, '.*already.*running.*'):
        # Since there are running operations, the next call raises an exception.
        test_instance.run_on_all_files()
      test_instance.wait_until_done()
      # Since all operations are done, the next call should raise no exceptions.
      test_instance.run_on_all_files()

  def test_wait_until_done_fail(self):
    mock_projects = mock.Mock()
    mock_opearations = mock.Mock()
    mock_request = mock.Mock()
    self._mock_service.projects = mock.Mock(return_value=mock_projects)
    mock_projects.operations = mock.Mock(return_value=mock_opearations)
    mock_opearations.get = mock.Mock(return_value=mock_request)
    mock_request.execute = mock.Mock(return_value={
        'done': True, 'error': {'message': 'failed'}})

    test_instance = self._create_test_instance()
    with patch('apache_beam.io.filesystems.FileSystems', _MockFileSystems):
      test_instance.run_on_all_files()
      with self.assertRaisesRegexp(RuntimeError, '.*failed.*retries.*'):
        test_instance.wait_until_done()


class PipelinesSpy(object):
  """A class to intercept calls to the run() function of Pipelines API."""

  def __init__(self, mock_request):
    # type: (mock.Mock) -> None
    self._actions_list = []
    self._mock_request = mock_request

  def run(self, body=None):
    assert body
    self._actions_list.append(body['pipeline']['actions'])
    return self._mock_request

  def num_run_calls(self):
    return len(self._actions_list)

  def validate_calls(self, input_file_list):
    # type: (List[str]) -> bool
    input_file_set = set(input_file_list)
    for one_call_actions in self._actions_list:
      start_len = len(input_file_set)
      for action in one_call_actions:
        for command_part in action['commands']:
          if command_part in input_file_set:
            input_file_set.remove(command_part)
      # Making sure that each actions for each call cover at least one file.
      if start_len == len(input_file_set):
        logging.error('None of input files appeared in %s or it was repeated',
                      str(one_call_actions))
        logging.error('List of remaining files: %s', str(input_file_set))
        return False
    if input_file_set:
      logging.error('Never ran on these files: %s', str(input_file_set))
      return False
    return True


class GetBaseNameTest(unittest.TestCase):
  def test_get_base_name(self):
    self.assertEqual('t.vcf', vep_runner._get_base_name('a/b/t.vcf'))
    self.assertEqual('t.vcf', vep_runner._get_base_name('/a/b/t.vcf'))
    self.assertEqual('t.vcf', vep_runner._get_base_name('gs://a/b/t.vcf'))
    self.assertEqual('t.vcf', vep_runner._get_base_name('a/b/t.vcf'))
