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

import mock

from mock import patch

from apache_beam.io import filesystems
from gcp_variant_transforms.libs.annotation import vep_runner


# TODO(bashir2): Create tests with non local inputs as well.
_INPUT_PATTERN = 'some/input/pattern*'
_INPUT_FILES_WITH_SIZE = [
    ('some/input/pattern/a', 100),
    ('some/input/pattern/b', 100),
    ('some/input/pattern/c', 100),
    ('some/input/pattern/dir1/a', 100),
    ('some/input/pattern/dir1/dir2/b', 100),
    ('some/input/pattern/dir2/b', 100),
]
_OUTPUT_DIR = 'gs://output/dir'
_IMAGE = 'gcr.io/image'
_CACHE = 'path/to/cache'
_NUM_FORK = 8
_PROJECT = 'test-project'
_REGION = 'test-region'
_NUM_WORKERS = '10'
_PIPELINE_ARGS = ['--project', _PROJECT,
                  '--region', _REGION,
                  '--max_num_workers', _NUM_WORKERS,
                  '--worker_machine_type', 'n1-standard-8',
                 ]


class _MockFileMetadata(object):
  def __init__(self, path, size_in_bytes):
    self.path = path
    self.size_in_bytes = size_in_bytes


class _MockFileSystems(filesystems.FileSystems):
  """This inherits from FileSystems such that most functions behave the same."""

  @staticmethod
  def match(patterns, limits=None):
    if len(patterns) == 1 and patterns[0] == _INPUT_PATTERN:
      return [mock.Mock(
          metadata_list=[_MockFileMetadata(path, size) for
                         (path, size) in _INPUT_FILES_WITH_SIZE])]
    return []

  @staticmethod
  def create(path, mime_type=None, compression_type=None):
    """Overriding `create` to remove any interaction with real file systems."""
    return mock.Mock()


class VepRunnerTest(unittest.TestCase):

  def _create_test_instance(self):
    default_cred = 12  # An arbitrary value just to check proper call chains.
    self._mock_service = mock.Mock()  # pylint: disable=attribute-defined-outside-init
    self._mock_pipelines = mock.Mock()  # pylint: disable=attribute-defined-outside-init
    self._mock_request = mock.Mock()  # pylint: disable=attribute-defined-outside-init
    self._mock_service.pipelines = mock.Mock(return_value=self._mock_pipelines)
    self._mock_pipelines.run = mock.Mock(return_value=self._mock_request)
    self._mock_request.execute = mock.Mock(return_value={'name': 'operation'})
    mock_service_builder = mock.Mock(return_value=self._mock_service)
    mock_app_default = mock.Mock(return_value=default_cred)
    # TODO: It is better to pass a `service` object to VepRunner instead of
    # this patch gymnastic to create a `mock_service`. A factor method can be
    # provided to create instances of VepRunner. This needs to be decided in
    # the review process and updated before submit.
    with patch('googleapiclient.discovery.build', mock_service_builder):
      with patch(
          'oauth2client.client.GoogleCredentials.get_application_default',
          mock_app_default):
        test_object = vep_runner.VepRunner(
            _INPUT_PATTERN, _OUTPUT_DIR, _IMAGE, _CACHE, _NUM_FORK,
            _PIPELINE_ARGS)
    mock_service_builder.assert_called_once_with(
        'genomics', 'v2alpha1', credentials=default_cred)
    return test_object

  def test_instantiation(self):
    """This is just to test object construction."""
    self._create_test_instance()

  def test_get_output_pattern(self):
    output_pattern = self._create_test_instance().get_output_pattern()
    self.assertEqual(output_pattern, _OUTPUT_DIR + '/**_vep_output.vcf')

  def test_run_on_all_files(self):
    test_instance = self._create_test_instance()
    with patch('apache_beam.io.filesystems.FileSystems', _MockFileSystems):
      test_instance.run_on_all_files()
    all_call_args = self._mock_pipelines.run.call_args_list
    self.assertEqual(len(all_call_args), len(_INPUT_FILES_WITH_SIZE))
    # TODO(bashir2): Add assertions about call arguments.


# TODO: Add more tests and validations after the first round of review.
