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

"""Tests for pipeline_common script."""

import collections
import unittest

from apache_beam.io.filesystems import FileSystems
import mock

from gcp_variant_transforms import pipeline_common
from gcp_variant_transforms.pipeline_common import PipelineModes


class VcfToBqCommonTest(unittest.TestCase):
  """Tests cases for the `pipeline_common` script."""

  def _create_mock_args(self, **args):
    return collections.namedtuple('MockArgs', args.keys())(*args.values())

  def _get_pipeline_mode(self, args):
    return pipeline_common.get_pipeline_mode(args.input_pattern,
                                             args.optimize_for_large_inputs)

  def test_get_mode_raises_error_for_no_match(self):
    args = self._create_mock_args(
        input_pattern='', optimize_for_large_inputs=False)

    with mock.patch.object(FileSystems, 'match', return_value=None), \
         self.assertRaises(ValueError):
      self._get_pipeline_mode(args)

  def test_get_mode_optimize_set(self):
    args = self._create_mock_args(
        input_pattern='', optimize_for_large_inputs=True)

    self.assertEqual(self._get_pipeline_mode(args), PipelineModes.LARGE)

  def test_get_mode_small(self):
    args = self._create_mock_args(
        input_pattern='', optimize_for_large_inputs=False)
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])
    match = match_result([None for _ in range(100)])

    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(self._get_pipeline_mode(args), PipelineModes.SMALL)

  def test_get_mode_medium(self):
    args = self._create_mock_args(
        input_pattern='', optimize_for_large_inputs=False)
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])

    match = match_result(range(101))
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(self._get_pipeline_mode(args), PipelineModes.MEDIUM)

    match = match_result(range(50000))
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(self._get_pipeline_mode(args), PipelineModes.MEDIUM)

  def test_get_mode_large(self):
    args = self._create_mock_args(
        input_pattern='', optimize_for_large_inputs=False)
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])

    match = match_result(range(50001))
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(self._get_pipeline_mode(args), PipelineModes.LARGE)

  def test_default_optimize_for_large_inputs(self):
    args = self._create_mock_args(input_pattern='')
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])

    match = match_result(range(101))
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(pipeline_common.get_pipeline_mode(args.input_pattern),
                       PipelineModes.MEDIUM)

  def test_fail_on_invalid_flags(self):
    # Start with valid flags, without setup.py.
    pipeline_args = ['--project',
                     'gcp-variant-transforms-test',
                     '--staging_location',
                     'gs://integration_test_runs/staging']
    pipeline_common._raise_error_on_invalid_flags(pipeline_args)

    # Add Dataflow runner (requires --setup_file).
    pipeline_args.extend(['--runner', 'DataflowRunner'])
    with self.assertRaisesRegexp(ValueError, 'setup_file'):
      pipeline_common._raise_error_on_invalid_flags(pipeline_args)

    # Add setup.py (required for Variant Transforms run). This is now valid.
    pipeline_args.extend(['--setup_file', 'setup.py'])
    pipeline_common._raise_error_on_invalid_flags(pipeline_args)

    # Add an unknown flag.
    pipeline_args.extend(['--unknown_flag', 'somevalue'])
    with self.assertRaisesRegexp(ValueError, 'Unrecognized.*unknown_flag'):
      pipeline_common._raise_error_on_invalid_flags(pipeline_args)
