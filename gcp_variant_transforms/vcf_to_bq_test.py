# Copyright 2017 Google Inc.  All Rights Reserved.
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

"""Tests for vcf_to_bq script."""

import unittest

import collections
import mock

from apache_beam.io.filesystems import FileSystems

from gcp_variant_transforms.vcf_to_bq import _get_pipeline_mode
from gcp_variant_transforms.vcf_to_bq import PipelineModes
from gcp_variant_transforms.options.variant_transform_options import MergeOptions
from gcp_variant_transforms.vcf_to_bq import _get_variant_merge_strategy
from gcp_variant_transforms.libs.variant_merge import move_to_calls_strategy


class VcfToBqTest(unittest.TestCase):
  """Tests cases for the ``vcf_to_bq`` script."""

  def _create_mock_args(self, **args):
    return collections.namedtuple('MockArgs', args.keys())(*args.values())

  def test_get_mode_raises_error_for_no_match(self):
    args = self._create_mock_args(
        input_pattern='', optimize_for_large_inputs=False)

    with mock.patch.object(FileSystems, 'match', return_value=None), \
         self.assertRaises(ValueError):
      _get_pipeline_mode(args)

  def test_get_mode_optimize_set(self):
    args = self._create_mock_args(
        input_pattern='', optimize_for_large_inputs=True)

    self.assertEqual(_get_pipeline_mode(args), PipelineModes.LARGE)

  def test_get_mode_small(self):
    args = self._create_mock_args(
        input_pattern='', optimize_for_large_inputs=False)
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])
    match = match_result([None for _ in range(100)])

    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(_get_pipeline_mode(args), PipelineModes.SMALL)

  def test_get_mode_medium(self):
    args = self._create_mock_args(
        input_pattern='', optimize_for_large_inputs=False)
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])

    match = match_result(range(101))
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(_get_pipeline_mode(args), PipelineModes.MEDIUM)

    match = match_result(range(50000))
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(_get_pipeline_mode(args), PipelineModes.MEDIUM)

  def test_get_mode_large(self):
    args = self._create_mock_args(
        input_pattern='', optimize_for_large_inputs=False)
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])

    match = match_result(range(50001))
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(_get_pipeline_mode(args), PipelineModes.LARGE)

  def test_no_merge_strategy(self):
    args = self._create_mock_args(
        variant_merge_strategy=MergeOptions.NONE,
        info_keys_to_move_to_calls_regex=None,
        copy_quality_to_calls=None,
        copy_filter_to_calls=None)
    self.assertEqual(_get_variant_merge_strategy(args), None)

  def test_invalid_merge_strategy_raises_error(self):
    args = self._create_mock_args(
        variant_merge_strategy='NotAMergeStrategy',
        info_keys_to_move_to_calls_regex=None,
        copy_quality_to_calls=None,
        copy_filter_to_calls=None)
    with self.assertRaises(ValueError):
      _ = _get_variant_merge_strategy(args)

  def test_valid_merge_strategy(self):
    args = self._create_mock_args(
        variant_merge_strategy=MergeOptions.MOVE_TO_CALLS,
        info_keys_to_move_to_calls_regex=None,
        copy_quality_to_calls=None,
        copy_filter_to_calls=None)
    self.assertIsInstance(_get_variant_merge_strategy(args),
                          move_to_calls_strategy.MoveToCallsStrategy)
