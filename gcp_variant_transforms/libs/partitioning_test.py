# Copyright 2020 Google Inc.  All Rights Reserved.
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
import unittest

import mock

from gcp_variant_transforms.libs import partitioning

class PartitioningTest(unittest.TestCase):
  def test_calculate_optimal_range_interval(self):
    range_end_to_expected_range_interval = {
        39980000: 10000,
        (39980000 - 1): 10000,
        (39980000 - 9999): 10000,
        39990000: 10010,
        40000000: 10010,
        40010000: 10010,
        40020000: 10020,
        40030000: 10020,
    }
    for range_end, expected_range_interval in (
        range_end_to_expected_range_interval.items()):
      (range_interval, range_end_enlarged) = (
          partitioning.calculate_optimal_range_interval(range_end))
      expected_range_end = (
          expected_range_interval * (partitioning._MAX_BQ_NUM_PARTITIONS - 1))
      self.assertEqual(expected_range_interval, range_interval)
      self.assertEqual(expected_range_end, range_end_enlarged)


  def test_calculate_optimal_range_interval_large(self):
    large_range_ends = [partitioning.MAX_RANGE_END,
                        partitioning.MAX_RANGE_END + 1,
                        partitioning.MAX_RANGE_END - 1,
                        partitioning.MAX_RANGE_END - 2 * pow(10, 4)]

    expected_interval = int(partitioning.MAX_RANGE_END /
                            float(partitioning._MAX_BQ_NUM_PARTITIONS))
    expected_end = partitioning.MAX_RANGE_END
    for large_range_end in large_range_ends:
      (range_interval, range_end_enlarged) = (
          partitioning.calculate_optimal_range_interval(large_range_end))
      self.assertEqual(expected_interval, range_interval)
      self.assertEqual(expected_end, range_end_enlarged)


class FlattenCallColumnTest(unittest.TestCase):
  """Test cases for class `FlattenCallColumn`."""

  @mock.patch('gcp_variant_transforms.libs.partitioning_test.partitioning.'
              'FlattenCallColumn._find_one_non_empty_table')
  def setUp(self, mock_find_non_empty):
    # We never query this table for running the following test, however, the
    # mock values are based on this table's schema. In other words:
    #   mock_columns.return_value = self._flatter._get_column_names()
    #   mock_sub_fields.return_value = self._flatter._get_call_sub_fields()
    input_base_table = ('gcp-variant-transforms-test:'
                        'bq_to_vcf_integration_tests.'
                        'merge_option_move_to_calls')
    self._flatter = partitioning.FlattenCallColumn(
        input_base_table, ['chr20'], False)
    # Set in mock_find_non_empty, ie mock of _find_one_non_empty_table()
    mock_find_non_empty.return_value = None
    self._flatter._schema_table_id = 'merge_option_move_to_calls__chr20'

  @mock.patch('gcp_variant_transforms.libs.partitioning_test.partitioning.'
              'FlattenCallColumn._get_column_names')
  @mock.patch('gcp_variant_transforms.libs.partitioning_test.partitioning.'
              'FlattenCallColumn._get_call_sub_fields')
  def test_get_flatten_column_names(self, mock_sub_fields, mock_columns):
    mock_columns.return_value = (
        ['reference_name', 'start_position', 'end_position', 'reference_bases',
         'alternate_bases', 'names', 'quality', 'filter', 'call', 'NS', 'DP',
         'AA', 'DB', 'H2'])
    mock_sub_fields.return_value = (
        ['sample_id', 'genotype', 'phaseset', 'DP', 'GQ', 'HQ'])
    expected_select = (
        'main_table.reference_name AS `reference_name`, '
        'main_table.start_position AS `start_position`, '
        'main_table.end_position AS `end_position`, '
        'main_table.reference_bases AS `reference_bases`, '
        'main_table.alternate_bases AS `alternate_bases`, '
        'main_table.names AS `names`, '
        'main_table.quality AS `quality`, '
        'main_table.filter AS `filter`, '
        'call_table.sample_id AS `sample_id`, '
        'STRUCT('
        'call_table.sample_id AS `sample_id`, '
        'call_table.genotype AS `genotype`, '
        'call_table.phaseset AS `phaseset`, '
        'call_table.DP AS `DP`, '
        'call_table.GQ AS `GQ`, '
        'call_table.HQ AS `HQ`'
        ') AS call, '
        'main_table.NS AS `NS`, '
        'main_table.DP AS `DP`, '
        'main_table.AA AS `AA`, '
        'main_table.DB AS `DB`, '
        'main_table.H2 AS `H2`')
    self.assertEqual(expected_select, self._flatter._get_flatten_column_names())
