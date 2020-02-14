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

"""Unit tests for variant_sharding module."""

from __future__ import absolute_import

import unittest

from gcp_variant_transforms.libs import variant_sharding
from gcp_variant_transforms.testing import temp_dir

class VariantShardingTest(unittest.TestCase):

  def test_config_boundaries(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_at_end.yaml')
    self.assertEqual(sharder.get_num_shards(), 8)
    for i in range(sharder.get_num_shards()):
      self.assertTrue(sharder.should_keep_shard(i))

    # 'chr1:0-1,000,000'
    self.assertEqual(sharder.get_shard_index('chr1', 0), 0)
    self.assertEqual(sharder.get_shard_index('chr1', 999999), 0)
    # 'chr1:1,000,000-2,000,000'
    self.assertEqual(sharder.get_shard_index('chr1', 1000000), 1)
    self.assertEqual(sharder.get_shard_index('chr1', 1999999), 1)
    # 'chr1:2,000,000-999,999,999'
    self.assertEqual(sharder.get_shard_index('chr1', 2000000), 2)
    self.assertEqual(sharder.get_shard_index('chr1', 999999998), 2)
    self.assertEqual(sharder.get_shard_index('chr1', 999999999), 7)

    # 'chr2' OR 'chr2_alternate_name1' OR 'chr2_ALteRNate_NAME2' OR '2'.
    self.assertEqual(sharder.get_shard_index('chr2', 0), 3)
    self.assertEqual(sharder.get_shard_index('chr2', 999999999000), 3)
    self.assertEqual(
        sharder.get_shard_index('chr2_alternate_name1', 0), 3)
    self.assertEqual(
        sharder.get_shard_index('chr2_alternate_name1', 999999999000), 3)
    self.assertEqual(
        sharder.get_shard_index('chr2_ALteRNate_NAME2', 0), 3)
    self.assertEqual(sharder.get_shard_index('2', 0), 3)
    self.assertEqual(sharder.get_shard_index('2', 999999999000), 3)
    self.assertEqual(sharder.get_shard_index('CHR2', 0), 7)
    self.assertEqual(sharder.get_shard_index('chr2_alternate_name2', 0), 7)
    self.assertEqual(sharder.get_shard_index('CHR2_ALTERNATE_NAME2', 0), 7)


    # 'chr4' OR 'chr5' OR 'chr6:1,000,000-2,000,000'
    self.assertEqual(sharder.get_shard_index('chr4', 0), 4)
    self.assertEqual(sharder.get_shard_index('chr4', 999999999000), 4)
    self.assertEqual(sharder.get_shard_index('chr5', 0), 4)
    self.assertEqual(sharder.get_shard_index('chr5', 999999999000), 4)
    self.assertEqual(sharder.get_shard_index('chr6', 1000000), 4)
    self.assertEqual(sharder.get_shard_index('chr6', 2000000 - 1), 4)
    self.assertEqual(sharder.get_shard_index('chr6', 0), 7)
    self.assertEqual(sharder.get_shard_index('chr6', 999999), 7)
    self.assertEqual(sharder.get_shard_index('chr6', 2000000), 7)

    # '3:0-500,000'
    self.assertEqual(sharder.get_shard_index('3', 0), 5)
    self.assertEqual(sharder.get_shard_index('3', 499999), 5)
    # '3:500,000-1,000,000'
    self.assertEqual(sharder.get_shard_index('3', 500000), 6)
    self.assertEqual(sharder.get_shard_index('3', 999999), 6)
    self.assertEqual(sharder.get_shard_index('3', 1000000), 7)

  def test_config_case_sensitive(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_at_end.yaml')
    self.assertEqual(sharder.get_num_shards(), 8)
    for i in range(sharder.get_num_shards()):
      self.assertTrue(sharder.should_keep_shard(i))

    # 'chr1:0-1,000,000'
    self.assertEqual(sharder.get_shard_index('chr1', 0), 0)
    self.assertEqual(sharder.get_shard_index('Chr1', 0), 7)
    self.assertEqual(sharder.get_shard_index('CHr1', 0), 7)
    self.assertEqual(sharder.get_shard_index('CHR1', 0), 7)

  def test_config_get_output_table_suffix(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_at_end.yaml')
    self.assertEqual(sharder.get_num_shards(), 8)
    for i in range(sharder.get_num_shards()):
      self.assertTrue(sharder.should_keep_shard(i))

    self.assertEqual(sharder.get_output_table_suffix(0), 'chr01_part1')
    self.assertEqual(sharder.get_output_table_suffix(1), 'chr01_part2')
    self.assertEqual(sharder.get_output_table_suffix(2), 'chr01_part3')
    self.assertEqual(sharder.get_output_table_suffix(3), 'chrom02')
    self.assertEqual(sharder.get_output_table_suffix(4), 'chrom04_05_part_06')
    self.assertEqual(sharder.get_output_table_suffix(5), 'chr3_01')
    self.assertEqual(sharder.get_output_table_suffix(6), 'chr3_02')
    self.assertEqual(sharder.get_output_table_suffix(7), 'all_remaining')

  def test_config_get_total_base_pairs(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_at_end.yaml')
    self.assertEqual(sharder.get_num_shards(), 8)
    for i in range(sharder.get_num_shards()):
      self.assertTrue(sharder.should_keep_shard(i))

    self.assertEqual(sharder.get_output_table_total_base_pairs(0), 1000000)
    self.assertEqual(sharder.get_output_table_total_base_pairs(1), 2000000)
    self.assertEqual(sharder.get_output_table_total_base_pairs(2), 249240615)
    self.assertEqual(sharder.get_output_table_total_base_pairs(3), 243189284)
    self.assertEqual(sharder.get_output_table_total_base_pairs(4), 191044274)
    self.assertEqual(sharder.get_output_table_total_base_pairs(5), 500000)
    self.assertEqual(sharder.get_output_table_total_base_pairs(6), 1000000)
    self.assertEqual(sharder.get_output_table_total_base_pairs(7), 249240615)

  def test_config_non_existent_shard_name(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_at_end.yaml')
    self.assertEqual(sharder.get_num_shards(), 8)

    with self.assertRaisesRegexp(
        ValueError, 'Given shard index -1 is outside of expected range*'):
      sharder.get_output_table_suffix(-1)
    with self.assertRaisesRegexp(
        ValueError, 'Given shard index 8 is outside of expected range*'):
      sharder.get_output_table_suffix(8)

  def test_config_residual_shard_in_middle(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_in_middle.yaml')
    self.assertEqual(sharder.get_num_shards(), 5)
    for i in range(sharder.get_num_shards()):
      self.assertTrue(sharder.should_keep_shard(i))

    # 'chr1:0-1,000,000'
    self.assertEqual(sharder.get_shard_index('chr1', 0), 0)
    self.assertEqual(sharder.get_shard_index('chr1', 999999), 0)
    # 'chr1:1,000,000-2,000,000'
    self.assertEqual(sharder.get_shard_index('chr1', 1000000), 2)
    self.assertEqual(sharder.get_shard_index('chr1', 1999999), 2)
    # 'chr2' OR 'ch2' OR 'c2' OR '2'
    self.assertEqual(sharder.get_shard_index('chr2', 0), 3)
    self.assertEqual(sharder.get_shard_index('chr2', 999999999000), 3)
    # '3:500,000-1,000,000'
    self.assertEqual(sharder.get_shard_index('3', 500000), 4)
    self.assertEqual(sharder.get_shard_index('3', 999999), 4)

    # All the followings are assigned to residual shard.
    self.assertEqual(sharder.get_shard_index('chr1', 2000000), 1)
    self.assertEqual(sharder.get_shard_index('chr1', 999999999), 1)
    self.assertEqual(sharder.get_shard_index('cHr1', 0), 1)
    self.assertEqual(sharder.get_shard_index('CHR1', 0), 1)

    self.assertEqual(sharder.get_shard_index('3', 0), 1)
    self.assertEqual(sharder.get_shard_index('3', 499999), 1)
    self.assertEqual(sharder.get_shard_index('3', 1000000), 1)

    self.assertEqual(sharder.get_shard_index('ch2', 0), 1)
    self.assertEqual(sharder.get_shard_index('c2', 0), 1)
    self.assertEqual(sharder.get_shard_index('2', 0), 1)

    self.assertEqual(sharder.get_shard_index('c4', 0), 1)
    self.assertEqual(sharder.get_shard_index('cr5', 0), 1)
    self.assertEqual(sharder.get_shard_index('chr6', 0), 1)

  def test_config_residual_shard_absent(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_missing.yaml')
    self.assertEqual(sharder.get_num_shards(), 5)
    # All shards excpet the last one (dummy residual) should be kept.
    for i in range(sharder.get_num_shards() - 1):
      self.assertTrue(sharder.should_keep_shard(i))
    self.assertFalse(sharder.should_keep_shard(5 - 1))

    # 'chr1:0-1,000,000'
    self.assertEqual(sharder.get_shard_index('chr1', 0), 0)
    self.assertEqual(sharder.get_shard_index('chr1', 999999), 0)
    # 'chr1:1,000,000-2,000,000'
    self.assertEqual(sharder.get_shard_index('chr1', 1000000), 1)
    self.assertEqual(sharder.get_shard_index('chr1', 1999999), 1)
    # 'chr2' OR 'ch2' OR 'c2' OR '2'
    self.assertEqual(sharder.get_shard_index('chr2', 0), 2)
    self.assertEqual(sharder.get_shard_index('chr2', 999999999000), 2)
    # '3:500,000-1,000,000'
    self.assertEqual(sharder.get_shard_index('3', 500000), 3)
    self.assertEqual(sharder.get_shard_index('3', 999999), 3)

    # All the followings are assigned to residual shard.
    self.assertEqual(sharder.get_shard_index('chr1', 2000000), 4)
    self.assertEqual(sharder.get_shard_index('chr1', 999999999), 4)
    self.assertEqual(sharder.get_shard_index('cHr1', 0), 4)
    self.assertEqual(sharder.get_shard_index('CHR1', 0), 4)

    self.assertEqual(sharder.get_shard_index('3', 0), 4)
    self.assertEqual(sharder.get_shard_index('3', 499999), 4)
    self.assertEqual(sharder.get_shard_index('3', 1000000), 4)

    self.assertEqual(sharder.get_shard_index('ch2', 0), 4)
    self.assertEqual(sharder.get_shard_index('c2', 0), 4)
    self.assertEqual(sharder.get_shard_index('2', 0), 4)

    self.assertEqual(sharder.get_shard_index('c4', 0), 4)
    self.assertEqual(sharder.get_shard_index('cr5', 0), 4)
    self.assertEqual(sharder.get_shard_index('chr6', 0), 4)

  def test_config_failed_missing_region(self):
    tempdir = temp_dir.TempDir()
    missing_region = [
        '-  output_table:',
        '     table_name_suffix: "chr01_part1"',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "all_remaining"',
        '     regions:',
        '       - "residual"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "missing_region"',
        '     regions:',
        '     total_base_pairs: 999999999',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, regions field missing.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_region)))

  def test_config_failed_missing_shard_name(self):
    tempdir = temp_dir.TempDir()
    missing_par_name = [
        '-  output_table:',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '     total_base_pairs: 999999999',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, table_name_suffix field missing.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_par_name)))
    empty_par_name = [
        '-  output_table:',
        '     table_name_suffix: "          "',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '     total_base_pairs: 999999999',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, table_name_suffix can not be empty.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(empty_par_name)))

  def test_config_failed_duplicate_residual_shard(self):
    tempdir = temp_dir.TempDir()
    duplicate_residual = [
        '-  output_table:',
        '     table_name_suffix: "all_remaining"',
        '     regions:',
        '       - "residual"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "chr01"',
        '     regions:',
        '       - "chr1"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "all_remaining_2"',
        '     regions:',
        '       - "residual"',
        '     total_base_pairs: 999999999',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, there can be only one residual output*'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(duplicate_residual)))

  def test_config_failed_overlapping_regions(self):
    tempdir = temp_dir.TempDir()
    overlapping_regions = [
        '-  output_table:',
        '     table_name_suffix: "chr01_part1"',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "chr01_part2_overlapping"',
        '     regions:',
        '       - "chr1:999,999-2,000,000"',
        '     total_base_pairs: 999999999',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Wrong sharding config file, regions must be unique*'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(overlapping_regions)))

    full_and_partial = [
        '-  output_table:',
        '     table_name_suffix: "chr01_full"',
        '     regions:',
        '       - "chr1"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "chr01_part_overlapping"',
        '     regions:',
        '       - "chr1:1,000,000-2,000,000"',
        '     total_base_pairs: 999999999',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Wrong sharding config file, regions must be unique*'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(full_and_partial)))

    partial_and_full = [
        '-  output_table:',
        '     table_name_suffix: "chr01_part"',
        '     regions:',
        '       - "chr1:1,000,000-2,000,000"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "chr01_full_overlapping"',
        '     regions:',
        '       - "chr1"',
        '     total_base_pairs: 999999999',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Wrong sharding config file, regions must be unique*'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(partial_and_full)))

    full_and_full = [
        '-  output_table:',
        '     table_name_suffix: "chr01_full"',
        '     regions:',
        '       - "chr1"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "chr02_part"',
        '     regions:',
        '       - "chr2:1,000,000-2,000,000"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "chr01_full_redundant"',
        '     regions:',
        '       - "chr1"',
        '     total_base_pairs: 999999999',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Wrong sharding config file, regions must be unique*'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(full_and_full)))

  def test_config_failed_duplicate_table_name(self):
    tempdir = temp_dir.TempDir()
    dup_table_name = [
        '-  output_table:',
        '     table_name_suffix: "duplicate_name"',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "all_remaining"',
        '     regions:',
        '       - "residual"',
        '     total_base_pairs: 999999999',
        '-  output_table:',
        '     table_name_suffix: "duplicate_name"',
        '     regions:',
        '       - "chr1:1,000,000-2,000,000"',
        '     total_base_pairs: 999999999',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, table name suffixes must be unique*'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(dup_table_name)))

  def test_config_failed_missing_fields(self):
    tempdir = temp_dir.TempDir()
    missing_output_table = [
        '-  missing___output_table:',
        '     table_name_suffix: "chr1"',
        '     regions:',
        '       - "chr1"',
        '       - "1"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharing config file, output_table field missing.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_output_table)))

    missing_table_name_suffix = [
        '-  output_table:',
        '     regions:',
        '       - "chr1"',
        '       - "1"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, table_name_suffix field missing.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_table_name_suffix)))

    missing_chrom_values = [
        '-  output_table:',
        '     table_name_suffix: "chr1"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, regions field missing.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_chrom_values)))

    missing_filters = [
        '-  output_table:',
        '     table_name_suffix: "chr1"',
        '     regions:',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, regions field missing.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_filters)))

    missing_total_base_pairs = [
        '-  output_table:',
        '     table_name_suffix: "chr1"',
        '     regions:',
        '       - "chr1"',
        '       - "1"',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, total_base_pairs field missing.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_total_base_pairs)))

  def test_config_failed_wrong_fields(self):
    tempdir = temp_dir.TempDir()
    empty_suffix = [
        '-  output_table:',
        '     table_name_suffix: " "',
        '     regions:',
        '       - "chr1"',
        '       - "1"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, table_name_suffix can not be empty.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(empty_suffix)))
    tempdir = temp_dir.TempDir()

    wrong_table_name = [
        '-  output_table:',
        '     table_name_suffix: "chr#"',
        '     regions:',
        '       - "chr1"',
        '       - "1"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, BigQuery table name can only contain *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(wrong_table_name)))
    tempdir = temp_dir.TempDir()

    duplicate_suffix = [
        '-  output_table:',
        '     table_name_suffix: "chr1"',
        '     regions:',
        '       - "chr1"',
        '     total_base_pairs: 249240615',
        '-  output_table:',
        '     table_name_suffix: "chr1"',
        '     regions:',
        '       - "chr2"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, table name suffixes must be unique*'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(duplicate_suffix)))
    tempdir = temp_dir.TempDir()

    empty_chrom_value = [
        '-  output_table:',
        '     table_name_suffix: "chr1"',
        '     regions:',
        '       - "chr1"',
        '       - " "',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, reference_name can not be empty string: '):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(empty_chrom_value)))
    tempdir = temp_dir.TempDir()

    duplicate_chrom_value1 = [
        '-  output_table:',
        '     table_name_suffix: "chr1"',
        '     regions:',
        '       - "dup_value"',
        '       - "dup_value"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, regions must be unique in config file: *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(duplicate_chrom_value1)))

    duplicate_chrom_value2 = [
        '-  output_table:',
        '     table_name_suffix: "chr1"',
        '     regions:',
        '       - "dup_value"',
        '     total_base_pairs: 249240615',
        '-  output_table:',
        '     table_name_suffix: "chr2"',
        '     regions:',
        '       - "dup_value"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, regions must be unique in config file: *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(duplicate_chrom_value2)))

    duplicate_residual = [
        '-  output_table:',
        '     table_name_suffix: "residual1"',
        '     regions:',
        '       - "residual"',
        '     total_base_pairs: 249240615',
        '-  output_table:',
        '     table_name_suffix: "residual2"',
        '     regions:',
        '       - "residual"',
        '     total_base_pairs: 249240615'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, there can be only one residual output *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(duplicate_residual)))

    not_int_total_base_pairs = [
        '-  output_table:',
        '     table_name_suffix: "chr1"',
        '     regions:',
        '       - "chr1"',
        '       - "1"',
        '     total_base_pairs: "not int"'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, each output table needs an integer for *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(not_int_total_base_pairs)))

    not_pos_total_base_pairs = [
        '-  output_table:',
        '     table_name_suffix: "chr1"',
        '     regions:',
        '       - "chr1"',
        '       - "1"',
        '     total_base_pairs: -10'
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Wrong sharding config file, each output table needs an integer for *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(not_pos_total_base_pairs)))
