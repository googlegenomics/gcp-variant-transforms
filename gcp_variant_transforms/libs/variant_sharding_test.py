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

class VariantPartitionTest(unittest.TestCase):

  def test_auto_sharding(self):
    sharder = variant_sharding.VariantSharding()
    self.assertTrue(sharder.should_flatten())
    self.assertEqual(sharder.get_num_shards(),
                     variant_sharding._DEFAULT_NUM_SHARDS)

    # Checking standard reference_name formatted as: 'chr[0-9][0-9]'
    for i in xrange(variant_sharding._RESERVED_AUTO_SHARDS):
      self.assertEqual(sharder.get_shard('chr' + str(i + 1)), i)
    # Checking standard reference_name formatted as: '[0-9][0-9]'
    for i in xrange(variant_sharding._RESERVED_AUTO_SHARDS):
      self.assertEqual(sharder.get_shard(str(i + 1)), i)

    # Every other reference_name will be assigned to shards >= 22
    self.assertGreaterEqual(sharder.get_shard('chrY'),
                            variant_sharding._RESERVED_AUTO_SHARDS)
    self.assertGreaterEqual(sharder.get_shard('chrX'),
                            variant_sharding._RESERVED_AUTO_SHARDS)
    self.assertGreaterEqual(sharder.get_shard('chrM'),
                            variant_sharding._RESERVED_AUTO_SHARDS)
    self.assertGreaterEqual(sharder.get_shard('chr23'),
                            variant_sharding._RESERVED_AUTO_SHARDS)
    self.assertGreaterEqual(sharder.get_shard('chr30'),
                            variant_sharding._RESERVED_AUTO_SHARDS)
    self.assertGreaterEqual(sharder.get_shard('Unknown'),
                            variant_sharding._RESERVED_AUTO_SHARDS)
    # Expected empty string as partition_name as we are in auto mode.
    self.assertEqual(sharder.get_shard_name(0), None)
    self.assertEqual(sharder.get_shard_name(100), None)


  def test_auto_sharding_invalid_shards(self):
    sharder = variant_sharding.VariantSharding()
    self.assertTrue(sharder.should_flatten())
    self.assertEqual(sharder.get_num_shards(),
                     variant_sharding._DEFAULT_NUM_SHARDS)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      sharder.get_shard('chr1', -1)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      sharder.get_shard('', 1)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      sharder.get_shard('  ', 1)

  def test_config_boundaries(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_at_end.yaml')
    self.assertFalse(sharder.should_flatten())
    self.assertEqual(sharder.get_num_shards(), 8)
    for i in range(sharder.get_num_shards()):
      self.assertTrue(sharder.should_keep_shard(i))

    # 'chr1:0-1,000,000'
    self.assertEqual(sharder.get_shard('chr1', 0), 0)
    self.assertEqual(sharder.get_shard('chr1', 999999), 0)
    # 'chr1:1,000,000-2,000,000'
    self.assertEqual(sharder.get_shard('chr1', 1000000), 1)
    self.assertEqual(sharder.get_shard('chr1', 1999999), 1)
    # 'chr1:2,000,000-999,999,999'
    self.assertEqual(sharder.get_shard('chr1', 2000000), 2)
    self.assertEqual(sharder.get_shard('chr1', 999999998), 2)
    self.assertEqual(sharder.get_shard('chr1', 999999999), 7)

    # 'chr2' OR 'chr2_alternate_name1' OR 'chr2_alternate_name2' OR '2'.
    self.assertEqual(sharder.get_shard('chr2', 0), 3)
    self.assertEqual(sharder.get_shard('chr2', 999999999000), 3)
    self.assertEqual(
        sharder.get_shard('chr2_alternate_name1', 0), 3)
    self.assertEqual(
        sharder.get_shard('chr2_alternate_name1', 999999999000), 3)
    self.assertEqual(sharder.get_shard('chr2_alternate_name2', 0), 3)
    self.assertEqual(
        sharder.get_shard('CHR2_ALTERNATE_NAME2', 999999999000), 3)
    self.assertEqual(sharder.get_shard('2', 0), 3)
    self.assertEqual(sharder.get_shard('2', 999999999000), 3)

    # 'chr4' OR 'chr5' OR 'chr6:1,000,000-2,000,000'
    self.assertEqual(sharder.get_shard('chr4', 0), 4)
    self.assertEqual(sharder.get_shard('chr4', 999999999000), 4)
    self.assertEqual(sharder.get_shard('chr5', 0), 4)
    self.assertEqual(sharder.get_shard('chr5', 999999999000), 4)
    self.assertEqual(sharder.get_shard('chr6', 1000000), 4)
    self.assertEqual(sharder.get_shard('chr6', 2000000 - 1), 4)
    self.assertEqual(sharder.get_shard('chr6', 0), 7)
    self.assertEqual(sharder.get_shard('chr6', 999999), 7)
    self.assertEqual(sharder.get_shard('chr6', 2000000), 7)

    # '3:0-500,000'
    self.assertEqual(sharder.get_shard('3', 0), 5)
    self.assertEqual(sharder.get_shard('3', 499999), 5)
    # '3:500,000-1,000,000'
    self.assertEqual(sharder.get_shard('3', 500000), 6)
    self.assertEqual(sharder.get_shard('3', 999999), 6)
    self.assertEqual(sharder.get_shard('3', 1000000), 7)

  def test_config_case_insensitive(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_at_end.yaml')
    self.assertFalse(sharder.should_flatten())
    self.assertEqual(sharder.get_num_shards(), 8)
    for i in range(sharder.get_num_shards()):
      self.assertTrue(sharder.should_keep_shard(i))

    # 'chr1:0-1,000,000'
    self.assertEqual(sharder.get_shard('chr1', 0), 0)
    self.assertEqual(sharder.get_shard('Chr1', 0), 0)
    self.assertEqual(sharder.get_shard('CHr1', 0), 0)
    self.assertEqual(sharder.get_shard('CHR1', 0), 0)

  def test_config_get_shard_name(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_at_end.yaml')
    self.assertFalse(sharder.should_flatten())
    self.assertEqual(sharder.get_num_shards(), 8)
    for i in range(sharder.get_num_shards()):
      self.assertTrue(sharder.should_keep_shard(i))

    self.assertEqual(sharder.get_shard_name(0), 'chr01_part1')
    self.assertEqual(sharder.get_shard_name(1), 'chr01_part2')
    self.assertEqual(sharder.get_shard_name(2), 'chr01_part3')
    self.assertEqual(sharder.get_shard_name(3), 'chrom02')
    self.assertEqual(sharder.get_shard_name(4), 'chrom04_05_part_06')
    self.assertEqual(sharder.get_shard_name(5), 'chr3_01')
    self.assertEqual(sharder.get_shard_name(6), 'chr3_02')
    self.assertEqual(sharder.get_shard_name(7), 'all_remaining')


  def test_config_non_existent_shard_name(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_at_end.yaml')
    self.assertFalse(sharder.should_flatten())
    self.assertEqual(sharder.get_num_shards(), 8)

    with self.assertRaisesRegexp(
        ValueError, 'Given shard index -1 is outside of expected range*'):
      sharder.get_shard_name(-1)
    with self.assertRaisesRegexp(
        ValueError, 'Given shard index 8 is outside of expected range*'):
      sharder.get_shard_name(8)

  def test_config_residual_shard_in_middle(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_in_middle.yaml')
    self.assertFalse(sharder.should_flatten())
    self.assertEqual(sharder.get_num_shards(), 5)
    for i in range(sharder.get_num_shards()):
      self.assertTrue(sharder.should_keep_shard(i))

    # 'chr1:0-1,000,000'
    self.assertEqual(sharder.get_shard('chr1', 0), 0)
    self.assertEqual(sharder.get_shard('chr1', 999999), 0)
    # 'chr1:1,000,000-2,000,000'
    self.assertEqual(sharder.get_shard('chr1', 1000000), 2)
    self.assertEqual(sharder.get_shard('chr1', 1999999), 2)
    # 'chr2' OR 'ch2' OR 'c2' OR '2'
    self.assertEqual(sharder.get_shard('chr2', 0), 3)
    self.assertEqual(sharder.get_shard('chr2', 999999999000), 3)
    # '3:500,000-1,000,000'
    self.assertEqual(sharder.get_shard('3', 500000), 4)
    self.assertEqual(sharder.get_shard('3', 999999), 4)

    # All the followings are assigned to residual shard.
    self.assertEqual(sharder.get_shard('chr1', 2000000), 1)
    self.assertEqual(sharder.get_shard('chr1', 999999999), 1)

    self.assertEqual(sharder.get_shard('3', 0), 1)
    self.assertEqual(sharder.get_shard('3', 499999), 1)
    self.assertEqual(sharder.get_shard('3', 1000000), 1)

    self.assertEqual(sharder.get_shard('ch2', 0), 1)
    self.assertEqual(sharder.get_shard('c2', 0), 1)
    self.assertEqual(sharder.get_shard('2', 0), 1)

    self.assertEqual(sharder.get_shard('c4', 0), 1)
    self.assertEqual(sharder.get_shard('cr5', 0), 1)
    self.assertEqual(sharder.get_shard('chr6', 0), 1)

  def test_config_residual_shard_absent(self):
    sharder = variant_sharding.VariantSharding(
        'gcp_variant_transforms/testing/data/sharding_configs/'
        'residual_missing.yaml')
    self.assertFalse(sharder.should_flatten())
    self.assertEqual(sharder.get_num_shards(), 5)
    # All shards excpet the last one (dummy residual) should be kept.
    for i in range(sharder.get_num_shards() - 1):
      self.assertTrue(sharder.should_keep_shard(i))
    self.assertFalse(sharder.should_keep_shard(5 - 1))

    # 'chr1:0-1,000,000'
    self.assertEqual(sharder.get_shard('chr1', 0), 0)
    self.assertEqual(sharder.get_shard('chr1', 999999), 0)
    # 'chr1:1,000,000-2,000,000'
    self.assertEqual(sharder.get_shard('chr1', 1000000), 1)
    self.assertEqual(sharder.get_shard('chr1', 1999999), 1)
    # 'chr2' OR 'ch2' OR 'c2' OR '2'
    self.assertEqual(sharder.get_shard('chr2', 0), 2)
    self.assertEqual(sharder.get_shard('chr2', 999999999000), 2)
    # '3:500,000-1,000,000'
    self.assertEqual(sharder.get_shard('3', 500000), 3)
    self.assertEqual(sharder.get_shard('3', 999999), 3)

    # All the followings are assigned to residual shard.
    self.assertEqual(sharder.get_shard('chr1', 2000000), 4)
    self.assertEqual(sharder.get_shard('chr1', 999999999), 4)

    self.assertEqual(sharder.get_shard('3', 0), 4)
    self.assertEqual(sharder.get_shard('3', 499999), 4)
    self.assertEqual(sharder.get_shard('3', 1000000), 4)

    self.assertEqual(sharder.get_shard('ch2', 0), 4)
    self.assertEqual(sharder.get_shard('c2', 0), 4)
    self.assertEqual(sharder.get_shard('2', 0), 4)

    self.assertEqual(sharder.get_shard('c4', 0), 4)
    self.assertEqual(sharder.get_shard('cr5', 0), 4)
    self.assertEqual(sharder.get_shard('chr6', 0), 4)

  def test_config_failed_missing_region(self):
    tempdir = temp_dir.TempDir()
    missing_region = [
        '-  partition:',
        '     partition_name: "chr01_part1"',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '-  partition:',
        '     partition_name: "all_remaining"',
        '     regions:',
        '       - "residual"',
        '-  partition:',
        '     partition_name: "missing_region"',
        '     regions:',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Each shard must have at least one region.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_region)))

  def test_config_failed_missing_shard_name(self):
    tempdir = temp_dir.TempDir()
    missing_par_name = [
        '-  partition:',
        '     regions:',
        '       - "chr1:0-1,000,000"',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Each shard must have partition_name field.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(missing_par_name)))
    empty_par_name = [
        '-  partition:',
        '     partition_name: "          "',
        '     regions:',
        '       - "chr1:0-1,000,000"',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Shard name can not be empty string.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(empty_par_name)))

  def test_config_failed_duplicate_residual_shard(self):
    tempdir = temp_dir.TempDir()
    duplicate_residual = [
        '-  partition:',
        '     partition_name: "all_remaining"',
        '     regions:',
        '       - "residual"',
        '-  partition:',
        '     partition_name: "chr01"',
        '     regions:',
        '       - "chr1"',
        '-  partition:',
        '     partition_name: "all_remaining_2"',
        '     regions:',
        '       - "residual"',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'There must be only one residual shard.'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(duplicate_residual)))

  def test_config_failed_overlapping_regions(self):
    tempdir = temp_dir.TempDir()
    overlapping_regions = [
        '-  partition:',
        '     partition_name: "chr01_part1"',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '-  partition:',
        '     partition_name: "chr01_part2_overlapping"',
        '     regions:',
        '       - "chr1:999,999-2,000,000"',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Cannot add overlapping region *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(overlapping_regions)))

    full_and_partial = [
        '-  partition:',
        '     partition_name: "chr01_full"',
        '     regions:',
        '       - "chr1"',
        '-  partition:',
        '     partition_name: "chr01_part_overlapping"',
        '     regions:',
        '       - "chr1:1,000,000-2,000,000"',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Cannot add overlapping region *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(full_and_partial)))

    partial_and_full = [
        '-  partition:',
        '     partition_name: "chr01_part"',
        '     regions:',
        '       - "chr1:1,000,000-2,000,000"',
        '-  partition:',
        '     partition_name: "chr01_full_overlapping"',
        '     regions:',
        '       - "chr1"',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Cannot add overlapping region *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(partial_and_full)))

    full_and_full = [
        '-  partition:',
        '     partition_name: "chr01_full"',
        '     regions:',
        '       - "chr1"',
        '-  partition:',
        '     partition_name: "chr02_part"',
        '     regions:',
        '       - "chr2:1,000,000-2,000,000"',
        '-  partition:',
        '     partition_name: "chr01_full_redundant"',
        '     regions:',
        '       - "chr1"',
    ]
    with self.assertRaisesRegexp(
        ValueError, 'Cannot add overlapping region *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(full_and_full)))

  def test_config_failed_duplicate_table_name(self):
    tempdir = temp_dir.TempDir()
    dup_table_name = [
        '-  partition:',
        '     partition_name: "duplicate_name"',
        '     regions:',
        '       - "chr1:0-1,000,000"',
        '-  partition:',
        '     partition_name: "all_remaining"',
        '     regions:',
        '       - "residual"',
        '-  partition:',
        '     partition_name: "duplicate_name"',
        '     regions:',
        '       - "chr1:1,000,000-2,000,000"',
    ]
    with self.assertRaisesRegexp(
        ValueError,
        'Shard names must be unique *'):
      _ = variant_sharding.VariantSharding(
          tempdir.create_temp_file(suffix='.yaml',
                                   lines='\n'.join(dup_table_name)))
