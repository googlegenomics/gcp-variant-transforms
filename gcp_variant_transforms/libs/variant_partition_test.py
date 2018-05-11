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

"""Unit tests for variant_partition module."""

from __future__ import absolute_import

import unittest

from gcp_variant_transforms.libs import variant_partition


class VariantPartitionTest(unittest.TestCase):

  def test_auto_partitioning(self):
    partitioner = variant_partition.VariantPartition()
    self.assertTrue(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(),
                     variant_partition._DEFAULT_NUM_PARTITIONS)

    # Checking standard reference_name formatted as: 'chr[0-9][0-9]'
    for i in xrange(variant_partition._RESERVED_AUTO_PARTITIONS):
      self.assertEqual(partitioner.get_partition('chr' + str(i + 1)), i)
    # Checking standard reference_name formatted as: '[0-9][0-9]'
    for i in xrange(variant_partition._RESERVED_AUTO_PARTITIONS):
      self.assertEqual(partitioner.get_partition(str(i + 1)), i)

    # Every other reference_name will be assigned to partitions >= 22
    self.assertGreaterEqual(partitioner.get_partition('chrY'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    self.assertGreaterEqual(partitioner.get_partition('chrX'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    self.assertGreaterEqual(partitioner.get_partition('chrM'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    self.assertGreaterEqual(partitioner.get_partition('chr23'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    self.assertGreaterEqual(partitioner.get_partition('chr30'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    self.assertGreaterEqual(partitioner.get_partition('Unknown'),
                            variant_partition._RESERVED_AUTO_PARTITIONS)
    # Expected empty string as partition_name
    self.assertEqual(partitioner.get_partition_name(0), '')

  def test_auto_partitioning_invalid_partitions(self):
    partitioner = variant_partition.VariantPartition()
    self.assertTrue(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(),
                     variant_partition._DEFAULT_NUM_PARTITIONS)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('chr1', -1)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('', 1)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('  ', 1)

  def test_config_boundaries(self):
    partitioner = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config1.yaml")
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 8)
    self.assertEqual(partitioner.get_residual_partition_index(), 7)
    self.assertTrue(partitioner.has_residual_partition())

    # "chr1:0-1,000,000"
    self.assertEqual(partitioner.get_partition('chr1', 0), 0)
    self.assertEqual(partitioner.get_partition('chr1', 999999), 0)
    # "chr1:1,000,000-2,000,000"
    self.assertEqual(partitioner.get_partition('chr1', 1000000), 1)
    self.assertEqual(partitioner.get_partition('chr1', 1999999), 1)
    # "chr1:2,000,000-999,999,999"
    self.assertEqual(partitioner.get_partition('chr1', 2000000), 2)
    self.assertEqual(partitioner.get_partition('chr1', 999999998), 2)
    self.assertEqual(partitioner.get_partition('chr1', 999999999), 7)

    # "chr2" OR "ch2" OR "c2" OR "2"
    self.assertEqual(partitioner.get_partition('chr2', 0), 3)
    self.assertEqual(partitioner.get_partition('chr2', 999999999000), 3)
    self.assertEqual(partitioner.get_partition('ch2', 0), 3)
    self.assertEqual(partitioner.get_partition('ch2', 999999999000), 3)
    self.assertEqual(partitioner.get_partition('c2', 0), 3)
    self.assertEqual(partitioner.get_partition('c2', 999999999000), 3)
    self.assertEqual(partitioner.get_partition('2', 0), 3)
    self.assertEqual(partitioner.get_partition('2', 999999999000), 3)

    # "C4" OR "cr5" OR "chr6:1,000,000-2,000,000"
    self.assertEqual(partitioner.get_partition('c4', 0), 4)
    self.assertEqual(partitioner.get_partition('c4', 999999999000), 4)
    self.assertEqual(partitioner.get_partition('cr5', 0), 4)
    self.assertEqual(partitioner.get_partition('cr5', 999999999000), 4)
    self.assertEqual(partitioner.get_partition('chr6', 1000000), 4)
    self.assertEqual(partitioner.get_partition('chr6', 2000000 - 1), 4)
    self.assertEqual(partitioner.get_partition('chr6', 0), 7)
    self.assertEqual(partitioner.get_partition('chr6', 999999), 7)
    self.assertEqual(partitioner.get_partition('chr6', 2000000), 7)

    # "3:0-500,000"
    self.assertEqual(partitioner.get_partition('3', 0), 5)
    self.assertEqual(partitioner.get_partition('3', 499999), 5)
    # "3:500,000-1,000,000"
    self.assertEqual(partitioner.get_partition('3', 500000), 6)
    self.assertEqual(partitioner.get_partition('3', 999999), 6)
    self.assertEqual(partitioner.get_partition('3', 1000000), 7)

  def test_config_case_insensitive(self):
    partitioner = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config1.yaml")
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 8)
    self.assertEqual(partitioner.get_residual_partition_index(), 7)
    self.assertTrue(partitioner.has_residual_partition())

    # "chr1:0-1,000,000"
    self.assertEqual(partitioner.get_partition('chr1', 0), 0)
    self.assertEqual(partitioner.get_partition('Chr1', 0), 0)
    self.assertEqual(partitioner.get_partition('cHr1', 0), 0)
    self.assertEqual(partitioner.get_partition('chR1', 0), 0)
    self.assertEqual(partitioner.get_partition('CHr1', 0), 0)
    self.assertEqual(partitioner.get_partition('ChR1', 0), 0)
    self.assertEqual(partitioner.get_partition('cHR1', 0), 0)
    self.assertEqual(partitioner.get_partition('CHR1', 0), 0)

  def test_config_get_partition_name(self):
    partitioner = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config1.yaml")
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 8)
    self.assertEqual(partitioner.get_residual_partition_index(), 7)
    self.assertTrue(partitioner.has_residual_partition())

    self.assertEqual(partitioner.get_partition_name(0), '_chr01_part1')
    self.assertEqual(partitioner.get_partition_name(1), '_chr01_part2')
    self.assertEqual(partitioner.get_partition_name(2), '_chr01_part3')
    self.assertEqual(partitioner.get_partition_name(3), '_chrom02')
    self.assertEqual(partitioner.get_partition_name(4), '_chrom04_05_part_06')
    self.assertEqual(partitioner.get_partition_name(5), '_chr3_01')
    self.assertEqual(partitioner.get_partition_name(6), '_chr3_02')
    self.assertEqual(partitioner.get_partition_name(7), '_all_remaining')


  def test_config_non_existent_partition_name(self):
    partitioner = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config1.yaml")
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 8)

    with self.assertRaisesRegexp(
        ValueError, 'Partition index is outside of expected range.'):
      partitioner.get_partition_name(-1)
    with self.assertRaisesRegexp(
        ValueError, 'Partition index is outside of expected range.'):
      partitioner.get_partition_name(8)

  def test_config_residual_partition_in_middle(self):
    partitioner = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config2.yaml")
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 5)
    self.assertEqual(partitioner.get_residual_partition_index(), 1)
    self.assertTrue(partitioner.has_residual_partition())

    # "chr1:0-1,000,000"
    self.assertEqual(partitioner.get_partition('chr1', 0), 0)
    self.assertEqual(partitioner.get_partition('chr1', 999999), 0)
    # "chr1:1,000,000-2,000,000"
    self.assertEqual(partitioner.get_partition('chr1', 1000000), 2)
    self.assertEqual(partitioner.get_partition('chr1', 1999999), 2)
    # "chr2" OR "ch2" OR "c2" OR "2"
    self.assertEqual(partitioner.get_partition('chr2', 0), 3)
    self.assertEqual(partitioner.get_partition('chr2', 999999999000), 3)
    # "3:500,000-1,000,000"
    self.assertEqual(partitioner.get_partition('3', 500000), 4)
    self.assertEqual(partitioner.get_partition('3', 999999), 4)

    # All the followings are assigned to residual partition.
    self.assertEqual(partitioner.get_partition('chr1', 2000000),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('chr1', 999999999),
                     partitioner.get_residual_partition_index())

    self.assertEqual(partitioner.get_partition('3', 0),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('3', 499999),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('3', 1000000),
                     partitioner.get_residual_partition_index())

    self.assertEqual(partitioner.get_partition('ch2', 0),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('c2', 0),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('2', 0),
                     partitioner.get_residual_partition_index())

    self.assertEqual(partitioner.get_partition('c4', 0),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('cr5', 0),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('chr6', 0),
                     partitioner.get_residual_partition_index())

  def test_config_residual_partition_absent(self):
    partitioner = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config3.yaml")
    self.assertFalse(partitioner.should_flatten())
    self.assertEqual(partitioner.get_num_partitions(), 5)
    self.assertEqual(partitioner.get_residual_partition_index(), 5 - 1)
    self.assertFalse(partitioner.has_residual_partition())

    # "chr1:0-1,000,000"
    self.assertEqual(partitioner.get_partition('chr1', 0), 0)
    self.assertEqual(partitioner.get_partition('chr1', 999999), 0)
    # "chr1:1,000,000-2,000,000"
    self.assertEqual(partitioner.get_partition('chr1', 1000000), 1)
    self.assertEqual(partitioner.get_partition('chr1', 1999999), 1)
    # "chr2" OR "ch2" OR "c2" OR "2"
    self.assertEqual(partitioner.get_partition('chr2', 0), 2)
    self.assertEqual(partitioner.get_partition('chr2', 999999999000), 2)
    # "3:500,000-1,000,000"
    self.assertEqual(partitioner.get_partition('3', 500000), 3)
    self.assertEqual(partitioner.get_partition('3', 999999), 3)

    # All the followings are assigned to residual partition.
    self.assertEqual(partitioner.get_partition('chr1', 2000000),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('chr1', 999999999),
                     partitioner.get_residual_partition_index())

    self.assertEqual(partitioner.get_partition('3', 0),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('3', 499999),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('3', 1000000),
                     partitioner.get_residual_partition_index())

    self.assertEqual(partitioner.get_partition('ch2', 0),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('c2', 0),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('2', 0),
                     partitioner.get_residual_partition_index())

    self.assertEqual(partitioner.get_partition('c4', 0),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('cr5', 0),
                     partitioner.get_residual_partition_index())
    self.assertEqual(partitioner.get_partition('chr6', 0),
                     partitioner.get_residual_partition_index())

  def test_config_failed_config_validation(self):
    with self.assertRaisesRegexp(
        ValueError,
        'Each partition must have at least one region.'):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_missing_region.yaml")

    with self.assertRaisesRegexp(
        ValueError,
        'Each partition must have partition_name field.'):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_missing_partition_name.yaml")

    with self.assertRaisesRegexp(
        ValueError,
        'There must be only one residual partition intercepted at least 2'):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_redundant_default.yaml")

    with self.assertRaisesRegexp(
        ValueError, 'Cannot add overlapping region *'):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_overlapping_regions.yaml")

    with self.assertRaisesRegexp(
        ValueError,
        'Can not add region to an existing full chromosome.'):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_full_and_partial_chr.yaml")

    with self.assertRaisesRegexp(
        ValueError,
        'A full chromosome must be disjoint from all other regions'):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_partial_and_full_chr.yaml")

    with self.assertRaisesRegexp(
        ValueError,
        'A full chromosome must be disjoint from all other regions'):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_redundant_full_chr.yaml")

    with self.assertRaisesRegexp(
        ValueError,
        'Cannot add overlapping region *'):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_conflicting_regions.yaml")

    with self.assertRaisesRegexp(
        ValueError,
        'Table names must be unique *'):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_redundant_table_names.yaml")
