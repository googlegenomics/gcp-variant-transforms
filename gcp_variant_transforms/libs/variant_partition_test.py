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

"""Unit tests for metrics_util module."""

from __future__ import absolute_import

import unittest

from gcp_variant_transforms.libs import variant_partition
from gcp_variant_transforms.libs.variant_partition import  \
  _DEFAULT_PARTITION_NO as default_partitions_no
from gcp_variant_transforms.libs.variant_partition import \
  _RESERVED_PARTITIONS as reserved_partitions


class VariantPartitionTest(unittest.TestCase):

  def test_no_config(self):
    config = variant_partition.VariantPartition()
    self.assertTrue(isinstance(config, variant_partition.VariantPartition))
    self.assertEqual(config.get_num_partitions(), default_partitions_no)
    self.assertEqual(config.get_default_partition_index(), -1)
    self.assertFalse(config.is_default_partition_absent())

    # Checking standard reference_name formatted as: 'chr[0-9][0-9]'
    for i in xrange(reserved_partitions):
      self.assertEqual(config.get_partition('chr'+str(i+1)), i)
    # Checking standard reference_name formatted as: '[0-9][0-9]'
    for i in xrange(reserved_partitions):
      self.assertEqual(config.get_partition(str(i+1)), i)

    # Every other reference_name will be assigned to partitions >= 22
    self.assertGreaterEqual(config.get_partition('chrY'), reserved_partitions)
    self.assertGreaterEqual(config.get_partition('chrX'), reserved_partitions)
    self.assertGreaterEqual(config.get_partition('chrM'), reserved_partitions)
    self.assertGreaterEqual(config.get_partition('chr23'), reserved_partitions)
    self.assertGreaterEqual(config.get_partition('chr30'), reserved_partitions)
    self.assertGreaterEqual(config.get_partition('Unknown'),
                            reserved_partitions)

  def test_config_boundries(self):
    config = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config1.yaml")
    self.assertTrue(isinstance(config, variant_partition.VariantPartition))
    self.assertEqual(config.get_num_partitions(), 8)
    self.assertEqual(config.get_default_partition_index(), 7)
    self.assertFalse(config.is_default_partition_absent())

    # "chr1:0-1,000,000"
    self.assertEqual(config.get_partition('chr1', 0), 0)
    self.assertEqual(config.get_partition('chr1', 999999), 0)
    # "chr1:1,000,000-2,000,000"
    self.assertEqual(config.get_partition('chr1', 1000000), 1)
    self.assertEqual(config.get_partition('chr1', 1999999), 1)
    # "chr1:2,000,000-999,999,999"
    self.assertEqual(config.get_partition('chr1', 2000000), 2)
    self.assertEqual(config.get_partition('chr1', 999999998), 2)
    self.assertEqual(config.get_partition('chr1', 999999999), 7)

    # "chr2" OR "ch2" OR "c2" OR "2"
    self.assertEqual(config.get_partition('chr2', 0), 3)
    self.assertEqual(config.get_partition('chr2', 999999999000), 3)
    self.assertEqual(config.get_partition('ch2', 0), 3)
    self.assertEqual(config.get_partition('ch2', 999999999000), 3)
    self.assertEqual(config.get_partition('c2', 0), 3)
    self.assertEqual(config.get_partition('c2', 999999999000), 3)
    self.assertEqual(config.get_partition('2', 0), 3)
    self.assertEqual(config.get_partition('2', 999999999000), 3)

    # "C4" OR "cr5" OR "chr6:1,000,000-2,000,000"
    self.assertEqual(config.get_partition('c4', 0), 4)
    self.assertEqual(config.get_partition('c4', 999999999000), 4)
    self.assertEqual(config.get_partition('cr5', 0), 4)
    self.assertEqual(config.get_partition('cr5', 999999999000), 4)
    self.assertEqual(config.get_partition('chr6', 1000000), 4)
    self.assertEqual(config.get_partition('chr6', 2000000 - 1), 4)
    self.assertEqual(config.get_partition('chr6', 0), 7)
    self.assertEqual(config.get_partition('chr6', 999999), 7)
    self.assertEqual(config.get_partition('chr6', 2000000), 7)

    # "3:0-500,000"
    self.assertEqual(config.get_partition('3', 0), 5)
    self.assertEqual(config.get_partition('3', 499999), 5)
    # "3:500,000-1,000,000"
    self.assertEqual(config.get_partition('3', 500000), 6)
    self.assertEqual(config.get_partition('3', 999999), 6)
    self.assertEqual(config.get_partition('3', 1000000), 7)

  def test_config_case_insensitive(self):
    config = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config1.yaml")
    self.assertTrue(isinstance(config, variant_partition.VariantPartition))
    self.assertEqual(config.get_num_partitions(), 8)
    self.assertEqual(config.get_default_partition_index(), 7)
    self.assertFalse(config.is_default_partition_absent())

    # "chr1:0-1,000,000"
    self.assertEqual(config.get_partition('chr1', 0), 0)
    self.assertEqual(config.get_partition('Chr1', 0), 0)
    self.assertEqual(config.get_partition('cHr1', 0), 0)
    self.assertEqual(config.get_partition('chR1', 0), 0)
    self.assertEqual(config.get_partition('CHr1', 0), 0)
    self.assertEqual(config.get_partition('ChR1', 0), 0)
    self.assertEqual(config.get_partition('cHR1', 0), 0)
    self.assertEqual(config.get_partition('CHR1', 0), 0)

  def test_config_get_suffix(self):
    config = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config1.yaml")
    self.assertTrue(isinstance(config, variant_partition.VariantPartition))
    self.assertEqual(config.get_num_partitions(), 8)
    self.assertEqual(config.get_default_partition_index(), 7)
    self.assertFalse(config.is_default_partition_absent())

    self.assertEqual(config.get_suffix(0), 'chr01_part1')
    self.assertEqual(config.get_suffix(1), 'chr01_part2')
    self.assertEqual(config.get_suffix(2), 'chr01_part3')
    self.assertEqual(config.get_suffix(3), 'chrom02')
    self.assertEqual(config.get_suffix(4), 'chrom04_05_and_part_06')
    self.assertEqual(config.get_suffix(5), 'chr3_01')
    self.assertEqual(config.get_suffix(6), 'chr3_02')
    self.assertEqual(config.get_suffix(7), 'all_remaining')


  def test_config_non_existent_suffix(self):
    config = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config1.yaml")
    self.assertTrue(isinstance(config, variant_partition.VariantPartition))
    self.assertEqual(config.get_num_partitions(), 8)

    with self.assertRaises(ValueError):
      config.get_suffix(-1)
      self.fail('Non existent partition index should throw an exception')
    with self.assertRaises(ValueError):
      config.get_suffix(8)
      self.fail('Non existent partition index should throw an exception')

  def test_config_default_partition_in_middle(self):
    config = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config2.yaml")
    self.assertTrue(isinstance(config, variant_partition.VariantPartition))
    self.assertEqual(config.get_num_partitions(), 5)
    self.assertEqual(config.get_default_partition_index(), 1)
    self.assertFalse(config.is_default_partition_absent())

    # "chr1:0-1,000,000"
    self.assertEqual(config.get_partition('chr1', 0), 0)
    self.assertEqual(config.get_partition('chr1', 999999), 0)
    # "chr1:1,000,000-2,000,000"
    self.assertEqual(config.get_partition('chr1', 1000000), 2)
    self.assertEqual(config.get_partition('chr1', 1999999), 2)
    # "chr2" OR "ch2" OR "c2" OR "2"
    self.assertEqual(config.get_partition('chr2', 0), 3)
    self.assertEqual(config.get_partition('chr2', 999999999000), 3)
    # "3:500,000-1,000,000"
    self.assertEqual(config.get_partition('3', 500000), 4)
    self.assertEqual(config.get_partition('3', 999999), 4)

    # All the followings are assigned to default partition.
    self.assertEqual(config.get_partition('chr1', 2000000),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('chr1', 999999999),
                     config.get_default_partition_index())

    self.assertEqual(config.get_partition('3', 0),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('3', 499999),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('3', 1000000),
                     config.get_default_partition_index())

    self.assertEqual(config.get_partition('ch2', 0),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('c2', 0),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('2', 0),
                     config.get_default_partition_index())

    self.assertEqual(config.get_partition('c4', 0),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('cr5', 0),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('chr6', 0),
                     config.get_default_partition_index())

  def test_config_default_partition_absent(self):
    config = variant_partition.VariantPartition(
        "gcp_variant_transforms/testing/data/misc/partition_config3.yaml")
    self.assertTrue(isinstance(config, variant_partition.VariantPartition))
    self.assertEqual(config.get_num_partitions(), 5)
    self.assertEqual(config.get_default_partition_index(), 4)
    self.assertTrue(config.is_default_partition_absent())

    # "chr1:0-1,000,000"
    self.assertEqual(config.get_partition('chr1', 0), 0)
    self.assertEqual(config.get_partition('chr1', 999999), 0)
    # "chr1:1,000,000-2,000,000"
    self.assertEqual(config.get_partition('chr1', 1000000), 1)
    self.assertEqual(config.get_partition('chr1', 1999999), 1)
    # "chr2" OR "ch2" OR "c2" OR "2"
    self.assertEqual(config.get_partition('chr2', 0), 2)
    self.assertEqual(config.get_partition('chr2', 999999999000), 2)
    # "3:500,000-1,000,000"
    self.assertEqual(config.get_partition('3', 500000), 3)
    self.assertEqual(config.get_partition('3', 999999), 3)

    # All the followings are assigned to default partition.
    self.assertEqual(config.get_partition('chr1', 2000000),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('chr1', 999999999),
                     config.get_default_partition_index())

    self.assertEqual(config.get_partition('3', 0),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('3', 499999),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('3', 1000000),
                     config.get_default_partition_index())

    self.assertEqual(config.get_partition('ch2', 0),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('c2', 0),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('2', 0),
                     config.get_default_partition_index())

    self.assertEqual(config.get_partition('c4', 0),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('cr5', 0),
                     config.get_default_partition_index())
    self.assertEqual(config.get_partition('chr6', 0),
                     config.get_default_partition_index())

  def test_config_failed_config_validation(self):
    with self.assertRaises(ValueError):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_redundant_default.yaml")
      self.fail('Broken config file should throw an exception')

    with self.assertRaises(ValueError):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_overlapping_regions.yaml")
      self.fail('Broken config file should throw an exception')

    with self.assertRaises(ValueError):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_full_and_partial_chr.yaml")
      self.fail('Broken config file should throw an exception')

    with self.assertRaises(ValueError):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_redundant_full_chr.yaml")
      self.fail('Broken config file should throw an exception')

    with self.assertRaises(ValueError):
      _ = variant_partition.VariantPartition(
          "gcp_variant_transforms/testing/data/misc/"
          "partition_config_redundant_table_names.yaml")
      self.fail('Broken config file should throw an exception')
