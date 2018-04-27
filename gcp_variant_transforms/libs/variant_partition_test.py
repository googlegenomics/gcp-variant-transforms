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


class VariantPartitionTest(unittest.TestCase):

  def test_auto_partitioning(self):
    partitioner = variant_partition.VariantPartition()
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

  def test_auto_partitioning_invalid_partitions(self):
    partitioner = variant_partition.VariantPartition()
    self.assertEqual(partitioner.get_num_partitions(),
                     variant_partition._DEFAULT_NUM_PARTITIONS)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('chr1', -1)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('', 1)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('  ', 1)
