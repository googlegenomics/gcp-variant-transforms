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
  _DEFAULT_NUM_PARTITIONS as default_num_partitions
from gcp_variant_transforms.libs.variant_partition import \
  _RESERVED_PARTITIONS as reserved_partitions


class VariantPartitionTest(unittest.TestCase):

  def test_auto_partitioning(self):
    partitioner = variant_partition.VariantPartition()
    self.assertTrue(isinstance(partitioner, variant_partition.VariantPartition))
    self.assertEqual(partitioner.get_num_partitions(), default_num_partitions)

    # Checking standard reference_name formatted as: 'chr[0-9][0-9]'
    for i in xrange(reserved_partitions):
      self.assertEqual(partitioner.get_partition('chr'+str(i+1)), i)
    # Checking standard reference_name formatted as: '[0-9][0-9]'
    for i in xrange(reserved_partitions):
      self.assertEqual(partitioner.get_partition(str(i+1)), i)

    # Every other reference_name will be assigned to partitions >= 22
    self.assertGreaterEqual(partitioner.get_partition('chrY'),
                            reserved_partitions)
    self.assertGreaterEqual(partitioner.get_partition('chrX'),
                            reserved_partitions)
    self.assertGreaterEqual(partitioner.get_partition('chrM'),
                            reserved_partitions)
    self.assertGreaterEqual(partitioner.get_partition('chr23'),
                            reserved_partitions)
    self.assertGreaterEqual(partitioner.get_partition('chr30'),
                            reserved_partitions)
    self.assertGreaterEqual(partitioner.get_partition('Unknown'),
                            reserved_partitions)

  def test_filed_partitioning(self):
    partitioner = variant_partition.VariantPartition()
    self.assertTrue(isinstance(partitioner, variant_partition.VariantPartition))
    self.assertEqual(partitioner.get_num_partitions(), default_num_partitions)

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('chr1', -1)
      self.fail('Negative position must fail partitioner.')

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('', 1)
      self.fail('Missing reference_name must fail partitioner.')

    with self.assertRaisesRegexp(ValueError, 'Cannot partition given input*'):
      partitioner.get_partition('  ', 1)
      self.fail('Missing reference_name must fail partitioner.')
