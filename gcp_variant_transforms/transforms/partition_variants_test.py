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

"""Tests for partition_variants module."""

from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.transforms import partition_variants
from gcp_variant_transforms.libs import variant_partition


class PartitionVariantsTest(unittest.TestCase):
  """Test cases for the ``PartitionVariants`` transform."""

  def _get_standard_variant_partitions(self):
    # Valid variants reference_name.strip().lower() will successfully matched to
    #   re.compile(r'^(chr)?([0-9][0-9]?)$')
    expected_partitions = {}
    # Partition 0
    expected_partitions[0] = [vcfio.Variant(reference_name='chr1', start=0),
                              vcfio.Variant(reference_name='CHR1', start=0)]
    # Partition 1
    expected_partitions[1] = [vcfio.Variant(reference_name='cHr2', start=0),
                              vcfio.Variant(reference_name='chR2', start=0)]
    # Partition 2
    expected_partitions[2] = [vcfio.Variant(reference_name='chr3', start=0),
                              vcfio.Variant(reference_name='chr03', start=0)]
    # Partition 5
    expected_partitions[5] = [vcfio.Variant(reference_name='6', start=0),
                              vcfio.Variant(reference_name='06', start=0),
                              vcfio.Variant(reference_name='chr06', start=0)]
    # Partition 9
    expected_partitions[9] = [vcfio.Variant(reference_name='chr10', start=0),
                              vcfio.Variant(reference_name='  chr10 ', start=0)]
    # Partition 21
    expected_partitions[21] = [vcfio.Variant(reference_name='chr22', start=0),
                               vcfio.Variant(reference_name='chr22  ', start=0)]
    return expected_partitions

  def _get_nonstandard_variant_partitions(self):
    # All these variants will not match to standard reference_name Reg Exp, thus
    # they will all end up in partitions >= 22.
    expected_partitions = {}
    # Partition 22
    expected_partitions[22] = [vcfio.Variant(reference_name='NOT6', start=0),
                               vcfio.Variant(reference_name='NOTch3', start=0)]
    # Partition 24
    expected_partitions[24] = [vcfio.Variant(reference_name='NOTchr1', start=0),
                               vcfio.Variant(reference_name='chr99', start=0)]
    # Partition 25
    expected_partitions[25] = [vcfio.Variant(reference_name='NOT07', start=0),
                               vcfio.Variant(reference_name='chr008', start=0),
                               vcfio.Variant(reference_name='chr23', start=0),
                               vcfio.Variant(reference_name='chrX', start=0),
                               vcfio.Variant(reference_name='chrM', start=0)]
    # Partition 26
    expected_partitions[26] = [vcfio.Variant(reference_name='NOTc4', start=0),
                               vcfio.Variant(reference_name='chr0', start=0),
                               vcfio.Variant(reference_name='chrY', start=0)]
    return expected_partitions

  def test_partition_variants(self):
    expected_partitions = self._get_standard_variant_partitions()
    expected_partitions.update(self._get_nonstandard_variant_partitions())
    variants = [variant
                for variant_list in expected_partitions.values()
                for variant in variant_list]

    partitioner = variant_partition.VariantPartition()
    pipeline = TestPipeline()
    partitions = (
        pipeline
        | Create(variants)
        | 'PartitionVariants' >> beam.Partition(
            partition_variants.PartitionVariants(partitioner),
            partitioner.get_num_partitions()))
    for i in xrange(partitioner.get_num_partitions()):
      assert_that(partitions[i], equal_to(expected_partitions.get(i, [])),
                  label=str(i))
    pipeline.run()
