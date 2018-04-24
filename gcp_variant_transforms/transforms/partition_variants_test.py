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

from apache_beam import Partition
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.transforms import partition_variants
from gcp_variant_transforms.libs import variant_partition


class PartitionVariantsTest(unittest.TestCase):
  """Test cases for the ``PartitionVariants`` transform."""

  def _get_standard_variants(self):
    # Valid varints reference_name.strip().lower() will successfully matched to
    #   re.compile(r'^(chr)?([0-9][0-9]?)$')
    variants = []
    # Partition 0
    variants.append(vcfio.Variant(reference_name='chr1', start=0))
    variants.append(vcfio.Variant(reference_name='CHR1', start=0))
    # Partition 1
    variants.append(vcfio.Variant(reference_name='cHr2', start=0))
    variants.append(vcfio.Variant(reference_name='chR2', start=0))
    # Partition 2
    variants.append(vcfio.Variant(reference_name='chr3', start=0))
    variants.append(vcfio.Variant(reference_name='chr03', start=0))
    # Partition 5
    variants.append(vcfio.Variant(reference_name='6', start=0))
    variants.append(vcfio.Variant(reference_name='06', start=0))
    variants.append(vcfio.Variant(reference_name='chr06', start=0))
    # Partition 9
    variants.append(vcfio.Variant(reference_name='chr10', start=0))
    variants.append(vcfio.Variant(reference_name='  chr10   ', start=0))
    # Partition 21
    variants.append(vcfio.Variant(reference_name='chr22', start=0))
    variants.append(vcfio.Variant(reference_name='chr22    ', start=0))
    return variants, variants[0:2], variants[2:4], variants[4:6],\
           variants[6:9], variants[9:11], variants[11:13]

  def _get_nonstandard_variants(self):
    variants = []
    variants.append(vcfio.Variant(reference_name='NOTchr1', start=0))
    variants.append(vcfio.Variant(reference_name='NOTch3', start=0))
    variants.append(vcfio.Variant(reference_name='NOTc4', start=0))
    variants.append(vcfio.Variant(reference_name='NOT6', start=0))
    variants.append(vcfio.Variant(reference_name='NOT07', start=0))
    variants.append(vcfio.Variant(reference_name='chr008', start=0))
    variants.append(vcfio.Variant(reference_name='chr010', start=0))
    variants.append(vcfio.Variant(reference_name='chr0', start=0))
    variants.append(vcfio.Variant(reference_name='chr23', start=0))
    variants.append(vcfio.Variant(reference_name='chr99', start=0))
    variants.append(vcfio.Variant(reference_name='chrX', start=0))
    variants.append(vcfio.Variant(reference_name='chrY', start=0))
    variants.append(vcfio.Variant(reference_name='chrM', start=0))
    # All these variants will not match to standard reference_name Reg Exp, thus
    # they will all end up in the default partition.
    return variants, variants

  def test_partition_variants(self):
    standard_variants, par00, par01, par02, par05, par09, par21 = \
      self._get_standard_variants()
    nonstandard_variants, par22 = self._get_nonstandard_variants()

    partitioner = variant_partition.VariantPartition()
    pipeline = TestPipeline()
    partitions = (
        pipeline
        | Create(standard_variants + nonstandard_variants)
        | 'PartitionVariants' >> Partition(
            partition_variants.PartitionVariants(partitioner), 22 + 1))

    assert_that(partitions[0], equal_to(par00))
    assert_that(partitions[1], equal_to(par01), label='p1')
    assert_that(partitions[2], equal_to(par02), label='p2')
    assert_that(partitions[5], equal_to(par05), label='p5')
    assert_that(partitions[9], equal_to(par09), label='p9')
    assert_that(partitions[21], equal_to(par21), label='p21')
    assert_that(partitions[22], equal_to(par22), label='p22')

    assert_that(partitions[3], equal_to([]), label='p3')
    assert_that(partitions[4], equal_to([]), label='p4')
    assert_that(partitions[6], equal_to([]), label='p6')
    assert_that(partitions[7], equal_to([]), label='p7')
    assert_that(partitions[8], equal_to([]), label='p8')
    assert_that(partitions[10], equal_to([]), label='p10')
    assert_that(partitions[11], equal_to([]), label='p11')
    assert_that(partitions[12], equal_to([]), label='p12')
    assert_that(partitions[13], equal_to([]), label='p13')
    assert_that(partitions[14], equal_to([]), label='p14')
    assert_that(partitions[15], equal_to([]), label='p15')
    assert_that(partitions[16], equal_to([]), label='p16')
    assert_that(partitions[17], equal_to([]), label='p17')
    assert_that(partitions[18], equal_to([]), label='p18')
    assert_that(partitions[19], equal_to([]), label='p19')
    assert_that(partitions[20], equal_to([]), label='p20')
    pipeline.run()
