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

"""Tests for shard_variants module."""

from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.transforms import shard_variants
from gcp_variant_transforms.libs import variant_sharding


class ShardVariantsTest(unittest.TestCase):
  """Test cases for the ``ShardVariants`` transform."""

  def _get_standard_variant_shards(self):
    # Valid variants reference_name.strip().lower() will successfully matched to
    #   re.compile(r'^(chr)?([0-9][0-9]?)$')
    expected_shards = {}
    # Partition 0
    expected_shards[0] = [vcfio.Variant(reference_name='chr1', start=0),
                          vcfio.Variant(reference_name='CHR1', start=0)]
    # Partition 1
    expected_shards[1] = [vcfio.Variant(reference_name='cHr2', start=0),
                          vcfio.Variant(reference_name='chR2', start=0)]
    # Partition 2
    expected_shards[2] = [vcfio.Variant(reference_name='chr3', start=0),
                          vcfio.Variant(reference_name='chr03', start=0)]
    # Partition 5
    expected_shards[5] = [vcfio.Variant(reference_name='6', start=0),
                          vcfio.Variant(reference_name='06', start=0),
                          vcfio.Variant(reference_name='chr06', start=0)]
    # Partition 9
    expected_shards[9] = [vcfio.Variant(reference_name='chr10', start=0),
                          vcfio.Variant(reference_name='  chr10 ', start=0)]
    # Partition 21
    expected_shards[21] = [vcfio.Variant(reference_name='chr22', start=0),
                           vcfio.Variant(reference_name='chr22  ', start=0)]
    return expected_shards

  def _get_nonstandard_variant_shards(self):
    # All these variants will not match to standard reference_name Reg Exp, thus
    # they will all end up in shards >= 22.
    expected_shards = {}
    # Partition 22
    expected_shards[22] = [vcfio.Variant(reference_name='NOT6', start=0),
                           vcfio.Variant(reference_name='NOTch3', start=0)]
    # Partition 24
    expected_shards[24] = [vcfio.Variant(reference_name='NOTchr1', start=0),
                           vcfio.Variant(reference_name='chr99', start=0)]
    # Partition 25
    expected_shards[25] = [vcfio.Variant(reference_name='NOT07', start=0),
                           vcfio.Variant(reference_name='chr008', start=0),
                           vcfio.Variant(reference_name='chr23', start=0),
                           vcfio.Variant(reference_name='chrX', start=0),
                           vcfio.Variant(reference_name='chrM', start=0)]
    # Partition 26
    expected_shards[26] = [vcfio.Variant(reference_name='NOTc4', start=0),
                           vcfio.Variant(reference_name='chr0', start=0),
                           vcfio.Variant(reference_name='chrY', start=0)]
    return expected_shards

  def test_shard_variants(self):
    expected_shards = self._get_standard_variant_shards()
    expected_shards.update(self._get_nonstandard_variant_shards())
    variants = [variant
                for variant_list in expected_shards.values()
                for variant in variant_list]

    sharder = variant_sharding.VariantSharding()
    pipeline = TestPipeline()
    shards = (
        pipeline
        | Create(variants)
        | 'ShardVariants' >> beam.Partition(
            shard_variants.ShardVariants(sharder),
            sharder.get_num_shards()))
    for i in xrange(sharder.get_num_shards()):
      assert_that(shards[i], equal_to(expected_shards.get(i, [])),
                  label=str(i))
    pipeline.run()
