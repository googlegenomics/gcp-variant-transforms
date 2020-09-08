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

  def _get_expected_variant_shards(self):
    # reference_name matched to shards defined in
    #   data/sharding_configs/homo_sapiens_default.yaml will be assigned to
    #   the corresponding shard, otherwise they will end up in residual shard.
    expected_shards = {}
    # Shard 0
    expected_shards[0] = [vcfio.Variant(reference_name='chr1', start=0),
                          vcfio.Variant(reference_name='1', start=100000000)]
    # Shard 1
    expected_shards[1] = [vcfio.Variant(reference_name='chr2', start=0),
                          vcfio.Variant(reference_name='2', start=100000000)]
    # Shard 2
    expected_shards[2] = [vcfio.Variant(reference_name='chr3', start=0),
                          vcfio.Variant(reference_name='3', start=100000000)]
    # Shard 5
    expected_shards[5] = [vcfio.Variant(reference_name='chr6', start=0),
                          vcfio.Variant(reference_name='6', start=100000000)]
    # Shard 9
    expected_shards[9] = [vcfio.Variant(reference_name='chr10', start=0),
                          vcfio.Variant(reference_name='10', start=100000000)]
    # Shard 21
    expected_shards[21] = [vcfio.Variant(reference_name='chr22', start=0),
                           vcfio.Variant(reference_name='22', start=100000000)]
    # Shard 22
    expected_shards[22] = [vcfio.Variant(reference_name='chrX', start=0),
                           vcfio.Variant(reference_name='chrx', start=0),
                           vcfio.Variant(reference_name='X', start=0),
                           vcfio.Variant(reference_name='x', start=0)]
    # Shard 23
    expected_shards[23] = [vcfio.Variant(reference_name='chrY', start=0),
                           vcfio.Variant(reference_name='chry', start=0),
                           vcfio.Variant(reference_name='Y', start=0),
                           vcfio.Variant(reference_name='y', start=0)]
    # Shard 24, aka residual
    expected_shards[24] = [vcfio.Variant(reference_name='ch1', start=0),
                           vcfio.Variant(reference_name='chr001', start=0),
                           vcfio.Variant(reference_name='01', start=0),
                           vcfio.Variant(reference_name='CHR1', start=0),
                           vcfio.Variant(reference_name='cHr1', start=0),
                           vcfio.Variant(reference_name='chR1', start=0),
                           vcfio.Variant(reference_name='CHRX', start=0),
                           vcfio.Variant(reference_name='CHRY', start=0),
                           vcfio.Variant(reference_name='contig1', start=0),
                           vcfio.Variant(reference_name='contig1000', start=0),
                           vcfio.Variant(reference_name='chr23', start=0),
                           vcfio.Variant(reference_name='chr24', start=0),
                           vcfio.Variant(reference_name='chr99', start=0),
                           vcfio.Variant(reference_name='chrM', start=0)]
    return expected_shards

  def test_shard_variants(self):
    expected_shards = self._get_expected_variant_shards()
    variants = [variant
                for variant_list in expected_shards.values()
                for variant in variant_list]

    sharding = variant_sharding.VariantSharding(
        'gcp_variant_transforms/data/sharding_configs/'
        'homo_sapiens_default.yaml')
    pipeline = TestPipeline()
    shards = (
        pipeline
        | Create(variants, reshuffle=False)
        | 'ShardVariants' >> beam.Partition(
            shard_variants.ShardVariants(sharding),
            sharding.get_num_shards()))
    for i in range(sharding.get_num_shards()):
      assert_that(shards[i], equal_to(expected_shards.get(i, [])),
                  label=str(i))
    pipeline.run()
