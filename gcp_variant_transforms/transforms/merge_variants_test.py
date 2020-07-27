# Copyright 2017 Google Inc.  All Rights Reserved.
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

"""Tests for merge_variants module."""



import unittest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs.variant_merge import move_to_calls_strategy
from gcp_variant_transforms.testing import asserts
from gcp_variant_transforms.testing.testdata_util import hash_name
from gcp_variant_transforms.transforms import merge_variants


class MergeVariantsTest(unittest.TestCase):
  """Test cases for the ``MergeVariants`` transform."""

  def _get_sample_merged_variants(self):
    variant_1 = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1'], quality=2,
        filters=['PASS'],
        info={'A1': 'some data', 'A2': ['data1', 'data2']},
        calls=[
            vcfio.VariantCall(
                sample_id=hash_name('Sample1'), genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'HQ': [10, 20]}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample2'), genotype=[1, 0],
                info={'GQ': 10, 'FLAG1': True}),
        ]
    )
    variant_2 = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1'], quality=20,
        filters=['q10'],
        info={'A1': 'some data2', 'A3': ['data3', 'data4']},
        calls=[
            vcfio.VariantCall(sample_id=hash_name('Sample3'), genotype=[1, 1]),
            vcfio.VariantCall(
                sample_id=hash_name('Sample4'), genotype=[1, 0],
                info={'GQ': 20}),
        ]
    )
    merged_variant = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1'],
        filters=['PASS', 'q10'], quality=20,
        info={'A2': ['data1', 'data2'], 'A3': ['data3', 'data4']},
        calls=[
            vcfio.VariantCall(
                sample_id=hash_name('Sample1'), genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'HQ': [10, 20], 'A1': 'some data'}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample2'), genotype=[1, 0],
                info={'GQ': 10, 'FLAG1': True, 'A1': 'some data'}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample3'), genotype=[1, 1],
                info={'A1': 'some data2'}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample4'), genotype=[1, 0],
                info={'GQ': 20, 'A1': 'some data2'}),
        ]
    )
    return [variant_1, variant_2], merged_variant

  def _get_sample_unmerged_variants(self):
    # Start/end are different from merged variants.
    variant_1 = vcfio.Variant(
        reference_name='19', start=123, end=125, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs2'],
        calls=[vcfio.VariantCall(sample_id=hash_name('Unmerged1'),
                                 genotype=[0, 1])])
    # Ordering of alternate_bases is different from merged variants.
    variant_2 = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['TT', 'A'], names=['rs3'],
        calls=[vcfio.VariantCall(sample_id=hash_name('Unmerged2'),
                                 genotype=[0, 1])])
    return [variant_1, variant_2]

  def test_merge_variants(self):
    variant_merger = move_to_calls_strategy.MoveToCallsStrategy(
        '^A1$', False, False)
    variant_list, merged_variant = self._get_sample_merged_variants()
    unmerged_variant_list = self._get_sample_unmerged_variants()
    pipeline = TestPipeline()
    merged_variants = (
        pipeline
        | Create(variant_list + unmerged_variant_list, reshuffle=False)
        | 'MergeVariants' >> merge_variants.MergeVariants(variant_merger))
    assert_that(merged_variants,
                asserts.variants_equal_to_ignore_order([merged_variant] +
                                                       unmerged_variant_list))
    pipeline.run()
