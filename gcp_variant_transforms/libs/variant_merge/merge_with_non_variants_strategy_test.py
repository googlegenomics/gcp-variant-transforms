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

"""Tests for merge_with_nonvariants_strategy."""

from __future__ import absolute_import

import copy
import unittest

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs.bigquery_util import ColumnKeyConstants
from gcp_variant_transforms.libs.variant_merge import merge_with_non_variants_strategy


class MergeWithNonVariantsStrategyTest(unittest.TestCase):
  """Tests for MergeWithNonVariantsStrategy."""

  def _get_sample_variants(self):
    variant_1 = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'A1': 'some data', 'A2': ['data1', 'data2']},
        calls=[
            vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                              info={'GQ': 20, 'HQ': [10, 20]}),
            vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                              info={'GQ': 10, 'FLAG1': True})])
    variant_2 = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs3'], quality=20,
        filters=['q10'],
        info={'A1': 'some data2', 'A3': ['data3', 'data4']},
        calls=[
            vcfio.VariantCall(name='Sample3', genotype=[1, 1]),
            vcfio.VariantCall(name='Sample4', genotype=[1, 0],
                              info={'GQ': 20})])
    return [variant_1, variant_2]

  def _assert_common_expected_merged_fields(self, merged_variant):
    """Asserts expected common fields in the merged variant."""
    self.assertEqual('19', merged_variant.reference_name)
    self.assertEqual(11, merged_variant.start)
    self.assertEqual(12, merged_variant.end)
    self.assertEqual('C', merged_variant.reference_bases)
    self.assertEqual(['A', 'TT'], merged_variant.alternate_bases)
    self.assertEqual(['rs1', 'rs2', 'rs3'], merged_variant.names)
    self.assertEqual(20, merged_variant.quality)
    self.assertEqual(['PASS', 'q10'], merged_variant.filters)

  def test_get_merged_variants_no_custom_options(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        info_keys_to_move_to_calls_regex=None,
        copy_quality_to_calls=False,
        copy_filter_to_calls=False)
    variants = self._get_sample_variants()

    actual = list(strategy.get_merged_variants([variants[0]]))
    # Test single variant merge.
    self.assertEqual([variants[0]], actual)

    # Test multiple variant merge.
    merged_variant = list(strategy.get_merged_variants(variants))[0]
    self._assert_common_expected_merged_fields(merged_variant)
    self.assertEqual(
        [vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                           info={'GQ': 20, 'HQ': [10, 20]}),
         vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                           info={'GQ': 10, 'FLAG1': True}),
         vcfio.VariantCall(name='Sample3', genotype=[1, 1]),
         vcfio.VariantCall(name='Sample4', genotype=[1, 0], info={'GQ': 20})],
        merged_variant.calls)
    self.assertItemsEqual(['A1', 'A2', 'A3'], merged_variant.info.keys())
    self.assertTrue(
        merged_variant.info['A1'] in ('some data', 'some data2'))
    self.assertEqual(['data1', 'data2'], merged_variant.info['A2'])
    self.assertEqual(['data3', 'data4'], merged_variant.info['A3'])

  def test_get_merged_variants_move_quality_and_filter_to_calls(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        info_keys_to_move_to_calls_regex='',
        copy_quality_to_calls=True,
        copy_filter_to_calls=True)
    variants = self._get_sample_variants()

    # Test single variant merge.
    single_merged_variant = list(strategy.get_merged_variants([variants[0]]))[0]
    self.assertEqual(
        [vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                           info={'GQ': 20, 'HQ': [10, 20],
                                 ColumnKeyConstants.QUALITY: 2,
                                 ColumnKeyConstants.FILTER: ['PASS']}),
         vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                           info={'GQ': 10, 'FLAG1': True,
                                 ColumnKeyConstants.QUALITY: 2,
                                 ColumnKeyConstants.FILTER: ['PASS']})],
        single_merged_variant.calls)

    # Test multiple variant merge.
    merged_variant = list(strategy.get_merged_variants(variants))[0]
    self._assert_common_expected_merged_fields(merged_variant)
    self.assertEqual(
        [vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                           info={'GQ': 20, 'HQ': [10, 20],
                                 ColumnKeyConstants.QUALITY: 2,
                                 ColumnKeyConstants.FILTER: ['PASS']}),
         vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                           info={'GQ': 10, 'FLAG1': True,
                                 ColumnKeyConstants.QUALITY: 2,
                                 ColumnKeyConstants.FILTER: ['PASS']}),
         vcfio.VariantCall(name='Sample3', genotype=[1, 1],
                           info={ColumnKeyConstants.QUALITY: 20,
                                 ColumnKeyConstants.FILTER: ['q10']}),
         vcfio.VariantCall(name='Sample4', genotype=[1, 0],
                           info={'GQ': 20,
                                 ColumnKeyConstants.QUALITY: 20,
                                 ColumnKeyConstants.FILTER: ['q10']})],
        merged_variant.calls)
    self.assertItemsEqual(['A1', 'A2', 'A3'], merged_variant.info.keys())
    self.assertTrue(
        merged_variant.info['A1'] in ('some data', 'some data2'))
    self.assertEqual(['data1', 'data2'], merged_variant.info['A2'])
    self.assertEqual(['data3', 'data4'], merged_variant.info['A3'])

  def test_get_merged_variants_move_info_to_calls(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        info_keys_to_move_to_calls_regex='^A1$',
        copy_quality_to_calls=False,
        copy_filter_to_calls=False)
    variants = self._get_sample_variants()

    # Test single variant merge.
    single_merged_variant = list(strategy.get_merged_variants([variants[0]]))[0]
    self.assertEqual(
        [vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                           info={'GQ': 20, 'HQ': [10, 20], 'A1': 'some data'}),
         vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                           info={'GQ': 10, 'FLAG1': True, 'A1': 'some data'})],
        single_merged_variant.calls)

    # Test multiple variant merge.
    merged_variant = list(strategy.get_merged_variants(variants))[0]
    self._assert_common_expected_merged_fields(merged_variant)
    self.assertEqual(
        [vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                           info={'GQ': 20, 'HQ': [10, 20], 'A1': 'some data'}),
         vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                           info={'GQ': 10, 'FLAG1': True, 'A1': 'some data'}),
         vcfio.VariantCall(name='Sample3', genotype=[1, 1],
                           info={'A1': 'some data2'}),
         vcfio.VariantCall(name='Sample4', genotype=[1, 0],
                           info={'GQ': 20, 'A1': 'some data2'})],
        merged_variant.calls)
    self.assertItemsEqual(['A2', 'A3'], merged_variant.info.keys())
    self.assertEqual(['data1', 'data2'], merged_variant.info['A2'])
    self.assertEqual(['data3', 'data4'], merged_variant.info['A3'])

  def test_get_merged_variants_move_everything_to_calls(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        info_keys_to_move_to_calls_regex='.*',
        copy_quality_to_calls=True,
        copy_filter_to_calls=True)
    variants = self._get_sample_variants()

    # Test single variant merge.
    single_merged_variant = list(strategy.get_merged_variants([variants[0]]))[0]
    self.assertEqual(
        [vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                           info={'GQ': 20, 'HQ': [10, 20],
                                 'A1': 'some data', 'A2': ['data1', 'data2'],
                                 ColumnKeyConstants.QUALITY: 2,
                                 ColumnKeyConstants.FILTER: ['PASS']}),
         vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                           info={'GQ': 10, 'FLAG1': True,
                                 'A1': 'some data', 'A2': ['data1', 'data2'],
                                 ColumnKeyConstants.QUALITY: 2,
                                 ColumnKeyConstants.FILTER: ['PASS']})],
        single_merged_variant.calls)

    merged_variant = list(strategy.get_merged_variants(variants))[0]
    self._assert_common_expected_merged_fields(merged_variant)
    self.assertEqual(
        [vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                           info={'GQ': 20, 'HQ': [10, 20],
                                 'A1': 'some data', 'A2': ['data1', 'data2'],
                                 ColumnKeyConstants.QUALITY: 2,
                                 ColumnKeyConstants.FILTER: ['PASS']}),
         vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                           info={'GQ': 10, 'FLAG1': True,
                                 'A1': 'some data', 'A2': ['data1', 'data2'],
                                 ColumnKeyConstants.QUALITY: 2,
                                 ColumnKeyConstants.FILTER: ['PASS']}),
         vcfio.VariantCall(name='Sample3', genotype=[1, 1],
                           info={'A1': 'some data2', 'A3': ['data3', 'data4'],
                                 ColumnKeyConstants.QUALITY: 20,
                                 ColumnKeyConstants.FILTER: ['q10']}),
         vcfio.VariantCall(name='Sample4', genotype=[1, 0],
                           info={'GQ': 20,
                                 'A1': 'some data2', 'A3': ['data3', 'data4'],
                                 ColumnKeyConstants.QUALITY: 20,
                                 ColumnKeyConstants.FILTER: ['q10']})],
        merged_variant.calls)
    self.assertEqual([], merged_variant.info.keys())

  def test_get_snp_merge_keys(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None, 2)

    variant_1 = vcfio.Variant(reference_name='1', start=3, end=4)
    variant_2 = vcfio.Variant(reference_name='2', start=4, end=5)

    self.assertEqual(next(strategy.get_merge_keys(variant_1)), '1:2')
    self.assertEqual(next(strategy.get_merge_keys(variant_2)), '2:4')

  def test_get_non_variant_merge_keys(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None, 2)
    variant = vcfio.Variant(reference_name='2', start=6, end=12)
    keys = strategy.get_merge_keys(variant)
    self.assertEqual(next(keys), '2:6')
    self.assertEqual(next(keys), '2:8')
    self.assertEqual(next(keys), '2:10')

  def test_merge_many_different_alternates(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)

    variant_1 = vcfio.Variant(reference_name='1',
                              start=1,
                              end=2,
                              reference_bases='A',
                              alternate_bases=['C'])
    variant_2 = vcfio.Variant(reference_name='1',
                              start=1,
                              end=2,
                              reference_bases='A',
                              alternate_bases=['G'])
    variant_3 = vcfio.Variant(reference_name='1',
                              start=1,
                              end=2,
                              reference_bases='A',
                              alternate_bases=['T'])
    variant_1.calls.append(vcfio.VariantCall(name='Sample1', genotype=[1, 0]))
    variant_2.calls.append(vcfio.VariantCall(name='Sample2', genotype=[1, 0]))
    variant_3.calls.append(vcfio.VariantCall(name='Sample3', genotype=[1, 0]))
    variants = [variant_1, variant_2, variant_3]
    merged_variants = list(strategy.get_merged_variants(variants))
    self.assertEqual(sorted(merged_variants), sorted(variants))

  def test_merge_one_overlap(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)

    variant_1 = vcfio.Variant(reference_name='1',
                              start=1,
                              end=2,
                              reference_bases='A',
                              alternate_bases=['C'])
    variant_2 = vcfio.Variant(reference_name='1',
                              start=1,
                              end=2,
                              reference_bases='A',
                              alternate_bases=['G'])
    variant_3 = vcfio.Variant(reference_name='1',
                              start=1,
                              end=2,
                              reference_bases='A',
                              alternate_bases=['T'])
    variant_4 = vcfio.Variant(reference_name='1',
                              start=1,
                              end=2,
                              reference_bases='A',
                              alternate_bases=['C'])
    variant_1.calls.append(vcfio.VariantCall(name='Sample1', genotype=[1, 0]))
    variant_2.calls.append(vcfio.VariantCall(name='Sample2', genotype=[1, 0]))
    variant_3.calls.append(vcfio.VariantCall(name='Sample3', genotype=[1, 0]))
    variant_4.calls.append(vcfio.VariantCall(name='Sample4', genotype=[1, 0]))
    variants = [variant_1, variant_2, variant_3, variant_4]
    merged = vcfio.Variant(reference_name='1',
                           start=1,
                           end=2,
                           reference_bases='A',
                           alternate_bases=['C'])
    merged.calls.append(vcfio.VariantCall(name='Sample1', genotype=[1, 0]))
    merged.calls.append(vcfio.VariantCall(name='Sample4', genotype=[1, 0]))
    merged_variants = list(strategy.get_merged_variants(variants))
    self.assertEqual(
        sorted(merged_variants), sorted([merged, variant_2, variant_3]))

  def test_merge_2_non_variants(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)

    non_variant_1 = vcfio.Variant(
        reference_name='1',
        start=0,
        end=10,
        alternate_bases=['<NON_REF>'],
        names=['nonv1', 'nonv2'],
        filters=['f1', 'f2'],
        quality=1)
    non_variant_2 = vcfio.Variant(
        reference_name='1',
        start=5,
        end=15,
        alternate_bases=['<NON_REF>'],
        names=['nonv2', 'nonv3'],
        filters=['f2', 'f3'],
        quality=2)
    call_1 = vcfio.VariantCall(name='1', genotype=[0, 0])
    call_2 = vcfio.VariantCall(name='2', genotype=[0, 0])
    non_variant_1.calls.append(call_1)
    non_variant_2.calls.append(call_2)
    expected_1 = vcfio.Variant(
        reference_name='1',
        start=0,
        end=5,
        alternate_bases=['<NON_REF>'],
        names=['nonv1', 'nonv2'],
        filters=['f1', 'f2'],
        quality=1)
    expected_2 = vcfio.Variant(
        reference_name='1',
        start=10,
        end=15,
        alternate_bases=['<NON_REF>'],
        names=['nonv2', 'nonv3'],
        filters=['f2', 'f3'],
        quality=2)
    expected_3 = vcfio.Variant(
        reference_name='1',
        start=5,
        end=10,
        alternate_bases=['<NON_REF>'],
        names=['nonv1', 'nonv2', 'nonv3'],
        filters=['f1', 'f2', 'f3'],
        quality=1)
    expected_1.calls.append(call_1)
    expected_2.calls.append(call_2)
    expected_3.calls.append(call_1)
    expected_3.calls.append(call_2)
    actual = list(strategy.get_merged_variants([non_variant_1, non_variant_2]))
    expected = [expected_1, expected_2, expected_3]

    self.assertEqual(sorted(actual), sorted(expected))

  def test_merge_non_variants_same_start(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)

    non_variant_1 = vcfio.Variant(
        reference_name='1',
        start=0,
        end=10,
        alternate_bases=['<NON_REF>'],
        names=['nonv1', 'nonv2'],
        filters=['f1', 'f2'],
        quality=1)
    non_variant_2 = vcfio.Variant(
        reference_name='1',
        start=0,
        end=15,
        alternate_bases=['<NON_REF>'],
        names=['nonv2', 'nonv3'],
        filters=['f2', 'f3'],
        quality=2)
    call_1 = vcfio.VariantCall(name='1', genotype=[0, 0])
    call_2 = vcfio.VariantCall(name='2', genotype=[0, 0])
    non_variant_1.calls.append(call_1)
    non_variant_2.calls.append(call_2)
    expected_1 = vcfio.Variant(
        reference_name='1',
        start=10,
        end=15,
        alternate_bases=['<NON_REF>'],
        names=['nonv2', 'nonv3'],
        filters=['f2', 'f3'],
        quality=2)
    expected_2 = vcfio.Variant(
        reference_name='1',
        start=0,
        end=10,
        alternate_bases=['<NON_REF>'],
        names=['nonv1', 'nonv2', 'nonv3'],
        filters=['f1', 'f2', 'f3'],
        quality=1)
    expected_1.calls.append(call_2)
    expected_2.calls.append(call_1)
    expected_2.calls.append(call_2)
    actual = list(strategy.get_merged_variants([non_variant_1, non_variant_2]))
    expected = [expected_1, expected_2]

    self.assertEqual(sorted(actual), sorted(expected))

  def test_merge_2_non_variants_same_end(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)

    non_variant_1 = vcfio.Variant(
        reference_name='1',
        start=0,
        end=10,
        reference_bases='A',
        alternate_bases=['<NON_REF>'],
        names=['nonv1', 'nonv2'],
        filters=['f1', 'f2'],
        quality=1)
    non_variant_2 = vcfio.Variant(
        reference_name='1',
        start=5,
        end=10,
        reference_bases='G',
        alternate_bases=['<NON_REF>'],
        names=['nonv2', 'nonv3'],
        filters=['f2', 'f3'],
        quality=2)
    call_1 = vcfio.VariantCall(name='1', genotype=[0, 0])
    call_2 = vcfio.VariantCall(name='2', genotype=[0, 0])
    non_variant_1.calls.append(call_1)
    non_variant_2.calls.append(call_2)
    expected_1 = vcfio.Variant(
        reference_name='1',
        start=0,
        end=5,
        alternate_bases=['<NON_REF>'],
        names=['nonv1', 'nonv2'],
        filters=['f1', 'f2'],
        quality=1)
    expected_2 = vcfio.Variant(
        reference_name='1',
        start=5,
        end=10,
        alternate_bases=['<NON_REF>'],
        names=['nonv1', 'nonv2', 'nonv3'],
        filters=['f1', 'f2', 'f3'],
        quality=1)
    expected_1.calls.append(call_1)
    expected_2.calls.append(call_1)
    expected_2.calls.append(call_2)
    actual = list(strategy.get_merged_variants([non_variant_1, non_variant_2]))
    expected = [expected_1, expected_2]

    self.assertEqual(sorted(actual), sorted(expected))

  def test_merge_snp_with_non_variant(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)

    variant = vcfio.Variant(
        reference_name='1',
        start=5,
        end=6,
        reference_bases='A',
        alternate_bases=['C'],
        names=['v'],
        filters=['vf'],
        quality=1)
    non_variant = vcfio.Variant(
        reference_name='1',
        start=0,
        end=10,
        reference_bases='G',
        alternate_bases=['<NON_REF>'],
        names=['nv'],
        filters=['nvf'],
        quality=2)

    call_1 = vcfio.VariantCall(name='1', genotype=[1, 0])
    call_2 = vcfio.VariantCall(name='2', genotype=[0, 0])
    variant.calls.append(call_1)
    non_variant.calls.append(call_2)
    expected_1 = vcfio.Variant(
        reference_name='1',
        start=0,
        end=5,
        alternate_bases=['<NON_REF>'],
        names=['nv'],
        filters=['nvf'],
        quality=2)
    expected_2 = vcfio.Variant(
        reference_name='1',
        start=6,
        end=10,
        alternate_bases=['<NON_REF>'],
        names=['nv'],
        filters=['nvf'],
        quality=2)
    expected_3 = vcfio.Variant(
        reference_name='1',
        start=5,
        end=6,
        reference_bases='A',
        alternate_bases=['C'],
        names=['v'],
        filters=['vf'],
        quality=1)
    expected_1.calls.append(call_2)
    expected_2.calls.append(call_2)
    expected_3.calls.append(call_1)
    expected_3.calls.append(call_2)
    actual = list(strategy.get_merged_variants([variant, non_variant]))
    expected = [expected_1, expected_2, expected_3]
    self.assertEqual(sorted(actual), sorted(expected))

  def test_merge_snp_with_non_variant_same_start(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)

    variant = vcfio.Variant(
        reference_name='1',
        start=5,
        end=6,
        reference_bases='A',
        alternate_bases=['C'],
        names=['v'],
        filters=['vf'],
        quality=1)
    non_variant = vcfio.Variant(
        reference_name='1',
        start=5,
        end=10,
        reference_bases='A',
        alternate_bases=['<NON_REF>'],
        names=['nv'],
        filters=['nvf'],
        quality=2)

    call_1 = vcfio.VariantCall(name='1', genotype=[1, 0])
    call_2 = vcfio.VariantCall(name='2', genotype=[0, 0])
    variant.calls.append(call_1)
    non_variant.calls.append(call_2)
    expected_1 = vcfio.Variant(
        reference_name='1',
        start=6,
        end=10,
        alternate_bases=['<NON_REF>'],
        names=['nv'],
        filters=['nvf'],
        quality=2)
    expected_2 = vcfio.Variant(
        reference_name='1',
        start=5,
        end=6,
        reference_bases='A',
        alternate_bases=['C'],
        names=['v'],
        filters=['vf'],
        quality=1)
    expected_1.calls.append(call_2)
    expected_2.calls.append(call_1)
    expected_2.calls.append(call_2)
    actual = list(strategy.get_merged_variants([variant, non_variant]))
    expected = [expected_1, expected_2]
    self.assertEqual(sorted(actual), sorted(expected))

  def test_merge_snp_with_non_variant_same_end(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)

    variant = vcfio.Variant(
        reference_name='1',
        start=5,
        end=6,
        reference_bases='A',
        alternate_bases=['C'],
        names=['v'],
        filters=['vf'],
        quality=1)
    non_variant = vcfio.Variant(
        reference_name='1',
        start=0,
        end=6,
        alternate_bases=['<NON_REF>'],
        names=['nv'],
        filters=['nvf'],
        quality=2)

    call_1 = vcfio.VariantCall(name='1', genotype=[1, 0])
    call_2 = vcfio.VariantCall(name='2', genotype=[0, 0])
    variant.calls.append(call_1)
    non_variant.calls.append(call_2)
    expected_1 = vcfio.Variant(
        reference_name='1',
        start=0,
        end=5,
        alternate_bases=['<NON_REF>'],
        names=['nv'],
        filters=['nvf'],
        quality=2)
    expected_2 = vcfio.Variant(
        reference_name='1',
        start=5,
        end=6,
        reference_bases='A',
        alternate_bases=['C'],
        names=['v'],
        filters=['vf'],
        quality=1)
    expected_1.calls.append(call_2)
    expected_2.calls.append(call_1)
    expected_2.calls.append(call_2)
    actual = list(strategy.get_merged_variants([variant, non_variant]))
    expected = [expected_1, expected_2]
    self.assertEqual(sorted(actual), sorted(expected))

  def test_merge_mnps(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)

    variant_1 = vcfio.Variant(
        reference_name='1',
        start=5,
        end=8,
        reference_bases='GTC',
        alternate_bases=['G', 'GTCG'],
        names=['mnp1', 'mnp2'],
        filters=['f1', 'f2'],
        quality=1)
    variant_2 = vcfio.Variant(
        reference_name='1',
        start=5,
        end=8,
        reference_bases='GTC',
        alternate_bases=['G', 'GTCG'],
        names=['mnp2', 'mnp3'],
        filters=['f2', 'f3'],
        quality=2)
    call_1 = vcfio.VariantCall(name='1', genotype=[1, 2])
    call_2 = vcfio.VariantCall(name='2', genotype=[2, 0])
    expected = vcfio.Variant(
        reference_name='1',
        start=5,
        end=8,
        reference_bases='GTC',
        alternate_bases=['G', 'GTCG'],
        names=['mnp1', 'mnp2', 'mnp3'],
        filters=['f1', 'f2', 'f3'],
        quality=2)
    expected.calls.append(call_1)
    expected.calls.append(call_2)

    variant_1.calls.append(call_1)
    variant_2.calls.append(call_2)
    actual = list(strategy.get_merged_variants([variant_1, variant_2]))
    self.assertEqual(actual, [expected])

  def test_align_non_variant(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None, 2)

    non_variant = vcfio.Variant(reference_name='1', start=5, end=12)

    expected = copy.deepcopy(non_variant)
    expected.start = 8
    expected.end = 10

    actual = list(strategy.get_merged_variants([non_variant], '1:8'))
    self.assertEqual(actual, [expected])

  def test_overlapping_three_non_variants(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)
    non_variant_1 = vcfio.Variant(reference_name='1', start=0, end=10)
    non_variant_2 = vcfio.Variant(reference_name='1', start=3, end=5)
    non_variant_3 = vcfio.Variant(reference_name='1', start=4, end=9)
    call_1 = vcfio.VariantCall('1', [0, 0])
    call_2 = vcfio.VariantCall('2', [0, 0])
    call_3 = vcfio.VariantCall('3', [0, 0])
    non_variant_1.calls.append(call_1)
    non_variant_2.calls.append(call_2)
    non_variant_3.calls.append(call_3)

    expected_1 = vcfio.Variant(reference_name='1', start=0, end=3)
    expected_2 = vcfio.Variant(reference_name='1', start=3, end=4)
    expected_3 = vcfio.Variant(reference_name='1', start=4, end=5)
    expected_4 = vcfio.Variant(reference_name='1', start=5, end=9)
    expected_5 = vcfio.Variant(reference_name='1', start=9, end=10)
    expected_1.calls.append(call_1)
    expected_2.calls.append(call_1)
    expected_2.calls.append(call_2)
    expected_3.calls.append(call_1)
    expected_3.calls.append(call_2)
    expected_3.calls.append(call_3)
    expected_4.calls.append(call_1)
    expected_4.calls.append(call_3)
    expected_5.calls.append(call_1)
    expected = [expected_1, expected_2, expected_3, expected_4, expected_5]
    actual = list(strategy.get_merged_variants(
        [non_variant_1, non_variant_2, non_variant_3]))
    self.assertEqual(sorted(actual), sorted(expected))

  def test_non_variant_split_by_snp(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None)
    non_variant = vcfio.Variant(reference_name='1', start=0, end=10)
    variant = vcfio.Variant(
        reference_name='1',
        start=5,
        end=6,
        reference_bases='C',
        alternate_bases=['A'])
    call_1 = vcfio.VariantCall(name='1', genotype=[0, 0])
    call_2 = vcfio.VariantCall(name='2', genotype=[1, 0])
    non_variant.calls.append(call_1)
    variant.calls.append(call_2)
    expected_1 = vcfio.Variant(
        reference_name='1',
        start=0,
        end=5)
    expected_2 = vcfio.Variant(
        reference_name='1',
        start=5,
        end=6,
        reference_bases='C',
        alternate_bases=['A'])
    expected_3 = vcfio.Variant(reference_name='1', start=6, end=10)
    expected_1.calls.append(call_1)
    expected_2.calls.append(call_1)
    expected_2.calls.append(call_2)
    expected_3.calls.append(call_1)

    actual = list(strategy.get_merged_variants([non_variant, variant]))
    expected = [expected_1, expected_2, expected_3]
    self.assertEqual(sorted(actual), sorted(expected))
