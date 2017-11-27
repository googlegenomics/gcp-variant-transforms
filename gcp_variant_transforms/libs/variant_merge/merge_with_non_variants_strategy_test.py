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

import unittest

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs.bigquery_vcf_schema import ColumnKeyConstants
from gcp_variant_transforms.libs.variant_merge import merge_with_non_variants_strategy


class MergeWithNonVariantsStrategyTest(unittest.TestCase):
  """Tests for MergeWithNonVariantsStrategy."""

  def _get_sample_variants(self):
    variant_1 = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'A1': vcfio.VariantInfo('some data', '1'),
              'A2': vcfio.VariantInfo(['data1', 'data2'], '2')},
        calls=[
            vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                              info={'GQ': 20, 'HQ': [10, 20]}),
            vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                              info={'GQ': 10, 'FLAG1': True})])
    variant_2 = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs3'], quality=20,
        filters=['q10'],
        info={'A1': vcfio.VariantInfo('some data2', '2'),
              'A3': vcfio.VariantInfo(['data3', 'data4'], '2')},
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

    # Test single variant merge.
    self.assertEqual([variants[0]], strategy.get_merged_variants([variants[0]]))

    # Test multiple variant merge.
    merged_variant = strategy.get_merged_variants(variants)[0]
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
        merged_variant.info['A1'].data in ('some data', 'some data2'))
    self.assertEqual(vcfio.VariantInfo(['data1', 'data2'], '2'),
                     merged_variant.info['A2'])
    self.assertEqual(vcfio.VariantInfo(['data3', 'data4'], '2'),
                     merged_variant.info['A3'])

  def test_get_merged_variants_move_quality_and_filter_to_calls(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        info_keys_to_move_to_calls_regex='',
        copy_quality_to_calls=True,
        copy_filter_to_calls=True)
    variants = self._get_sample_variants()

    # Test single variant merge.
    single_merged_variant = strategy.get_merged_variants([variants[0]])[0]
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
    merged_variant = strategy.get_merged_variants(variants)[0]
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
        merged_variant.info['A1'].data in ('some data', 'some data2'))
    self.assertEqual(vcfio.VariantInfo(['data1', 'data2'], '2'),
                     merged_variant.info['A2'])
    self.assertEqual(vcfio.VariantInfo(['data3', 'data4'], '2'),
                     merged_variant.info['A3'])

  def test_get_merged_variants_move_info_to_calls(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        info_keys_to_move_to_calls_regex='^A1$',
        copy_quality_to_calls=False,
        copy_filter_to_calls=False)
    variants = self._get_sample_variants()

    # Test single variant merge.
    single_merged_variant = strategy.get_merged_variants([variants[0]])[0]
    self.assertEqual(
        [vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                           info={'GQ': 20, 'HQ': [10, 20], 'A1': 'some data'}),
         vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                           info={'GQ': 10, 'FLAG1': True, 'A1': 'some data'})],
        single_merged_variant.calls)

    # Test multiple variant merge.
    merged_variant = strategy.get_merged_variants(variants)[0]
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
    self.assertEqual(vcfio.VariantInfo(['data1', 'data2'], '2'),
                     merged_variant.info['A2'])
    self.assertEqual(vcfio.VariantInfo(['data3', 'data4'], '2'),
                     merged_variant.info['A3'])

  def test_get_merged_variants_move_everything_to_calls(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        info_keys_to_move_to_calls_regex='.*',
        copy_quality_to_calls=True,
        copy_filter_to_calls=True)
    variants = self._get_sample_variants()

    # Test single variant merge.
    single_merged_variant = strategy.get_merged_variants([variants[0]])[0]
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

    merged_variant = strategy.get_merged_variants(variants)[0]
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
        None, None, None, 2)

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
    merged_variants = strategy.get_merged_variants(variants)
    self.assertEqual(sorted(merged_variants), sorted(variants))

  def test_merge_one_overlap(self):
    strategy = merge_with_non_variants_strategy.MergeWithNonVariantsStrategy(
        None, None, None, 2)

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
    merged_variants = strategy.get_merged_variants(variants)
    self.assertEqual(
        sorted(merged_variants), sorted([merged, variant_2, variant_3]))
