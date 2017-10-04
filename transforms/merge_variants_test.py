"""Tests for merge_variants module."""

import unittest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.transforms import Create

from beam_io.vcfio import Variant
from beam_io.vcfio import VariantCall
from beam_io.vcfio import VariantInfo

from libs.variant_merge.move_to_calls_strategy import MoveToCallsStrategy
from testing import asserts
from transforms.merge_variants import MergeVariants


class MergeVariantsTest(unittest.TestCase):
  """Test cases for the ``MergeVariants`` transform."""

  def _get_sample_merged_variants(self):
    variant_1 = Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1'], quality=2,
        filters=['PASS'],
        info={'A1': VariantInfo('some data', '1'),
              'A2': VariantInfo(['data1', 'data2'], '2')},
        calls=[
            VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'HQ': [10, 20]}),
            VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10, 'FLAG1': True}),
        ]
    )
    variant_2 = Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1'], quality=20,
        filters=['q10'],
        info={'A1': VariantInfo('some data2', '2'),
              'A3': VariantInfo(['data3', 'data4'], '2')},
        calls=[
            VariantCall(name='Sample3', genotype=[1, 1]),
            VariantCall(
                name='Sample4', genotype=[1, 0],
                info={'GQ': 20}),
        ]
    )
    merged_variant = Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1'],
        filters=['PASS', 'q10'], quality=20,
        info={'A2': VariantInfo(['data1', 'data2'], '2'),
              'A3': VariantInfo(['data3', 'data4'], '2')},
        calls=[
            VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'HQ': [10, 20], 'A1': 'some data'}),
            VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10, 'FLAG1': True, 'A1': 'some data'}),
            VariantCall(
                name='Sample3', genotype=[1, 1],
                info={'A1': 'some data2'}),
            VariantCall(
                name='Sample4', genotype=[1, 0],
                info={'GQ': 20, 'A1': 'some data2'}),
        ]
    )
    return [variant_1, variant_2], merged_variant

  def _get_sample_unmerged_variants(self):
    # Start/end are different from merged variants.
    variant_1 = Variant(
        reference_name='19', start=123, end=125, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs2'],
        calls=[VariantCall(name='Unmerged1', genotype=[0, 1])])
    # Ordering of alternate_bases is different from merged variants.
    variant_2 = Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['TT', 'A'], names=['rs3'],
        calls=[VariantCall(name='Unmerged2', genotype=[0, 1])])
    return [variant_1, variant_2]

  def test_merge_variants(self):
    variant_merger = MoveToCallsStrategy('^A1$', False, False)
    variant_list, merged_variant = self._get_sample_merged_variants()
    unmerged_variant_list = self._get_sample_unmerged_variants()
    pipeline = TestPipeline()
    merged_variants = (
        pipeline
        | Create(variant_list + unmerged_variant_list)
        | 'MergeVariants' >> MergeVariants(variant_merger))
    assert_that(merged_variants,
                asserts.variants_equal_to_ignore_order([merged_variant] +
                                                       unmerged_variant_list))
    pipeline.run()
