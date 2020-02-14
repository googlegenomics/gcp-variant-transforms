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

"""Tests for vcf_parser module."""

from __future__ import absolute_import

import logging
import unittest
from itertools import permutations

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io.vcfio import Variant
from gcp_variant_transforms.beam_io.vcfio import VariantCall
from gcp_variant_transforms.testing.testdata_util import hash_name

class VariantTest(unittest.TestCase):

  def _assert_variants_equal(self, actual, expected):
    self.assertEqual(
        sorted(expected),
        sorted(actual))

  def test_sort_variants(self):
    sorted_variants = [
        Variant(reference_name='a', start=20, end=22),
        Variant(reference_name='a', start=20, end=22, quality=20),
        Variant(reference_name='b', start=20, end=22),
        Variant(reference_name='b', start=21, end=22),
        Variant(reference_name='b', start=21, end=23)]

    for permutation in permutations(sorted_variants):
      self.assertEqual(sorted(permutation), sorted_variants)

  def test_variant_equality(self):
    base_variant = Variant(reference_name='a', start=20, end=22,
                           reference_bases='a', alternate_bases=['g', 't'],
                           names=['variant'], quality=9, filters=['q10'],
                           info={'key': 'value'},
                           calls=[VariantCall(genotype=[0, 0])])
    equal_variant = Variant(reference_name='a', start=20, end=22,
                            reference_bases='a', alternate_bases=['g', 't'],
                            names=['variant'], quality=9, filters=['q10'],
                            info={'key': 'value'},
                            calls=[VariantCall(genotype=[0, 0])])
    different_calls = Variant(reference_name='a', start=20, end=22,
                              reference_bases='a', alternate_bases=['g', 't'],
                              names=['variant'], quality=9, filters=['q10'],
                              info={'key': 'value'},
                              calls=[VariantCall(genotype=[1, 0])])
    missing_field = Variant(reference_name='a', start=20, end=22,
                            reference_bases='a', alternate_bases=['g', 't'],
                            names=['variant'], quality=9, filters=['q10'],
                            info={'key': 'value'})

    self.assertEqual(base_variant, equal_variant)
    self.assertNotEqual(base_variant, different_calls)
    self.assertNotEqual(base_variant, missing_field)


class VariantCallTest(unittest.TestCase):

  def _default_variant_call(self):
    return vcfio.VariantCall(
        sample_id=hash_name('Sample1'), genotype=[1, 0],
        phaseset=vcfio.DEFAULT_PHASESET_VALUE, info={'GQ': 48})

  def test_variant_call_order(self):
    variant_call_1 = self._default_variant_call()
    variant_call_2 = self._default_variant_call()
    self.assertEqual(variant_call_1, variant_call_2)
    variant_call_1.phaseset = 0
    variant_call_2.phaseset = 1
    self.assertGreater(variant_call_2, variant_call_1)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
