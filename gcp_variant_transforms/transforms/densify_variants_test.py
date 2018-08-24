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

"""Tests for densify_variants module."""

from __future__ import absolute_import

import unittest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.testing import asserts
from gcp_variant_transforms.transforms import densify_variants


class DensifyVariantsTest(unittest.TestCase):
  """Test cases for the ``DensifyVariants`` transform."""

  def test_densify_variants_pipeline_no_calls(self):
    variant_calls = [
        vcfio.VariantCall(name='sample1'),
        vcfio.VariantCall(name='sample2'),
        vcfio.VariantCall(name='sample3'),
    ]
    variants = [
        vcfio.Variant(calls=[variant_calls[0], variant_calls[1]]),
        vcfio.Variant(calls=[variant_calls[1], variant_calls[2]]),
    ]
    pipeline = TestPipeline()
    densified_variants = (
        pipeline
        | Create(variants)
        | 'DensifyVariants' >> densify_variants.DensifyVariants([]))
    assert_that(densified_variants, asserts.has_calls([]))

    pipeline.run()

  def test_densify_variants_pipeline(self):
    call_names = ['sample1', 'sample2', 'sample3']
    variant_calls = [
        vcfio.VariantCall(name=call_names[0]),
        vcfio.VariantCall(name=call_names[1]),
        vcfio.VariantCall(name=call_names[2]),
    ]
    variants = [
        vcfio.Variant(calls=[variant_calls[0], variant_calls[1]]),
        vcfio.Variant(calls=[variant_calls[1], variant_calls[2]]),
    ]

    pipeline = TestPipeline()
    densified_variants = (
        pipeline
        | Create(variants)
        | 'DensifyVariants' >> densify_variants.DensifyVariants(call_names))
    assert_that(densified_variants, asserts.has_calls(call_names))

    pipeline.run()
