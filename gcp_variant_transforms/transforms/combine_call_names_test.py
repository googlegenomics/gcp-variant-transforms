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

"""Tests for `combine_call_names` module."""

import unittest

from apache_beam import transforms
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.transforms import combine_call_names


class GetCallNamesTest(unittest.TestCase):
  """Test cases for the `CallNamesCombiner` transform."""

  def test_call_names_combiner_pipeline(self):
    call_names = ['sample1', 'sample2', 'sample3']
    variant_calls = [
        vcfio.VariantCall(name=call_names[0]),
        vcfio.VariantCall(name=call_names[1]),
        vcfio.VariantCall(name=call_names[2])
    ]
    variants = [
        vcfio.Variant(calls=[variant_calls[0], variant_calls[1]]),
        vcfio.Variant(calls=[variant_calls[1], variant_calls[2]])
    ]

    pipeline = TestPipeline()
    combined_call_names = (
        pipeline
        | transforms.Create(variants)
        | 'CombineCallNames' >> combine_call_names.CallNamesCombiner())
    assert_that(combined_call_names, equal_to([call_names]))
    pipeline.run()

  def test_call_names_combiner_pipeline_same_call_names(self):
    call_names = ['sample2', 'sample1', 'sample3']
    variant_calls = [
        vcfio.VariantCall(name=call_names[0]),
        vcfio.VariantCall(name=call_names[1]),
        vcfio.VariantCall(name=call_names[2])
    ]
    variants = [
        vcfio.Variant(calls=[variant_calls[0],
                             variant_calls[1],
                             variant_calls[2]]),
        vcfio.Variant(calls=[variant_calls[0],
                             variant_calls[1],
                             variant_calls[2]])
    ]

    pipeline = TestPipeline()
    combined_call_names = (
        pipeline
        | transforms.Create(variants)
        | 'CombineCallNames' >> combine_call_names.CallNamesCombiner())
    assert_that(combined_call_names, equal_to([call_names]))
    pipeline.run()

  def test_call_names_combiner_pipeline_duplicate_call_names(self):
    variant_call = vcfio.VariantCall(name='sample1')
    variants = [vcfio.Variant(calls=[variant_call, variant_call])]

    pipeline = TestPipeline()
    _ = (
        pipeline
        | transforms.Create(variants)
        | 'CombineCallNames' >> combine_call_names.CallNamesCombiner())
    with self.assertRaises(ValueError):
      pipeline.run()
