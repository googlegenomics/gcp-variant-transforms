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

"""Tests for `combine_call_sample_ids` module."""

import unittest

from apache_beam import transforms
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.testing.testdata_util import hash_name
from gcp_variant_transforms.transforms import combine_call_sample_ids


class GetCallSampleIdsTest(unittest.TestCase):
  """Test cases for the `CallSampleIdsCombiner` transform."""

  def test_call_sample_ids_combiner_pipe_preserve_call_sample_ids_order_error(
      self):
    call_sample_ids = [hash_name('sample1'),
                       hash_name('sample2'),
                       hash_name('sample3')]
    variant_calls = [
        vcfio.VariantCall(sample_id=call_sample_ids[0]),
        vcfio.VariantCall(sample_id=call_sample_ids[1]),
        vcfio.VariantCall(sample_id=call_sample_ids[2])
    ]
    variants = [
        vcfio.Variant(calls=[variant_calls[0], variant_calls[1]]),
        vcfio.Variant(calls=[variant_calls[1], variant_calls[2]])
    ]

    pipeline = TestPipeline()
    _ = (
        pipeline
        | transforms.Create(variants)
        | 'CombineCallSampleIds' >>
        combine_call_sample_ids.CallSampleIdsCombiner(
            preserve_call_sample_ids_order=True))
    with self.assertRaises(ValueError):
      pipeline.run()

  def test_call_sample_ids_combiner_pipe_preserve_call_sample_ids_order(self):
    call_sample_ids = [hash_name('sample2'),
                       hash_name('sample1'),
                       hash_name('sample3')]
    variant_calls = [
        vcfio.VariantCall(sample_id=call_sample_ids[0]),
        vcfio.VariantCall(sample_id=call_sample_ids[1]),
        vcfio.VariantCall(sample_id=call_sample_ids[2])
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
    combined_call_sample_ids = (
        pipeline
        | transforms.Create(variants)
        | 'CombineCallSampleIds' >>
        combine_call_sample_ids.CallSampleIdsCombiner(
            preserve_call_sample_ids_order=True))
    assert_that(combined_call_sample_ids, equal_to([call_sample_ids]))
    pipeline.run()

  def test_call_sample_ids_combiner_pipeline(self):
    call_sample_ids = [hash_name('sample3'),
                       hash_name('sample2'),
                       hash_name('sample1')]
    variant_calls = [
        vcfio.VariantCall(sample_id=call_sample_ids[0]),
        vcfio.VariantCall(sample_id=call_sample_ids[1]),
        vcfio.VariantCall(sample_id=call_sample_ids[2])
    ]
    variants = [
        vcfio.Variant(calls=[variant_calls[0], variant_calls[1]]),
        vcfio.Variant(calls=[variant_calls[1], variant_calls[2]])
    ]

    pipeline = TestPipeline()
    combined_call_sample_ids = (
        pipeline
        | transforms.Create(variants)
        | 'CombineCallSampleIds' >>
        combine_call_sample_ids.CallSampleIdsCombiner())
    assert_that(combined_call_sample_ids, equal_to([call_sample_ids]))
    pipeline.run()

  def test_call_sample_ids_combiner_pipeline_duplicate_call_sample_ids(self):
    variant_call = vcfio.VariantCall(sample_id=hash_name('sample1'))
    variants = [vcfio.Variant(calls=[variant_call, variant_call])]

    pipeline = TestPipeline()
    _ = (
        pipeline
        | transforms.Create(variants)
        | 'CombineCallSampleIds' >>
        combine_call_sample_ids.CallSampleIdsCombiner())
    with self.assertRaises(ValueError):
      pipeline.run()
