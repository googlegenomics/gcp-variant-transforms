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

"""Tests for `combine_sample_ids` module."""

import unittest

from apache_beam import transforms
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.testing.testdata_util import hash_name
from gcp_variant_transforms.transforms import combine_sample_ids


class GetSampleIdsTest(unittest.TestCase):
  """Test cases for the `SampleIdsCombiner` transform."""

  def test_sample_ids_combiner_pipeline_preserve_sample_order_error(self):
    sample_ids = [hash_name('sample1'),
                  hash_name('sample2'),
                  hash_name('sample3')]
    variant_calls = [
        vcfio.VariantCall(sample_id=sample_ids[0]),
        vcfio.VariantCall(sample_id=sample_ids[1]),
        vcfio.VariantCall(sample_id=sample_ids[2])
    ]
    variants = [
        vcfio.Variant(calls=[variant_calls[0], variant_calls[1]]),
        vcfio.Variant(calls=[variant_calls[1], variant_calls[2]])
    ]

    pipeline = TestPipeline()
    _ = (
        pipeline
        | transforms.Create(variants)
        | 'CombineSampleIds' >>
        combine_sample_ids.SampleIdsCombiner(
            preserve_sample_order=True))
    with self.assertRaises(ValueError):
      pipeline.run()

  def test_sample_ids_combiner_pipeline_preserve_sample_order(self):
    sample_ids = [hash_name('sample2'),
                  hash_name('sample1'),
                  hash_name('sample3')]
    variant_calls = [
        vcfio.VariantCall(sample_id=sample_ids[0]),
        vcfio.VariantCall(sample_id=sample_ids[1]),
        vcfio.VariantCall(sample_id=sample_ids[2])
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
    combined_sample_ids = (
        pipeline
        | transforms.Create(variants)
        | 'CombineSampleIds' >>
        combine_sample_ids.SampleIdsCombiner(
            preserve_sample_order=True))
    assert_that(combined_sample_ids, equal_to([sample_ids]))
    pipeline.run()

  def test_sample_ids_combiner_pipeline(self):
    sample_ids = [hash_name('sample3'),
                  hash_name('sample2'),
                  hash_name('sample1')]
    variant_calls = [
        vcfio.VariantCall(sample_id=sample_ids[0]),
        vcfio.VariantCall(sample_id=sample_ids[1]),
        vcfio.VariantCall(sample_id=sample_ids[2])
    ]
    variants = [
        vcfio.Variant(calls=[variant_calls[0], variant_calls[1]]),
        vcfio.Variant(calls=[variant_calls[1], variant_calls[2]])
    ]

    pipeline = TestPipeline()
    combined_sample_ids = (
        pipeline
        | transforms.Create(variants)
        | 'CombineSampleIds' >>
        combine_sample_ids.SampleIdsCombiner())
    assert_that(combined_sample_ids, equal_to([sample_ids]))
    pipeline.run()

  def test_sample_ids_combiner_pipeline_duplicate_sample_ids(self):
    variant_call = vcfio.VariantCall(sample_id=hash_name('sample1'))
    variants = [vcfio.Variant(calls=[variant_call, variant_call])]

    pipeline = TestPipeline()
    _ = (
        pipeline
        | transforms.Create(variants)
        | 'CombineSampleIds' >>
        combine_sample_ids.SampleIdsCombiner())
    with self.assertRaises(ValueError):
      pipeline.run()
