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

"""Tests for limit_write module."""


import unittest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.transforms import limit_write


class LimitWriteTest(unittest.TestCase):
  """Test cases for the ``LimitWrite`` PTransform."""

  def _get_sample_variants(self):
    variant1 = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C')
    variant2 = vcfio.Variant(
        reference_name='20', start=123, end=125, reference_bases='CT')
    variant3 = vcfio.Variant(
        reference_name='20', start=None, end=None, reference_bases=None)
    variant4 = vcfio.Variant(
        reference_name='20', start=123, end=125, reference_bases='CT')
    return [variant1, variant2, variant3, variant4]


  def test_limit_write_default_shard_limit(self):
    variants = self._get_sample_variants()
    input_pcoll = Create(variants)
    pipeline = TestPipeline()
    output_pcoll = (
        pipeline
        | input_pcoll
        | 'LimitWrite' >> limit_write.LimitWrite(4500))
    assert_that(output_pcoll, equal_to(variants))
    pipeline.run()

  def test_limit_write_shard_limit_4(self):
    variants = self._get_sample_variants()
    input_pcoll = Create(variants)
    pipeline = TestPipeline()
    output_pcoll = (
        pipeline
        | input_pcoll
        | 'LimitWrite' >> limit_write.LimitWrite(4))
    assert_that(output_pcoll, equal_to(variants))
    pipeline.run()

  def test_limit_write_shard_limit_1(self):
    variants = self._get_sample_variants()
    input_pcoll = Create(variants)
    pipeline = TestPipeline()
    output_pcoll = (
        pipeline
        | input_pcoll
        | 'LimitWrite' >> limit_write.LimitWrite(1))
    assert_that(output_pcoll, equal_to(variants))
    pipeline.run()
