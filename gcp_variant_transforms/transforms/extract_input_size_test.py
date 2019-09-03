# Copyright 2019 Google LLC.
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

"""Tests for ``extract_input_size`` module."""

import os
import unittest

from apache_beam import transforms
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from gcp_variant_transforms.beam_io.vcf_estimate_io import VcfEstimate
from gcp_variant_transforms.testing import temp_dir
from gcp_variant_transforms.transforms import extract_input_size


class ExtractInputSizeTest(unittest.TestCase):
  def _create_vcf_estimates(self):
    vcf_estimate_1 = VcfEstimate(file_name='vcf_estimate_1',
                                 estimated_variant_count=10,
                                 samples=['s1', 's2'],
                                 size_in_bytes=100)
    vcf_estimate_2 = VcfEstimate(file_name='vcf_estimate_2',
                                 estimated_variant_count=15,
                                 samples=['s1', 's3'],
                                 size_in_bytes=200)
    vcf_estimate_3 = VcfEstimate(file_name='vcf_estimate_3',
                                 estimated_variant_count=7,
                                 samples=['s2', 's3', 's4'],
                                 size_in_bytes=300)
    return [vcf_estimate_1, vcf_estimate_2, vcf_estimate_3]

  def _create_sample_map(self):
    return [('s1', [10, 15]), ('s2', [10, 7]), ('s3', [15, 7]), ('s4', [7])]


  def test_get_file_sizes(self):
    vcf_estimates = self._create_vcf_estimates()

    pipeline = TestPipeline()
    size = (
        pipeline
        | transforms.Create(vcf_estimates)
        | 'GetFilesSize' >> extract_input_size.GetFilesSize())
    assert_that(size, equal_to([600]))
    pipeline.run()

  def test_get_estimated_variant_count(self):
    vcf_estimates = self._create_vcf_estimates()

    pipeline = TestPipeline()
    estimated_variant_count = (
        pipeline
        | transforms.Create(vcf_estimates)
        | 'GetEstimatedVariantCount' >>
        extract_input_size.GetEstimatedVariantCount())
    assert_that(estimated_variant_count, equal_to([32]))
    pipeline.run()

  def test_get_sample_map(self):
    vcf_estimates = self._create_vcf_estimates()

    pipeline = TestPipeline()
    sample_map = (
        pipeline
        | transforms.Create(vcf_estimates)
        | 'GetSampleMap' >> extract_input_size.GetSampleMap())
    assert_that(sample_map, equal_to(self._create_sample_map()))
    pipeline.run()

  def test_get_estimated_value_count(self):
    sample_map = self._create_sample_map()

    pipeline = TestPipeline()
    estimated_value_count = (
        pipeline
        | transforms.Create(sample_map)
        | 'GetEstimatedValueCount' >>
        extract_input_size.GetEstimatedValueCount())
    assert_that(estimated_value_count, equal_to([71]))
    pipeline.run()

  def test_get_estimated_sample_count(self):
    sample_map = self._create_sample_map()

    pipeline = TestPipeline()
    estimated_sample_count = (
        pipeline
        | transforms.Create(sample_map)
        | 'GetEstimatedSampleCount' >>
        extract_input_size.GetEstimatedSampleCount())
    assert_that(estimated_sample_count, equal_to([4]))
    pipeline.run()

  def test_print_estimates_to_file(self):
    with temp_dir.TempDir() as tempdir:
      file_path = os.path.join(tempdir.get_path(), 'test_file_name')
      extract_input_size.print_estimates_to_file(1, 2, 3, 4, 5, file_path)
      with FileSystems.open(file_path) as f:
        lines = f.readlines()
      self.assertEqual([int(line.strip()) for line in lines], [1, 2, 3, 4, 5])
