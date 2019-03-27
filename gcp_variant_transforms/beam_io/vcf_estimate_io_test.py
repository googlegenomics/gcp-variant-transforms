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

"""Tests for vcfio_header_io module."""

import os
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam.io.source_test_utils as source_test_utils
from apache_beam.testing.test_pipeline import TestPipeline
from gcp_variant_transforms.beam_io.vcf_estimate_io import VcfEstimate
from gcp_variant_transforms.beam_io.vcf_estimate_io import VcfEstimateSource
from gcp_variant_transforms.beam_io.vcf_estimate_io import GetAllEstimates
from gcp_variant_transforms.beam_io.vcf_estimate_io import GetEstimates
from gcp_variant_transforms.testing import asserts
from gcp_variant_transforms.testing import temp_dir
from gcp_variant_transforms.testing import testdata_util


def _construct_estimate(headers, records, file_name=None):
  single_line_size = len(records[0].encode('utf8'))
  total_record_size = sum([len(x.encode('utf8')) for x in records])
  total_header_size = sum([len(x.encode('utf8')) for x in headers])
  return VcfEstimate(
      file_name=file_name,
      estimated_line_count=float(total_record_size)/single_line_size,
      samples=headers[-1].split()[9:],
      size_in_bytes=total_record_size+total_header_size)

def _get_estimate_from_lines(lines, file_name=None):
  for i in range(len(lines)): # pylint: disable=consider-using-enumerate
    if lines[i][0] != '#':
      return _construct_estimate(lines[:i], lines[i:], file_name)
  raise ValueError('Missing record line.')

class VcfEstimateSourceTest(unittest.TestCase):

  # TODO(msaul): Replace get_full_dir() with function from utils.
  # Distribution should skip tests that need VCF files due to large size
  VCF_FILE_DIR_MISSING = not os.path.exists(testdata_util.get_full_dir())

  def setUp(self):
    self.lines = testdata_util.get_sample_vcf_file_lines()
    self.headers = testdata_util.get_sample_vcf_header_lines()
    self.records = testdata_util.get_sample_vcf_record_lines()

  def _create_file_and_read_estimates(self):
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(suffix='.vcf', lines=self.lines)
      estimates = source_test_utils.read_from_source(
          VcfEstimateSource(filename))
      return estimates[0]

  def test_vcf_estimate_eq(self):
    estimate_1 = _get_estimate_from_lines(self.lines)
    estimate_2 = _get_estimate_from_lines(self.lines)
    self.assertEqual(estimate_1, estimate_2)

  def test_read_file_estimates(self):
    estimate = self._create_file_and_read_estimates()
    self.assertEqual(estimate,
                     _get_estimate_from_lines(self.lines, estimate.file_name))

  def test_empty_header_raises_error(self):
    self.lines = testdata_util.get_sample_vcf_record_lines()
    with self.assertRaises(ValueError):
      self._create_file_and_read_estimates()

  def test_read_file_pattern(self):
    with temp_dir.TempDir() as tempdir:
      lines_1 = self.headers[1:2] + self.headers[-1:] + self.records[:2]
      lines_2 = self.headers[2:4] + self.headers[-1:] + self.records[2:4]
      lines_3 = self.headers[4:5] + self.headers[-1:] + self.records[4:]
      file_name_1 = tempdir.create_temp_file(suffix='.vcf', lines=lines_1)
      file_name_2 = tempdir.create_temp_file(suffix='.vcf', lines=lines_2)
      file_name_3 = tempdir.create_temp_file(suffix='.vcf', lines=lines_3)

      actual = source_test_utils.read_from_source(VcfEstimateSource(
          os.path.join(tempdir.get_path(), '*.vcf')))

      expected = [_get_estimate_from_lines(lines, file_name=file_name)
                  for lines, file_name in [(lines_1, file_name_1),
                                           (lines_2, file_name_2),
                                           (lines_3, file_name_3)]]

      asserts.header_vars_equal(expected)(actual)

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_read_single_file_large(self):
    test_data_conifgs = [
        {'file': 'valid-4.0.vcf', 'line_count': 4, 'size': 1500},
        {'file': 'valid-4.0.vcf.gz', 'line_count': 6, 'size': 727},
        {'file': 'valid-4.0.vcf.bz2', 'line_count': 7, 'size': 781},
        {'file': 'valid-4.1-large.vcf', 'line_count': 14425, 'size': 832396},
        {'file': 'valid-4.2.vcf', 'line_count': 10, 'size': 3195},
    ]
    for config in test_data_conifgs:
      read_data = source_test_utils.read_from_source(VcfEstimateSource(
          testdata_util.get_full_file_path(config['file'])))
      self.assertEqual(config['line_count'],
                       int(read_data[0].estimated_line_count))
      self.assertEqual(config['size'], read_data[0].size_in_bytes)

  def test_pipeline_read_file_headers(self):

    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(suffix='.vcf', lines=self.lines)

      pipeline = TestPipeline()
      pcoll = pipeline | 'GetEstimates' >> GetEstimates(filename)

      assert_that(pcoll,
                  equal_to([_get_estimate_from_lines(self.lines, filename)]))
      pipeline.run()

  def test_pipeline_read_all_file_headers(self):
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(suffix='.vcf', lines=self.lines)

      pipeline = TestPipeline()
      pcoll = (pipeline
               | 'Create' >> beam.Create([filename])
               | 'GetAllEstimates' >> GetAllEstimates(filename))

      assert_that(pcoll,
                  equal_to([_get_estimate_from_lines(self.lines, filename)]))
      pipeline.run()




  def test_pipeline_read_file_pattern(self):
    with temp_dir.TempDir() as tempdir:
      lines_1 = self.headers[1:2] + self.headers[-1:] + self.records[:2]
      lines_2 = self.headers[2:4] + self.headers[-1:] + self.records[2:4]
      lines_3 = self.headers[4:5] + self.headers[-1:] + self.records[4:]
      file_name_1 = tempdir.create_temp_file(suffix='.vcf', lines=lines_1)
      file_name_2 = tempdir.create_temp_file(suffix='.vcf', lines=lines_2)
      file_name_3 = tempdir.create_temp_file(suffix='.vcf', lines=lines_3)

      pipeline = TestPipeline()
      pcoll = pipeline | 'ReadHeaders' >> GetEstimates(
          os.path.join(tempdir.get_path(), '*.vcf'))

      expected = [_get_estimate_from_lines(lines, file_name=file_name)
                  for lines, file_name in [(lines_1, file_name_1),
                                           (lines_2, file_name_2),
                                           (lines_3, file_name_3)]]
      assert_that(pcoll, asserts.header_vars_equal(expected))
      pipeline.run()




  def test_pipeline_read_all_file_pattern(self):
    with temp_dir.TempDir() as tempdir:
      lines_1 = self.headers[1:2] + self.headers[-1:] + self.records[:2]
      lines_2 = self.headers[2:4] + self.headers[-1:] + self.records[2:4]
      lines_3 = self.headers[4:5] + self.headers[-1:] + self.records[4:]
      file_name_1 = tempdir.create_temp_file(suffix='.vcf', lines=lines_1)
      file_name_2 = tempdir.create_temp_file(suffix='.vcf', lines=lines_2)
      file_name_3 = tempdir.create_temp_file(suffix='.vcf', lines=lines_3)

      pipeline = TestPipeline()
      pcoll = pipeline | 'ReadHeaders' >> GetEstimates(
          os.path.join(tempdir.get_path(), '*.vcf'))
      pcoll = (pipeline
               | 'Create' >> beam.Create(
                   [os.path.join(tempdir.get_path(), '*.vcf')])
               | 'GetAllEstimates' >> GetAllEstimates())

      expected = [_get_estimate_from_lines(lines, file_name=file_name)
                  for lines, file_name in [(lines_1, file_name_1),
                                           (lines_2, file_name_2),
                                           (lines_3, file_name_3)]]
      assert_that(pcoll, asserts.header_vars_equal(expected))
      pipeline.run()
