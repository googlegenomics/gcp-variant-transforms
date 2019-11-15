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

"""Tests for `sample_info_to_bigquery` module."""

import unittest

from apache_beam import transforms
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import sample_info_table_schema_generator
from gcp_variant_transforms.transforms import sample_info_to_bigquery


class ConvertSampleInfoToRowTest(unittest.TestCase):

  def test_convert_sample_info_to_row(self):
    vcf_header_1 = vcf_header_io.VcfHeader(samples=['Sample 1', 'Sample 2'],
                                           file_path='file_1')
    vcf_header_2 = vcf_header_io.VcfHeader(samples=['Sample 1', 'Sample 2'],
                                           file_path='file_2')
    expected_rows = [
        {sample_info_table_schema_generator.SAMPLE_ID: 5961690698012655974,
         sample_info_table_schema_generator.SAMPLE_NAME: 'Sample 1',
         sample_info_table_schema_generator.FILE_PATH: 'file_1'},
        {sample_info_table_schema_generator.SAMPLE_ID: 5854056809620188906,
         sample_info_table_schema_generator.SAMPLE_NAME: 'Sample 2',
         sample_info_table_schema_generator.FILE_PATH: 'file_1'},
        {sample_info_table_schema_generator.SAMPLE_ID: 5259968798637352651,
         sample_info_table_schema_generator.SAMPLE_NAME: 'Sample 1',
         sample_info_table_schema_generator.FILE_PATH: 'file_2'},
        {sample_info_table_schema_generator.SAMPLE_ID: 6253115674664185777,
         sample_info_table_schema_generator.SAMPLE_NAME: 'Sample 2',
         sample_info_table_schema_generator.FILE_PATH: 'file_2'}
    ]
    pipeline = test_pipeline.TestPipeline()
    bigquery_rows = (
        pipeline
        | transforms.Create([vcf_header_1, vcf_header_2])
        | 'ConvertToRow'
        >> transforms.ParDo(sample_info_to_bigquery.ConvertSampleInfoToRow(
            ), False))

    assert_that(bigquery_rows, equal_to(expected_rows))
    pipeline.run()

  def test_convert_sample_info_to_row_without_file_in_hash(self):
    vcf_header_1 = vcf_header_io.VcfHeader(samples=['Sample 1', 'Sample 2'],
                                           file_path='file_1')
    vcf_header_2 = vcf_header_io.VcfHeader(samples=['Sample 1', 'Sample 2'],
                                           file_path='file_2')
    expected_rows = [
        {sample_info_table_schema_generator.SAMPLE_ID: 6721344017406412066,
         sample_info_table_schema_generator.SAMPLE_NAME: 'Sample 1',
         sample_info_table_schema_generator.FILE_PATH: 'file_1'},
        {sample_info_table_schema_generator.SAMPLE_ID: 7224630242958043176,
         sample_info_table_schema_generator.SAMPLE_NAME: 'Sample 2',
         sample_info_table_schema_generator.FILE_PATH: 'file_1'},
        {sample_info_table_schema_generator.SAMPLE_ID: 6721344017406412066,
         sample_info_table_schema_generator.SAMPLE_NAME: 'Sample 1',
         sample_info_table_schema_generator.FILE_PATH: 'file_2'},
        {sample_info_table_schema_generator.SAMPLE_ID: 7224630242958043176,
         sample_info_table_schema_generator.SAMPLE_NAME: 'Sample 2',
         sample_info_table_schema_generator.FILE_PATH: 'file_2'}
    ]
    pipeline = test_pipeline.TestPipeline()
    bigquery_rows = (
        pipeline
        | transforms.Create([vcf_header_1, vcf_header_2])
        | 'ConvertToRow'
        >> transforms.ParDo(sample_info_to_bigquery.ConvertSampleInfoToRow(
            ), True))

    assert_that(bigquery_rows, equal_to(expected_rows))
    pipeline.run()
