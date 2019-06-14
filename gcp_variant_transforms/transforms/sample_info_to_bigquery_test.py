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
                                           file_name='file_1')
    vcf_header_2 = vcf_header_io.VcfHeader(samples=['Sample 1', 'Sample 2'],
                                           file_name='file_2')
    file_path_to_file_hash = {'file_1': 'hash_1',
                              'file_2': 'hash_2'}
    expected_rows = [
        {sample_info_table_schema_generator.SAMPLE_ID: 1773518740613211130,
         sample_info_table_schema_generator.FILE_PATH: 'file_1'},
        {sample_info_table_schema_generator.SAMPLE_ID: 2179255556240246529,
         sample_info_table_schema_generator.FILE_PATH: 'file_1'},
        {sample_info_table_schema_generator.SAMPLE_ID: -3371474033889505544,
         sample_info_table_schema_generator.FILE_PATH: 'file_2'},
        {sample_info_table_schema_generator.SAMPLE_ID: -6659115699645196269,
         sample_info_table_schema_generator.FILE_PATH: 'file_2'}
    ]
    pipeline = test_pipeline.TestPipeline()
    bigquery_rows = (
        pipeline
        | transforms.Create([vcf_header_1, vcf_header_2])
        | 'ConvertToRow'
        >> transforms.ParDo(sample_info_to_bigquery.ConvertSampleInfoToRow(
            file_path_to_file_hash)))

    assert_that(bigquery_rows, equal_to(expected_rows))
    pipeline.run()