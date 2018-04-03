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

"""Tests for variant_to_bigquery module."""

from __future__ import absolute_import

import unittest

from apache_beam import ParDo
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create


from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.libs.bigquery_util import ColumnKeyConstants
from gcp_variant_transforms.testing import mock_bigquery_schema_descriptor
from gcp_variant_transforms.transforms.variant_to_bigquery import _ConvertToBigQueryTableRow as ConvertToBigQueryTableRow


class ConvertToBigQueryTableRowTest(unittest.TestCase):
  """Test cases for the ``ConvertToBigQueryTableRow`` DoFn."""

  def _get_sample_variant_1(self, split_alternate_allele_info_fields=True):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'AF': vcfio.VariantInfo([0.1, 0.2], 'A'),
              'AF2': vcfio.VariantInfo([0.2, 0.3], 'A'),
              'A1': vcfio.VariantInfo('some data', '1'),
              'A2': vcfio.VariantInfo(['data1', 'data2'], '2')},
        calls=[
            vcfio.VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'HQ': [10, 20]}),
            vcfio.VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10, 'FLAG1': True}),
        ]
    )
    row = {ColumnKeyConstants.REFERENCE_NAME: 'chr19',
           ColumnKeyConstants.START_POSITION: 11,
           ColumnKeyConstants.END_POSITION: 12,
           ColumnKeyConstants.REFERENCE_BASES: 'C',
           ColumnKeyConstants.NAMES: ['rs1', 'rs2'],
           ColumnKeyConstants.QUALITY: 2,
           ColumnKeyConstants.FILTER: ['PASS'],
           ColumnKeyConstants.CALLS: [
               {ColumnKeyConstants.CALLS_NAME: 'Sample1',
                ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
                ColumnKeyConstants.CALLS_PHASESET: '*',
                'GQ': 20, 'HQ': [10, 20]},
               {ColumnKeyConstants.CALLS_NAME: 'Sample2',
                ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
                ColumnKeyConstants.CALLS_PHASESET: None,
                'GQ': 10, 'FLAG1': True}],
           'A1': 'some data',
           'A2': ['data1', 'data2']}
    if split_alternate_allele_info_fields:
      row[ColumnKeyConstants.ALTERNATE_BASES] = [
          {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A', 'AF': 0.1, 'AF2': 0.2},
          {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT', 'AF': 0.2, 'AF2': 0.3}]
    else:
      row[ColumnKeyConstants.ALTERNATE_BASES] = [
          {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A'},
          {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT'}]
      row['AF'] = [0.1, 0.2]
      row['AF2'] = [0.2, 0.3]
    return variant, row

  def _get_sample_variant_2(self):
    variant = vcfio.Variant(
        reference_name='20', start=123, end=125, reference_bases='CT',
        alternate_bases=[], filters=['q10', 's10'],
        info={'INTINFO': vcfio.VariantInfo(1234, '1')})
    row = {ColumnKeyConstants.REFERENCE_NAME: '20',
           ColumnKeyConstants.START_POSITION: 123,
           ColumnKeyConstants.END_POSITION: 125,
           ColumnKeyConstants.REFERENCE_BASES: 'CT',
           ColumnKeyConstants.ALTERNATE_BASES: [],
           ColumnKeyConstants.FILTER: ['q10', 's10'],
           ColumnKeyConstants.CALLS: [],
           'INTINFO': 1234}
    return variant, row

  def _get_sample_variant_3(self):
    variant = vcfio.Variant(
        reference_name='20', start=None, end=None, reference_bases=None)
    row = {ColumnKeyConstants.REFERENCE_NAME: '20',
           ColumnKeyConstants.START_POSITION: None,
           ColumnKeyConstants.END_POSITION: None,
           ColumnKeyConstants.REFERENCE_BASES: None,
           ColumnKeyConstants.ALTERNATE_BASES: [],
           ColumnKeyConstants.CALLS: []}
    return variant, row

  def test_convert_variant_to_bigquery_row(self):
    variant_1, row_1 = self._get_sample_variant_1()
    variant_2, row_2 = self._get_sample_variant_2()
    variant_3, row_3 = self._get_sample_variant_3()
    header_fields = vcf_header_parser.HeaderFields({}, {})
    proc_var_1 = processed_variant.ProcessedVariantFactory(
        header_fields).create_processed_variant(variant_1)
    proc_var_2 = processed_variant.ProcessedVariantFactory(
        header_fields).create_processed_variant(variant_2)
    proc_var_3 = processed_variant.ProcessedVariantFactory(
        header_fields).create_processed_variant(variant_3)
    pipeline = TestPipeline()
    bigquery_rows = (
        pipeline
        | Create([proc_var_1, proc_var_2, proc_var_3])
        | 'ConvertToRow' >> ParDo(ConvertToBigQueryTableRow(
            mock_bigquery_schema_descriptor.MockSchemaDescriptor())))
    assert_that(bigquery_rows, equal_to([row_1, row_2, row_3]))
    pipeline.run()
