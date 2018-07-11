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

"""Test cases for bigquery_to_variant module."""

import unittest

from gcp_variant_transforms.libs.bigquery_util import ColumnKeyConstants
from gcp_variant_transforms.transforms import bigquery_to_variant
from gcp_variant_transforms.beam_io import vcfio


class BigQueryToVariantTest(unittest.TestCase):

  def _get_big_query_row(self):
    return {unicode(ColumnKeyConstants.REFERENCE_NAME): unicode('chr19'),
            unicode(ColumnKeyConstants.START_POSITION): 11,
            unicode(ColumnKeyConstants.END_POSITION): 12,
            unicode(ColumnKeyConstants.REFERENCE_BASES): 'C',
            unicode(ColumnKeyConstants.NAMES): ['rs1', 'rs2'],
            unicode(ColumnKeyConstants.QUALITY): 2,
            unicode(ColumnKeyConstants.FILTER): ['PASS'],
            unicode(ColumnKeyConstants.CALLS): [
                {unicode(ColumnKeyConstants.CALLS_NAME): unicode('Sample1'),
                 unicode(ColumnKeyConstants.CALLS_GENOTYPE): [0, 1],
                 unicode(ColumnKeyConstants.CALLS_PHASESET): unicode('*'),
                 unicode('GQ'): 20, unicode('FIR'): [10, 20]},
                {unicode(ColumnKeyConstants.CALLS_NAME): unicode('Sample2'),
                 unicode(ColumnKeyConstants.CALLS_GENOTYPE): [1, 0],
                 unicode(ColumnKeyConstants.CALLS_PHASESET): None,
                 unicode('GQ'): 10, unicode('FB'): True}
            ],
            unicode(ColumnKeyConstants.ALTERNATE_BASES): [
                {unicode(ColumnKeyConstants.ALTERNATE_BASES_ALT): unicode('A'),
                 unicode('IFR'): None,
                 unicode('IFR2'): 0.2},
                {unicode(ColumnKeyConstants.ALTERNATE_BASES_ALT): unicode('TT'),
                 unicode('IFR'): 0.2,
                 unicode('IFR2'): 0.3}
            ],
            unicode('IS'): unicode('some data'),
            unicode('ISR'): [unicode('data1'), unicode('data2')]}

  def test_alternate_bases(self):
    alternate_base_records = self._get_big_query_row()[
        ColumnKeyConstants.ALTERNATE_BASES]

    expected_alternate_bases = ['A', 'TT']
    bq_to_variant = bigquery_to_variant.BigQueryToVariant()
    self.assertEqual(expected_alternate_bases,
                     bq_to_variant._get_alternate_bases(alternate_base_records))

  def test_get_variant_info(self):
    row = self._get_big_query_row()
    expected_variant_info = {'IFR': [0.2],
                             'IFR2': [0.2, 0.3],
                             'IS': 'some data',
                             'ISR': ['data1', 'data2']}
    bq_to_variant = bigquery_to_variant.BigQueryToVariant()
    self.assertEqual(expected_variant_info,
                     bq_to_variant._get_variant_info(row))

  def test_get_variant_calls(self):
    variant_call_records = self._get_big_query_row()[ColumnKeyConstants.CALLS]

    expected_calls = [
        vcfio.VariantCall(
            name='Sample1', genotype=[0, 1], phaseset='*',
            info={'GQ': 20, 'FIR': [10, 20]}),
        vcfio.VariantCall(
            name='Sample2', genotype=[1, 0],
            info={'GQ': 10, 'FB': True}),
    ]

    bq_to_variant = bigquery_to_variant.BigQueryToVariant()
    self.assertEqual(expected_calls,
                     bq_to_variant._get_variant_calls(variant_call_records))

  def test_convert_bq_row_to_variant(self):
    row = self._get_big_query_row()
    expected_variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IFR': [0.2], 'IFR2': [0.2, 0.3],
              'IS': 'some data', 'ISR': ['data1', 'data2']},
        calls=[
            vcfio.VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'FIR': [10, 20]}),
            vcfio.VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10, 'FB': True})
        ]
    )
    bq_to_variant = bigquery_to_variant.BigQueryToVariant()
    self.assertEqual(expected_variant,
                     bq_to_variant._convert_bq_row_to_variant(row))
