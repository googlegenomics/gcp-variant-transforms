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

"""Tests for bigquery_row_generator module."""

from __future__ import absolute_import

import json
import sys
import unittest
import mock

from apache_beam.io.gcp.internal.clients import bigquery


from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_schema_descriptor
from gcp_variant_transforms.libs import bigquery_row_generator
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.libs.bigquery_util import ColumnKeyConstants
from gcp_variant_transforms.libs.bigquery_util import TableFieldConstants
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy


class _DummyVariantMergeStrategy(variant_merge_strategy.VariantMergeStrategy):
  """A dummy strategy. It just adds a new field to the schema."""

  def modify_bigquery_schema(self, schema, info_keys):
    schema.fields.append(bigquery.TableFieldSchema(
        name='ADDED_BY_MERGER',
        type=TableFieldConstants.TYPE_STRING,
        mode=TableFieldConstants.MODE_NULLABLE))


class BigQueryRowGeneratorTest(unittest.TestCase):
  """Test cases for class ``BigQueryRowGenerator``."""

  def setUp(self):
    self._schema_descriptor = bigquery_schema_descriptor.SchemaDescriptor(
        self._get_table_schema())
    self._conflict_resolver = (
        vcf_field_conflict_resolver.FieldConflictResolver())

    self._row_generator = bigquery_row_generator.BigQueryRowGenerator(
        self._schema_descriptor, self._conflict_resolver)

  def _get_table_schema(self):
    # type (None) -> bigquery.TableSchema
    schema = bigquery.TableSchema()
    schema.fields.append(bigquery.TableFieldSchema(
        name='IB',
        type=TableFieldConstants.TYPE_BOOLEAN,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='INFO foo desc'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='IBR',
        type=TableFieldConstants.TYPE_BOOLEAN,
        mode=TableFieldConstants.MODE_REPEATED,
        description='INFO foo desc'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='II',
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='INFO foo desc'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='II2',
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='INFO foo desc'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='IIR',
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_REPEATED,
        description='INFO foo desc'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='IFR',
        type=TableFieldConstants.TYPE_FLOAT,
        mode=TableFieldConstants.MODE_REPEATED,
        description='INFO foo desc'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='IFR2',
        type=TableFieldConstants.TYPE_FLOAT,
        mode=TableFieldConstants.MODE_REPEATED,
        description='INFO foo desc'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='field__IS',
        type=TableFieldConstants.TYPE_STRING,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='INFO foo desc'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='IS',
        type=TableFieldConstants.TYPE_STRING,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='INFO foo desc'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='ISR',
        type=TableFieldConstants.TYPE_STRING,
        mode=TableFieldConstants.MODE_REPEATED,
        description='INFO foo desc'))
    # Call record.
    call_record = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    call_record.fields.append(bigquery.TableFieldSchema(
        name='FB',
        type=TableFieldConstants.TYPE_BOOLEAN,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='FORMAT foo desc'))
    call_record.fields.append(bigquery.TableFieldSchema(
        name='FI',
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='FORMAT foo desc'))
    call_record.fields.append(bigquery.TableFieldSchema(
        name='GQ',
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='FORMAT foo desc'))
    call_record.fields.append(bigquery.TableFieldSchema(
        name='FIR',
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_REPEATED,
        description='FORMAT foo desc'))
    call_record.fields.append(bigquery.TableFieldSchema(
        name='FSR',
        type=TableFieldConstants.TYPE_STRING,
        mode=TableFieldConstants.MODE_REPEATED,
        description='FORMAT foo desc'))
    schema.fields.append(call_record)
    return schema

  def _get_row_list_from_variant(
      self, variant, allow_incompatible_records=False,
      omit_empty_sample_calls=False, **kwargs):
    # TODO(bashir2): To make this more of a "unit" test, we should create
    # ProcessedVariant instances directly (instead of Variant) and avoid calling
    # create_processed_variant here. Then we should also add cases that
    # have annotation fields.
    header_fields = vcf_header_parser.HeaderFields({}, {})
    proc_var = processed_variant.ProcessedVariantFactory(
        header_fields).create_processed_variant(variant)

    return list(self._row_generator.get_rows(
        proc_var, allow_incompatible_records,
        omit_empty_sample_calls, **kwargs))

  def test_all_fields(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IFR': vcfio.VariantInfo([0.1, 0.2], 'A'),
              'IFR2': vcfio.VariantInfo([0.2, 0.3], 'A'),
              'IS': vcfio.VariantInfo('some data', '1'),
              'ISR': vcfio.VariantInfo(['data1', 'data2'], '2')},
        calls=[
            vcfio.VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'FIR': [10, 20]}),
            vcfio.VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10, 'FB': True}),
            vcfio.VariantCall(
                name='Sample3', genotype=[vcfio.MISSING_GENOTYPE_VALUE])])
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'C',
        ColumnKeyConstants.ALTERNATE_BASES: [
            {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A',
             'IFR': 0.1, 'IFR2': 0.2},
            {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT',
             'IFR': 0.2, 'IFR2': 0.3}],
        ColumnKeyConstants.NAMES: ['rs1', 'rs2'],
        ColumnKeyConstants.QUALITY: 2,
        ColumnKeyConstants.FILTER: ['PASS'],
        ColumnKeyConstants.CALLS: [
            {ColumnKeyConstants.CALLS_NAME: 'Sample1',
             ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
             ColumnKeyConstants.CALLS_PHASESET: '*',
             'GQ': 20, 'FIR': [10, 20]},
            {ColumnKeyConstants.CALLS_NAME: 'Sample2',
             ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
             ColumnKeyConstants.CALLS_PHASESET: None,
             'GQ': 10, 'FB': True},
            {ColumnKeyConstants.CALLS_NAME: 'Sample3',
             ColumnKeyConstants.CALLS_GENOTYPE: [vcfio.MISSING_GENOTYPE_VALUE],
             ColumnKeyConstants.CALLS_PHASESET: None}],
        'IS': 'some data',
        'ISR': ['data1', 'data2']}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_no_alternate_bases(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=['q10'],
        info={'IS': vcfio.VariantInfo('some data', '1'),
              'ISR': vcfio.VariantInfo(['data1', 'data2'], '2')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.FILTER: ['q10'],
        ColumnKeyConstants.CALLS: [],
        'IS': 'some data',
        'ISR': ['data1', 'data2']}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_some_fields_set(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=None, end=123, reference_bases=None,
        alternate_bases=[], quality=20)
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: None,
        ColumnKeyConstants.END_POSITION: 123,
        ColumnKeyConstants.REFERENCE_BASES: None,
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.QUALITY: 20,
        ColumnKeyConstants.CALLS: []}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_no_field_set(self):
    variant = vcfio.Variant()
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: None,
        ColumnKeyConstants.START_POSITION: None,
        ColumnKeyConstants.END_POSITION: None,
        ColumnKeyConstants.REFERENCE_BASES: None,
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: []}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_null_repeated_fields(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=['q10'],
        info={'IIR': vcfio.VariantInfo([0, 1, None], '3'),
              'IBR': vcfio.VariantInfo([True, None, False], '3'),
              'IFR': vcfio.VariantInfo([0.1, 0.2, None, 0.4], '4'),
              'ISR': vcfio.VariantInfo([None, 'data1', 'data2'], '3')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.FILTER: ['q10'],
        ColumnKeyConstants.CALLS: [],
        'IIR': [0, 1, -sys.maxint],
        'IBR': [True, False, False],
        'IFR': [0.1, 0.2, -sys.maxint, 0.4],
        'ISR': ['.', 'data1', 'data2']}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_unicode_fields(self):
    sample_unicode_str = u'\xc3\xb6'
    sample_utf8_str = sample_unicode_str.encode('utf-8')
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[sample_unicode_str, sample_utf8_str],
        info={'IS': vcfio.VariantInfo(sample_utf8_str, '1'),
              'ISR': vcfio.VariantInfo(
                  [sample_unicode_str, sample_utf8_str], '2')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.FILTER: [sample_unicode_str, sample_unicode_str],
        ColumnKeyConstants.CALLS: [],
        'IS': sample_unicode_str,
        'ISR': [sample_unicode_str, sample_unicode_str]}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_nonstandard_float_values(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[],
        info={'II': vcfio.VariantInfo(float('inf'), '1'),
              'IIR': vcfio.VariantInfo([float('-inf'), float('nan'), 1.2], '3'),
              'II2': vcfio.VariantInfo(float('nan'), '1'),})
    null_replacement_value = -sys.maxint
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [],
        'II': sys.maxint,
        'IIR': [-sys.maxint, null_replacement_value, 1.2],
        'II2': None}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_nonstandard_fields_names(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[],
        info={'IS': vcfio.VariantInfo('data1', '1'),
              '_IS': vcfio.VariantInfo('data2', '2')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [],
        'IS': 'data1',
        'field__IS': 'data2'}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_sharded_rows(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IFR': vcfio.VariantInfo([0.1, 0.2], 'A'),
              'IFR2': vcfio.VariantInfo([0.2, 0.3], 'A'),
              'IS': vcfio.VariantInfo('some data', '1'),},
        calls=[
            vcfio.VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'FIR': [10, 20]}),
            vcfio.VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10, 'FB': True}),
            vcfio.VariantCall(
                name='Sample3', genotype=[1, 0],
                info={'GQ': 30, 'FB': True})])
    expected_rows = [
        {
            ColumnKeyConstants.REFERENCE_NAME: 'chr19',
            ColumnKeyConstants.START_POSITION: 11,
            ColumnKeyConstants.END_POSITION: 12,
            ColumnKeyConstants.REFERENCE_BASES: 'C',
            ColumnKeyConstants.ALTERNATE_BASES: [
                {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A',
                 'IFR': 0.1, 'IFR2': 0.2},
                {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT',
                 'IFR': 0.2, 'IFR2': 0.3}],
            ColumnKeyConstants.NAMES: ['rs1', 'rs2'],
            ColumnKeyConstants.QUALITY: 2,
            ColumnKeyConstants.FILTER: ['PASS'],
            ColumnKeyConstants.CALLS: [
                {ColumnKeyConstants.CALLS_NAME: 'Sample1',
                 ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
                 ColumnKeyConstants.CALLS_PHASESET: '*',
                 'GQ': 20, 'FIR': [10, 20]},
                {ColumnKeyConstants.CALLS_NAME: 'Sample2',
                 ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
                 ColumnKeyConstants.CALLS_PHASESET: None,
                 'GQ': 10, 'FB': True}],
            'IS': 'some data'
        },
        {
            ColumnKeyConstants.REFERENCE_NAME: 'chr19',
            ColumnKeyConstants.START_POSITION: 11,
            ColumnKeyConstants.END_POSITION: 12,
            ColumnKeyConstants.REFERENCE_BASES: 'C',
            ColumnKeyConstants.ALTERNATE_BASES: [
                {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A',
                 'IFR': 0.1, 'IFR2': 0.2},
                {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT',
                 'IFR': 0.2, 'IFR2': 0.3}],
            ColumnKeyConstants.NAMES: ['rs1', 'rs2'],
            ColumnKeyConstants.QUALITY: 2,
            ColumnKeyConstants.FILTER: ['PASS'],
            ColumnKeyConstants.CALLS: [
                {ColumnKeyConstants.CALLS_NAME: 'Sample3',
                 ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
                 ColumnKeyConstants.CALLS_PHASESET: None,
                 'GQ': 30, 'FB': True}],
            'IS': 'some data'
        },
    ]
    with mock.patch.object(bigquery_row_generator,
                           '_MAX_BIGQUERY_ROW_SIZE_BYTES',
                           len(json.dumps(expected_rows[0])) + 10):
      self.assertEqual(expected_rows, self._get_row_list_from_variant(variant))

  def test_omit_empty_sample_calls(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=[], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={},
        calls=[
            vcfio.VariantCall(
                name='Sample1', info={'GQ': None}),
            vcfio.VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10}),
            vcfio.VariantCall(
                name='Sample3', genotype=[vcfio.MISSING_GENOTYPE_VALUE,
                                          vcfio.MISSING_GENOTYPE_VALUE])])

    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'C',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.NAMES: ['rs1', 'rs2'],
        ColumnKeyConstants.QUALITY: 2,
        ColumnKeyConstants.FILTER: ['PASS'],
        ColumnKeyConstants.CALLS: [
            {ColumnKeyConstants.CALLS_NAME: 'Sample2',
             ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
             ColumnKeyConstants.CALLS_PHASESET: None,
             'GQ': 10}]}

    self.assertEqual(
        [expected_row],
        self._get_row_list_from_variant(variant, omit_empty_sample_calls=True))

  def test_schema_conflict_in_info_field_type(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[],
        info={'IB': vcfio.VariantInfo(data=1, field_count='1'),
              'II': vcfio.VariantInfo(data=1.1, field_count='1'),
              'IFR': vcfio.VariantInfo(data=[1, 2], field_count='2'),
              'ISR': vcfio.VariantInfo(data=[1.0, 2.0], field_count='2')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [],
        'IB': True,
        'II': 1,
        'IFR': [1.0, 2.0],
        'ISR': ['1.0', '2.0']}
    self.assertEqual([expected_row], self._get_row_list_from_variant(
        variant, allow_incompatible_records=True))

    with self.assertRaises(ValueError):
      variant = vcfio.Variant(
          reference_name='chr19', start=11, end=12, reference_bases='CT',
          alternate_bases=[], filters=[],
          # String cannot be casted to integer.
          info={'II': vcfio.VariantInfo(data='1.1', field_count='1'),})
      self._get_row_list_from_variant(
          variant, allow_incompatible_records=True)
      self.fail('String data for an integer schema must cause an exception')

  def test_schema_conflict_in_info_field_number(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[],
        info={'IB': vcfio.VariantInfo(data=[1, 2], field_count='2'),
              'IBR': vcfio.VariantInfo(data=1, field_count='1'),
              'II': vcfio.VariantInfo(data=[10, 20], field_count='2'),
              'IFR': vcfio.VariantInfo(data=1.1, field_count='1'),
              'ISR': vcfio.VariantInfo(data='foo', field_count='1')},)
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [],
        'IB': True,
        'IBR': [True],
        'II': 10,
        'IFR': [1.1],
        'ISR': ['foo'],}
    self.assertEqual([expected_row], self._get_row_list_from_variant(
        variant, allow_incompatible_records=True))

  def test_schema_conflict_in_format_field_type(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[],
        calls=[
            vcfio.VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'FB': '', 'FI': 1.0, 'FSR': [1, 2]}),
            vcfio.VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'FB': 1, 'FI': True, 'FSR': [1.0, 2.0]})])

    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [],
        ColumnKeyConstants.CALLS: [
            {ColumnKeyConstants.CALLS_NAME: 'Sample1',
             ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
             ColumnKeyConstants.CALLS_PHASESET: '*',
             'FB': False, 'FI': 1, 'FSR': ['1', '2']},
            {ColumnKeyConstants.CALLS_NAME: 'Sample2',
             ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
             ColumnKeyConstants.CALLS_PHASESET: None,
             'FB': True, 'FI': 1, 'FSR': ['1.0', '2.0']},],
    }

    self.assertEqual([expected_row], self._get_row_list_from_variant(
        variant, allow_incompatible_records=True))

    with self.assertRaises(ValueError):
      variant = vcfio.Variant(
          reference_name='chr19', start=11, end=12, reference_bases='CT',
          alternate_bases=[], filters=[],
          # String cannot be casted to integer.
          calls=[
              vcfio.VariantCall(
                  name='Sample1', genotype=[0, 1], phaseset='*',
                  info={'FI': 'string_for_int_field'}),],)
      self._get_row_list_from_variant(
          variant, allow_incompatible_records=True)
      self.fail('String data for an integer schema must cause an exception')

  def test_schema_conflict_in_format_field_number(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[],
        calls=[
            vcfio.VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'FB': [1, 2], 'FI': [1, 2], 'FSR': 'str'}),
            vcfio.VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'FB': [], 'FI': [], 'FSR': ''})])

    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [],
        ColumnKeyConstants.CALLS: [
            {ColumnKeyConstants.CALLS_NAME: 'Sample1',
             ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
             ColumnKeyConstants.CALLS_PHASESET: '*',
             'FB': True, 'FI': 1, 'FSR': ['str']},
            {ColumnKeyConstants.CALLS_NAME: 'Sample2',
             ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
             ColumnKeyConstants.CALLS_PHASESET: None,
             'FB': False, 'FI': None, 'FSR': ['']},],
    }

    self.assertEqual([expected_row], self._get_row_list_from_variant(
        variant, allow_incompatible_records=True))
