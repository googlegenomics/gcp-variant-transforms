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

"""Tests for bigquery_vcf_schema module."""

from __future__ import absolute_import

from collections import OrderedDict
import json
import sys
import unittest

from vcf.parser import _Format as Format
from vcf.parser import _Info as Info
from vcf.parser import field_counts

from apache_beam.io.gcp.internal.clients import bigquery

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_vcf_schema
from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.libs.bigquery_vcf_schema import _TableFieldConstants as TableFieldConstants
from gcp_variant_transforms.libs.bigquery_vcf_schema import ColumnKeyConstants
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy


class _DummyVariantMergeStrategy(variant_merge_strategy.VariantMergeStrategy):
  """A dummy strategy. It just adds a new field to the schema."""

  def modify_bigquery_schema(self, schema, info_keys):
    schema.fields.append(bigquery.TableFieldSchema(
        name='ADDED_BY_MERGER',
        type=TableFieldConstants.TYPE_STRING,
        mode=TableFieldConstants.MODE_NULLABLE))


class GenerateSchemaFromHeaderFieldsTest(unittest.TestCase):
  """Test cases for the ``generate_schema_from_header_fields`` function."""

  def _generate_expected_fields(self, alt_fields=None, call_fields=None,
                                info_fields=None):
    fields = [ColumnKeyConstants.REFERENCE_NAME,
              ColumnKeyConstants.START_POSITION,
              ColumnKeyConstants.END_POSITION,
              ColumnKeyConstants.REFERENCE_BASES,
              ColumnKeyConstants.ALTERNATE_BASES,
              '.'.join([ColumnKeyConstants.ALTERNATE_BASES,
                        ColumnKeyConstants.ALTERNATE_BASES_ALT])]
    fields.extend(
        ['.'.join([ColumnKeyConstants.ALTERNATE_BASES, a])
         for a in alt_fields or []])
    fields.extend([ColumnKeyConstants.NAMES,
                   ColumnKeyConstants.QUALITY,
                   ColumnKeyConstants.FILTER,
                   ColumnKeyConstants.CALLS,
                   '.'.join([ColumnKeyConstants.CALLS,
                             ColumnKeyConstants.CALLS_NAME]),
                   '.'.join([ColumnKeyConstants.CALLS,
                             ColumnKeyConstants.CALLS_GENOTYPE]),
                   '.'.join([ColumnKeyConstants.CALLS,
                             ColumnKeyConstants.CALLS_PHASESET])])
    fields.extend(
        ['.'.join([ColumnKeyConstants.CALLS, c]) for c in call_fields or []])
    fields.extend(info_fields or [])
    return fields

  def _get_fields_from_schema(self, schema, prefix=''):
    fields = []
    for field in schema.fields:
      fields.append(prefix + field.name)
      if field.type == TableFieldConstants.TYPE_RECORD:
        fields.extend(self._get_fields_from_schema(field,
                                                   prefix=field.name + '.'))
    return fields

  def _assert_fields_equal(self, expected_fields, actual_schema):
    self.assertEqual(expected_fields,
                     self._get_fields_from_schema(actual_schema))

  def test_no_header_fields(self):
    header_fields = vcf_header_parser.HeaderFields({}, {})
    self._assert_fields_equal(
        self._generate_expected_fields(),
        bigquery_vcf_schema.generate_schema_from_header_fields(header_fields))

  def test_info_header_fields(self):
    infos = OrderedDict([
        ('I1', Info('I1', 1, 'String', 'desc', 'src', 'v')),
        ('I2', Info('I2', 2, 'Integer', 'desc', 'src', 'v')),
        ('IA', Info('IA', field_counts['A'], 'Float', 'desc', 'src', 'v')),
        ('IU', Info('IU', field_counts['.'], 'Character', 'desc', 'src', 'v')),
        ('IG', Info('IG', field_counts['G'], 'String', 'desc', 'src', 'v')),
        ('I0', Info('I0', 0, 'Flag', 'desc', 'src', 'v')),
        ('IA2', Info('IA2', field_counts['A'], 'Float', 'desc', 'src', 'v')),
        ('END',  # END should not be included in the generated schema.
         Info('END', 1, 'Integer', 'Special END key', 'src', 'v'))])
    header_fields = vcf_header_parser.HeaderFields(infos, {})

    self._assert_fields_equal(
        self._generate_expected_fields(
            alt_fields=['IA', 'IA2'],
            info_fields=['I1', 'I2', 'IU', 'IG', 'I0']),
        bigquery_vcf_schema.generate_schema_from_header_fields(header_fields))

    # Test with split_alternate_allele_info_fields=False.
    actual_schema = bigquery_vcf_schema.generate_schema_from_header_fields(
        header_fields, split_alternate_allele_info_fields=False)
    self._assert_fields_equal(
        self._generate_expected_fields(
            info_fields=['I1', 'I2', 'IA', 'IU', 'IG', 'I0', 'IA2']),
        actual_schema)
    # Verify types and modes.
    expected_type_modes = {
        'I1': (TableFieldConstants.TYPE_STRING,
               TableFieldConstants.MODE_NULLABLE),
        'I2': (TableFieldConstants.TYPE_INTEGER,
               TableFieldConstants.MODE_REPEATED),
        'IA': (TableFieldConstants.TYPE_FLOAT,
               TableFieldConstants.MODE_REPEATED),
        'IU': (TableFieldConstants.TYPE_STRING,
               TableFieldConstants.MODE_REPEATED),
        'IG': (TableFieldConstants.TYPE_STRING,
               TableFieldConstants.MODE_REPEATED),
        'I0': (TableFieldConstants.TYPE_BOOLEAN,
               TableFieldConstants.MODE_NULLABLE),
        'IA2': (TableFieldConstants.TYPE_FLOAT,
                TableFieldConstants.MODE_REPEATED)}
    for field in actual_schema.fields:
      if field.name in expected_type_modes:
        expected_type, expected_mode = expected_type_modes[field.name]
        self.assertEqual(expected_type, field.type)
        self.assertEqual(expected_mode, field.mode)

  def test_info_and_format_header_fields(self):
    infos = OrderedDict([
        ('I1', Info('I1', 1, 'String', 'desc', 'src', 'v')),
        ('IA', Info('IA', field_counts['A'], 'Integer', 'desc', 'src', 'v'))])
    # GT and PS should not be set as they're already included in special
    # 'genotype' and 'phaseset' fields.
    formats = OrderedDict([
        ('F1', Format('F1', 1, 'String', 'desc')),
        ('F2', Format('F2', 2, 'Integer', 'desc')),
        ('FU', Format('FU', field_counts['.'], 'Float', 'desc')),
        ('GT', Format('GT', 2, 'Integer', 'Special GT key')),
        ('PS', Format('PS', 1, 'Integer', 'Special PS key'))])
    header_fields = vcf_header_parser.HeaderFields(infos, formats)
    self._assert_fields_equal(
        self._generate_expected_fields(
            alt_fields=['IA'],
            call_fields=['F1', 'F2', 'FU'],
            info_fields=['I1']),
        bigquery_vcf_schema.generate_schema_from_header_fields(header_fields))

  def test_bigquery_field_name_sanitize(self):
    infos = OrderedDict([
        ('_', Info('_', 1, 'String', 'desc', 'src', 'v')),
        ('_A', Info('_A', 1, 'String', 'desc', 'src', 'v')),
        ('0a', Info('0a', 1, 'String', 'desc', 'src', 'v')),
        ('A-B*C', Info('A-B*C', 1, 'String', 'desc', 'src', 'v')),
        ('I-A', Info('I-A', field_counts['A'], 'Float', 'desc', 'src', 'v')),
        ('OK_info_09', Format('OK_info_09', 1, 'String', 'desc'))])
    formats = OrderedDict([
        ('a^b', Format('a^b', 1, 'String', 'desc')),
        ('OK_format_09', Format('OK_format_09', 1, 'String', 'desc'))])
    header_fields = vcf_header_parser.HeaderFields(infos, formats)
    self._assert_fields_equal(
        self._generate_expected_fields(
            alt_fields=['I_A'],
            call_fields=['a_b', 'OK_format_09'],
            info_fields=['field__', 'field__A', 'field_0a', 'A_B_C',
                         'OK_info_09']),
        bigquery_vcf_schema.generate_schema_from_header_fields(header_fields))

  def test_variant_merger_modify_schema(self):
    infos = OrderedDict([
        ('I1', Info('I1', 1, 'String', 'desc', 'src', 'v')),
        ('IA', Info('IA', field_counts['A'], 'Integer', 'desc', 'src', 'v'))])
    formats = OrderedDict([('F1', Format('F1', 1, 'String', 'desc'))])
    header_fields = vcf_header_parser.HeaderFields(infos, formats)
    self._assert_fields_equal(
        self._generate_expected_fields(
            alt_fields=['IA'],
            call_fields=['F1'],
            info_fields=['I1', 'ADDED_BY_MERGER']),
        bigquery_vcf_schema.generate_schema_from_header_fields(
            header_fields, variant_merger=_DummyVariantMergeStrategy()))


class GetRowsFromVariantTest(unittest.TestCase):
  """Test cases for the ``get_rows_from_variant`` library function."""

  def _get_row_list_from_variant(self, variant, **kwargs):
    return list(bigquery_vcf_schema.get_rows_from_variant(variant, **kwargs))

  def test_all_fields(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'AF': vcfio.VariantInfo([0.1, 0.2], 'A'),
              'AF2': vcfio.VariantInfo([0.2, 0.3], 'A'),
              'I1': vcfio.VariantInfo('some data', '1'),
              'I2': vcfio.VariantInfo(['data1', 'data2'], '2')},
        calls=[
            vcfio.VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'HQ': [10, 20]}),
            vcfio.VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10, 'FLAG1': True}),
            vcfio.VariantCall(
                name='Sample3', genotype=[vcfio.MISSING_GENOTYPE_VALUE])])
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'C',
        ColumnKeyConstants.ALTERNATE_BASES: [
            {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A',
             'AF': 0.1, 'AF2': 0.2},
            {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT',
             'AF': 0.2, 'AF2': 0.3}],
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
             'GQ': 10, 'FLAG1': True},
            {ColumnKeyConstants.CALLS_NAME: 'Sample3',
             ColumnKeyConstants.CALLS_GENOTYPE: [vcfio.MISSING_GENOTYPE_VALUE],
             ColumnKeyConstants.CALLS_PHASESET: None}],
        'I1': 'some data',
        'I2': ['data1', 'data2']}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

    # Test with split_alternate_allele_info_fields=False.
    expected_row[ColumnKeyConstants.ALTERNATE_BASES] = [
        {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A'},
        {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT'}]
    expected_row['AF'] = [0.1, 0.2]
    expected_row['AF2'] = [0.2, 0.3]
    self.assertEqual(
        [expected_row],
        self._get_row_list_from_variant(
            variant, split_alternate_allele_info_fields=False))

  def test_no_alternate_bases(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=['q10'],
        info={'A1': vcfio.VariantInfo('some data', '1'),
              'A2': vcfio.VariantInfo(['data1', 'data2'], '2')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.FILTER: ['q10'],
        ColumnKeyConstants.CALLS: [],
        'A1': 'some data',
        'A2': ['data1', 'data2']}
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
        info={'AI': vcfio.VariantInfo([0, 1, None], '3'),
              'AB': vcfio.VariantInfo([True, None, False], '3'),
              'AF': vcfio.VariantInfo([0.1, 0.2, None, 0.4], '4'),
              'AS': vcfio.VariantInfo([None, 'data1', 'data2'], '3')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.FILTER: ['q10'],
        ColumnKeyConstants.CALLS: [],
        'AI': [0, 1, -sys.maxint],
        'AB': [True, False, False],
        'AF': [0.1, 0.2, -sys.maxint, 0.4],
        'AS': ['.', 'data1', 'data2']}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_unicode_fields(self):
    sample_unicode_str = u'\xc3\xb6'
    sample_utf8_str = sample_unicode_str.encode('utf-8')
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[sample_unicode_str, sample_utf8_str],
        info={'AS1': vcfio.VariantInfo(sample_utf8_str, '1'),
              'AS2': vcfio.VariantInfo(
                  [sample_unicode_str, sample_utf8_str], '2')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.FILTER: [sample_unicode_str, sample_unicode_str],
        ColumnKeyConstants.CALLS: [],
        'AS1': sample_unicode_str,
        'AS2': [sample_unicode_str, sample_unicode_str]}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_nonstandard_float_values(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[],
        info={'F1': vcfio.VariantInfo(float('inf'), '1'),
              'F2': vcfio.VariantInfo([float('-inf'), float('nan'), 1.2], '3'),
              'F3': vcfio.VariantInfo(float('nan'), '1'),})
    null_replacement_value = -sys.maxint
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [],
        'F1': sys.maxint,
        'F2': [-sys.maxint, null_replacement_value, 1.2],
        'F3': None}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_nonstandard_fields_names(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[],
        info={'A-1': vcfio.VariantInfo('data1', '1'),
              '_A': vcfio.VariantInfo('data2', '2')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [],
        'A_1': 'data1',
        'field__A': 'data2'}
    self.assertEqual([expected_row], self._get_row_list_from_variant(variant))

  def test_sharded_rows(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'AF': vcfio.VariantInfo([0.1, 0.2], 'A'),
              'AF2': vcfio.VariantInfo([0.2, 0.3], 'A'),
              'I1': vcfio.VariantInfo('some data', '1'),},
        calls=[
            vcfio.VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'HQ': [10, 20]}),
            vcfio.VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10, 'FLAG1': True}),
            vcfio.VariantCall(
                name='Sample3', genotype=[1, 0],
                info={'GQ': 30, 'FLAG1': True})])
    expected_rows = [
        {
            ColumnKeyConstants.REFERENCE_NAME: 'chr19',
            ColumnKeyConstants.START_POSITION: 11,
            ColumnKeyConstants.END_POSITION: 12,
            ColumnKeyConstants.REFERENCE_BASES: 'C',
            ColumnKeyConstants.ALTERNATE_BASES: [
                {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A',
                 'AF': 0.1, 'AF2': 0.2},
                {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT',
                 'AF': 0.2, 'AF2': 0.3}],
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
            'I1': 'some data'
        },
        {
            ColumnKeyConstants.REFERENCE_NAME: 'chr19',
            ColumnKeyConstants.START_POSITION: 11,
            ColumnKeyConstants.END_POSITION: 12,
            ColumnKeyConstants.REFERENCE_BASES: 'C',
            ColumnKeyConstants.ALTERNATE_BASES: [
                {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A',
                 'AF': 0.1, 'AF2': 0.2},
                {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT',
                 'AF': 0.2, 'AF2': 0.3}],
            ColumnKeyConstants.NAMES: ['rs1', 'rs2'],
            ColumnKeyConstants.QUALITY: 2,
            ColumnKeyConstants.FILTER: ['PASS'],
            ColumnKeyConstants.CALLS: [
                {ColumnKeyConstants.CALLS_NAME: 'Sample3',
                 ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
                 ColumnKeyConstants.CALLS_PHASESET: None,
                 'GQ': 30, 'FLAG1': True}],
            'I1': 'some data'
        },
    ]

    original_max_row_size = bigquery_vcf_schema._MAX_BIGQUERY_ROW_SIZE_BYTES
    try:
      bigquery_vcf_schema._MAX_BIGQUERY_ROW_SIZE_BYTES = (
          len(json.dumps(expected_rows[0])) + 10)
      self.assertEqual(expected_rows, self._get_row_list_from_variant(variant))
    finally:
      bigquery_vcf_schema._MAX_BIGQUERY_ROW_SIZE_BYTES = original_max_row_size

  def test_omit_empty_sample_calls(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=[], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={},
        calls=[
            vcfio.VariantCall(
                name='Sample1', info={'GQ': vcfio.MISSING_FIELD_VALUE}),
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
