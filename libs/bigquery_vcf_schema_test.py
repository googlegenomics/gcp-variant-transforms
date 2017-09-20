"""Tests for bigquery_vcf_schema module."""

from collections import OrderedDict
import sys
import unittest

from beam_io.vcfio import Variant
from beam_io.vcfio import VariantCall
from beam_io.vcfio import VariantInfo
from libs.bigquery_vcf_schema import _ColumnKeyConstants as ColumnKeyConstants
from libs.bigquery_vcf_schema import _TableFieldConstants as TableFieldConstants
from libs.bigquery_vcf_schema import generate_schema_from_header_fields
from libs.bigquery_vcf_schema import get_row_from_variant
from libs.vcf_header_parser import HeaderFields

from vcf.parser import _Format as Format
from vcf.parser import _Info as Info
from vcf.parser import field_counts


class GemerateSchemaFromHeaderFieldsTest(unittest.TestCase):
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
    header_fields = HeaderFields({}, {})
    self._assert_fields_equal(self._generate_expected_fields(),
                              generate_schema_from_header_fields(header_fields))

  def test_info_header_fields(self):
    infos = OrderedDict([
        ('I1', Info('I1', 1, 'String', 'desc', 'src', 'v')),
        ('I2', Info('I2', 2, 'Integer', 'desc', 'src', 'v')),
        ('IA', Info('IA', field_counts['A'], 'Float', 'desc', 'src', 'v')),
        ('IU', Info('IU', field_counts['.'], 'Character', 'desc', 'src', 'v')),
        ('IG', Info('IG', field_counts['G'], 'String', 'desc', 'src', 'v')),
        ('I0', Info('I0', 0, 'Flag', 'desc', 'src', 'v')),
        ('IA2', Info('IA2', field_counts['A'], 'Float', 'desc', 'src', 'v'))])
    header_fields = HeaderFields(infos, {})

    self._assert_fields_equal(
        self._generate_expected_fields(
            alt_fields=['IA', 'IA2'],
            info_fields=['I1', 'I2', 'IU', 'IG', 'I0']),
        generate_schema_from_header_fields(header_fields))

    # Test with split_alternate_allele_info_fields=False.
    actual_schema = generate_schema_from_header_fields(
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
    formats = OrderedDict([
        ('F1', Format('F1', 1, 'String', 'desc')),
        ('F2', Format('F2', 2, 'Integer', 'desc')),
        ('FU', Format('FU', field_counts['.'], 'Float', 'desc'))])
    header_fields = HeaderFields(infos, formats)
    self._assert_fields_equal(
        self._generate_expected_fields(
            alt_fields=['IA'],
            call_fields=['F1', 'F2', 'FU'],
            info_fields=['I1']),
        generate_schema_from_header_fields(header_fields))


class GetRowFromVariantTest(unittest.TestCase):
  """Test cases for the ``get_row_from_variant`` library function."""

  def test_all_fields(self):
    variant = Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'AF': VariantInfo([0.1, 0.2], 'A'),
              'AF2': VariantInfo([0.2, 0.3], 'A'),
              'I1': VariantInfo('some data', '1'),
              'I2': VariantInfo(['data1', 'data2'], '2')},
        calls=[
            VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'HQ': [10, 20]}),
            VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10, 'FLAG1': True})])
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
             'GQ': 10, 'FLAG1': True}],
        'I1': 'some data',
        'I2': ['data1', 'data2']}
    self.assertEqual(expected_row, get_row_from_variant(variant))

    # Test with split_alternate_allele_info_fields=False.
    expected_row[ColumnKeyConstants.ALTERNATE_BASES] = [
        {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A'},
        {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT'}]
    expected_row['AF'] = [0.1, 0.2]
    expected_row['AF2'] = [0.2, 0.3]
    self.assertEqual(
        expected_row,
        get_row_from_variant(variant, split_alternate_allele_info_fields=False))

  def test_no_alternate_bases(self):
    variant = Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=['q10'],
        info={'A1': VariantInfo('some data', '1'),
              'A2': VariantInfo(['data1', 'data2'], '2')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.NAMES: [],
        ColumnKeyConstants.QUALITY: None,
        ColumnKeyConstants.FILTER: ['q10'],
        ColumnKeyConstants.CALLS: [],
        'A1': 'some data',
        'A2': ['data1', 'data2']}
    self.assertEqual(expected_row, get_row_from_variant(variant))

  def test_some_fields_set(self):
    variant = Variant(
        reference_name='chr19', start=None, end=123, reference_bases=None,
        alternate_bases=[], quality=20)
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: None,
        ColumnKeyConstants.END_POSITION: 123,
        ColumnKeyConstants.REFERENCE_BASES: None,
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.NAMES: [],
        ColumnKeyConstants.QUALITY: 20,
        ColumnKeyConstants.FILTER: [],
        ColumnKeyConstants.CALLS: []}
    self.assertEqual(expected_row, get_row_from_variant(variant))

  def test_no_field_set(self):
    variant = Variant()
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: None,
        ColumnKeyConstants.START_POSITION: None,
        ColumnKeyConstants.END_POSITION: None,
        ColumnKeyConstants.REFERENCE_BASES: None,
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.NAMES: [],
        ColumnKeyConstants.QUALITY: None,
        ColumnKeyConstants.FILTER: [],
        ColumnKeyConstants.CALLS: []}
    self.assertEqual(expected_row, get_row_from_variant(variant))

  def test_null_repeated_fields(self):
    variant = Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=['q10'],
        info={'AI': VariantInfo([0, 1, None], '3'),
              'AB': VariantInfo([True, None, False], '3'),
              'AF': VariantInfo([0.1, 0.2, None, 0.4], '4'),
              'AS': VariantInfo([None, 'data1', 'data2'], '3')})
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.NAMES: [],
        ColumnKeyConstants.QUALITY: None,
        ColumnKeyConstants.FILTER: ['q10'],
        ColumnKeyConstants.CALLS: [],
        'AI': [0, 1, -sys.maxint],
        'AB': [True, False, False],
        'AF': [0.1, 0.2, -sys.maxint, 0.4],
        'AS': ['.', 'data1', 'data2']}
    self.assertEqual(expected_row, get_row_from_variant(variant))
