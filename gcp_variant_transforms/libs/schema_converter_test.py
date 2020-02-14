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

"""Tests for `schema_converter` module."""

from __future__ import absolute_import

from collections import OrderedDict
import json
from typing import List, Union  # pylint: disable=unused-import
import unittest

import avro
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json


from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io.vcf_header_io import CreateFormatField as createFormat
from gcp_variant_transforms.beam_io.vcf_header_io import CreateInfoField as createInfo
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import schema_converter
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs.bigquery_util import ColumnKeyConstants
from gcp_variant_transforms.libs.bigquery_util import TableFieldConstants
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy
from gcp_variant_transforms.testing import bigquery_schema_util


class _DummyVariantMergeStrategy(variant_merge_strategy.VariantMergeStrategy):
  """A dummy strategy. It just adds a new field to the schema."""

  def modify_bigquery_schema(self, schema, info_keys):
    schema.fields.append(bigquery.TableFieldSchema(
        name='ADDED_BY_MERGER',
        type=TableFieldConstants.TYPE_STRING,
        mode=TableFieldConstants.MODE_NULLABLE))


class GenerateSchemaFromHeaderFieldsTest(unittest.TestCase):
  """Test cases for the ``generate_schema_from_header_fields`` function."""

  def _validate_schema(self, expected_fields, actual_schema):
    """This can be overridden by child classes to do more validations.

    This is called at the end of each test to verify that `actual_schema`
    has all the `expected_fields`.
    """
    self.assertEqual(expected_fields, _get_fields_from_schema(actual_schema))

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
                             ColumnKeyConstants.CALLS_SAMPLE_ID]),
                   '.'.join([ColumnKeyConstants.CALLS,
                             ColumnKeyConstants.CALLS_GENOTYPE]),
                   '.'.join([ColumnKeyConstants.CALLS,
                             ColumnKeyConstants.CALLS_PHASESET])])
    fields.extend(
        ['.'.join([ColumnKeyConstants.CALLS, c]) for c in call_fields or []])
    fields.extend(info_fields or [])
    return fields

  def test_no_header_fields(self):
    header_fields = vcf_header_io.VcfHeader()
    self._validate_schema(
        self._generate_expected_fields(),
        schema_converter.generate_schema_from_header_fields(
            header_fields,
            processed_variant.ProcessedVariantFactory(header_fields)))

  def test_info_header_fields(self):
    infos = OrderedDict([
        ('I1', createInfo('I1', 1, 'String', 'desc', 'src', 'v')),
        ('I2', createInfo('I2', 2, 'Integer', 'desc', 'src', 'v')),
        ('IA', createInfo('IA', 'A', 'Float', 'desc', 'src', 'v')),
        ('IU', createInfo('IU', '.', 'Character', 'desc', 'src', 'v')),
        ('IG', createInfo('IG', 'G', 'String', 'desc', 'src', 'v')),
        ('I0', createInfo('I0', 0, 'Flag', 'desc', 'src', 'v')),
        ('IA2', createInfo('IA2', 'A', 'Float', 'desc', 'src', 'v')),
        ('END',  # END should not be included in the generated schema.
         createInfo('END', 1, 'Integer', 'Special END key', 'src', 'v'))])
    header_fields = vcf_header_io.VcfHeader(infos=infos)

    self._validate_schema(
        self._generate_expected_fields(
            alt_fields=['IA', 'IA2'],
            info_fields=['I1', 'I2', 'IU', 'IG', 'I0']),
        schema_converter.generate_schema_from_header_fields(
            header_fields,
            processed_variant.ProcessedVariantFactory(header_fields)))

    # Test with split_alternate_allele_info_fields=False.
    actual_schema = (
        schema_converter.generate_schema_from_header_fields(
            header_fields,
            processed_variant.ProcessedVariantFactory(
                header_fields, split_alternate_allele_info_fields=False)))
    self._validate_schema(
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
        ('I1', createInfo('I1', 1, 'String', 'desc', 'src', 'v')),
        ('IA', createInfo('IA', 'A', 'Integer', 'desc', 'src', 'v'))])
    # GT and PS should not be set as they're already included in special
    # 'genotype' and 'phaseset' fields.
    formats = OrderedDict([
        ('F1', createFormat('F1', 1, 'String', 'desc')),
        ('F2', createFormat('F2', 2, 'Integer', 'desc')),
        ('FU', createFormat('FU', '.', 'Float', 'desc')),
        ('GT', createFormat('GT', 2, 'Integer', 'Special GT key')),
        ('PS', createFormat('PS', 1, 'Integer', 'Special PS key'))])
    header_fields = vcf_header_io.VcfHeader(infos=infos, formats=formats)
    self._validate_schema(
        self._generate_expected_fields(
            alt_fields=['IA'],
            call_fields=['F1', 'F2', 'FU'],
            info_fields=['I1']),
        schema_converter.generate_schema_from_header_fields(
            header_fields,
            processed_variant.ProcessedVariantFactory(header_fields)))

  def test_bigquery_field_name_sanitize(self):
    infos = OrderedDict([
        ('_', createInfo('_', 1, 'String', 'desc', 'src', 'v')),
        ('_A', createInfo('_A', 1, 'String', 'desc', 'src', 'v')),
        ('0a', createInfo('0a', 1, 'String', 'desc', 'src', 'v')),
        ('A-B*C', createInfo('A-B*C', 1, 'String', 'desc', 'src', 'v')),
        ('I-A', createInfo('I-A', 'A', 'Float', 'desc', 'src', 'v')),
        ('OK_info_09', createInfo('OK_info_09', 1, 'String', 'desc'))])
    formats = OrderedDict([
        ('a^b', createFormat('a^b', 1, 'String', 'desc')),
        ('OK_format_09', createFormat('OK_format_09', 1, 'String', 'desc'))])
    header_fields = vcf_header_io.VcfHeader(infos=infos, formats=formats)
    self._validate_schema(
        self._generate_expected_fields(
            alt_fields=['I_A'],
            call_fields=['a_b', 'OK_format_09'],
            info_fields=['field__', 'field__A', 'field_0a', 'A_B_C',
                         'OK_info_09']),
        schema_converter.generate_schema_from_header_fields(
            header_fields,
            processed_variant.ProcessedVariantFactory(header_fields)))

  def test_variant_merger_modify_schema(self):
    infos = OrderedDict([
        ('I1', createInfo('I1', 1, 'String', 'desc', 'src', 'v')),
        ('IA', createInfo('IA', 'A', 'Integer', 'desc', 'src', 'v'))])
    formats = OrderedDict([('F1', createFormat('F1', 1, 'String', 'desc'))])
    header_fields = vcf_header_io.VcfHeader(infos=infos, formats=formats)
    self._validate_schema(
        self._generate_expected_fields(
            alt_fields=['IA'],
            call_fields=['F1'],
            info_fields=['I1', 'ADDED_BY_MERGER']),
        schema_converter.generate_schema_from_header_fields(
            header_fields,
            processed_variant.ProcessedVariantFactory(header_fields),
            variant_merger=_DummyVariantMergeStrategy()))


class ConvertTableSchemaToJsonAvroSchemaTest(
    GenerateSchemaFromHeaderFieldsTest):
  """Test cases for `convert_table_schema_to_json_avro_schema`.

  This basically works by extending GenerateSchemaFromHeaderFieldsTest such
  that each BigQuery table schema that is generated by tests in that class,
  are converted to Avro schema and verified in this class.
  """

  def _validate_schema(self, expected_fields, actual_schema):
    super(ConvertTableSchemaToJsonAvroSchemaTest, self)._validate_schema(
        expected_fields, actual_schema)
    avro_schema = avro.schema.parse(
        schema_converter.convert_table_schema_to_json_avro_schema(
            actual_schema))
    self.assertEqual(expected_fields,
                     _get_fields_from_avro_type(avro_schema, ''))


class ConvertTableSchemaToJsonBQSchemaTest(
    GenerateSchemaFromHeaderFieldsTest):
  """Test cases for `convert_table_schema_to_json_bq_schema`.

  This basically works by extending GenerateSchemaFromHeaderFieldsTest such
  that each BigQuery table schema that is generated by tests in that class,
  are converted to BQ json, converted back to BQ schema and validated.
  """

  def _validate_schema(self, expected_fields, actual_schema):
    super(ConvertTableSchemaToJsonBQSchemaTest, self)._validate_schema(
        expected_fields, actual_schema)
    json_schema = schema_converter.convert_table_schema_to_json_bq_schema(
        actual_schema)
    # Beam expects schema to be generated from dict with 'fields' item being
    # list of columns, while 'bq mk' command expects just the list of fields.
    updated_json_schema = json.dumps({"fields": json.loads(json_schema)})
    schema_from_json = parse_table_schema_from_json(updated_json_schema)
    self.assertEqual(schema_from_json, actual_schema)


class GenerateHeaderFieldsFromSchemaTest(unittest.TestCase):
  """Test cases for the `generate_header_fields_from_schema` function."""

  def test_add_info_fields_from_alternate_bases_reserved_field(self):
    alternate_bases_record_with_desc = bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.ALTERNATE_BASES,
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='One record for each alternate base (if any).')
    alternate_bases_record_with_desc.fields.append(bigquery.TableFieldSchema(
        name='AF',
        type=bigquery_util.TableFieldConstants.TYPE_FLOAT,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='bigquery desc'))
    infos_with_desc = OrderedDict()
    schema_converter._add_info_fields(
        alternate_bases_record_with_desc, infos_with_desc)
    expected_infos = OrderedDict([
        ('AF', createInfo('AF', 'A', 'Float', 'bigquery desc',
                          None, None))])
    self.assertEqual(infos_with_desc, expected_infos)

    alternate_bases_record_no_desc = bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.ALTERNATE_BASES,
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='One record for each alternate base (if any).')
    alternate_bases_record_no_desc.fields.append(bigquery.TableFieldSchema(
        name='AF',
        type=bigquery_util.TableFieldConstants.TYPE_FLOAT,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description=''))
    infos_no_desc = OrderedDict()
    schema_converter._add_info_fields(
        alternate_bases_record_no_desc, infos_no_desc)
    expected_infos = OrderedDict([
        ('AF', createInfo(
            'AF', 'A', 'Float',
            'Allele frequency for each ALT allele in the same order '
            'as listed (estimated from primary data, not called genotypes',
            None, None))])
    self.assertEqual(infos_no_desc, expected_infos)

  def test_add_info_fields_from_alternate_bases_schema_compatibility(self):
    schema_conflict_info = bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.ALTERNATE_BASES,
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='One record for each alternate base (if any).')
    schema_conflict_info.fields.append(bigquery.TableFieldSchema(
        name='AF',
        type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='desc'))
    with self.assertRaises(ValueError):
      schema_converter._add_info_fields(schema_conflict_info,
                                        OrderedDict())

    infos_allow_incompatible_schema = OrderedDict()
    schema_converter._add_info_fields(
        schema_conflict_info,
        infos_allow_incompatible_schema,
        allow_incompatible_schema=True)
    expected_infos = OrderedDict([
        ('AF', createInfo('AF', 'A', 'Integer', 'desc', None, None))])
    self.assertEqual(infos_allow_incompatible_schema, expected_infos)

  def test_add_info_fields_from_alternate_bases_non_reserved_field(self):
    alternate_bases_record = bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.ALTERNATE_BASES,
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='One record for each alternate base (if any).')
    alternate_bases_record.fields.append(bigquery.TableFieldSchema(
        name='non_reserved',
        type=bigquery_util.TableFieldConstants.TYPE_FLOAT,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='bigquery desc'))
    infos = OrderedDict()
    schema_converter._add_info_fields(
        alternate_bases_record, infos)
    expected_infos = OrderedDict([
        ('non_reserved', createInfo('non_reserved', 'A', 'Float',
                                    'bigquery desc', None, None))])
    self.assertEqual(infos, expected_infos)

  def test_add_info_fields_reserved_field(self):
    field_with_desc = bigquery.TableFieldSchema(
        name='AA',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='bigquery desc')
    infos = OrderedDict()
    schema_converter._add_info_fields(field_with_desc, infos)
    expected_infos = OrderedDict([
        ('AA', createInfo('AA', 1, 'String', 'bigquery desc', None, None))])
    self.assertEqual(infos, expected_infos)

    field_without_desc = bigquery.TableFieldSchema(
        name='AA',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='')
    infos = OrderedDict()
    schema_converter._add_info_fields(field_without_desc, infos)
    expected_infos = OrderedDict([
        ('AA', createInfo('AA', 1, 'String', 'Ancestral allele', None, None))])
    self.assertEqual(infos, expected_infos)

  def test_add_info_fields_reserved_field_schema_compatibility(self):
    field_conflict_info_type = bigquery.TableFieldSchema(
        name='AA',
        type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='desc')
    with self.assertRaises(ValueError):
      schema_converter._add_info_fields(field_conflict_info_type,
                                        OrderedDict())

    field_conflict_info_format = bigquery.TableFieldSchema(
        name='AA',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='desc')
    with self.assertRaises(ValueError):
      schema_converter._add_info_fields(field_conflict_info_format,
                                        OrderedDict())

    info_allow_incompatible_schema = OrderedDict()
    schema_converter._add_info_fields(
        field_conflict_info_format,
        info_allow_incompatible_schema,
        allow_incompatible_schema=True)
    expected_infos = OrderedDict([
        ('AA', createInfo('AA', '.', 'String', 'desc', None, None))])
    self.assertEqual(info_allow_incompatible_schema, expected_infos)

  def test_add_info_fields_non_reserved_field(self):
    non_reserved_field = bigquery.TableFieldSchema(
        name='non_reserved_info',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='')
    infos = OrderedDict()
    schema_converter._add_info_fields(non_reserved_field, infos)
    expected_infos = OrderedDict([
        ('non_reserved_info', createInfo('non_reserved_info', 1, 'String', '',
                                         None, None))])
    self.assertEqual(infos, expected_infos)

  def test_add_format_fields_reserved_field(self):
    calls_record_with_desc = bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.CALLS,
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    calls_record_with_desc.fields.append(bigquery.TableFieldSchema(
        name='GQ',
        type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='bigquery desc'))
    formats = OrderedDict()
    schema_converter._add_format_fields(calls_record_with_desc,
                                        formats)
    expected_formats = OrderedDict([
        ('GQ', createFormat('GQ', 1, 'Integer', 'bigquery desc'))])
    self.assertEqual(formats, expected_formats)

    calls_record_without_desc = bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.CALLS,
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    calls_record_without_desc.fields.append(bigquery.TableFieldSchema(
        name='GQ',
        type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description=''))
    formats = OrderedDict()
    schema_converter._add_format_fields(calls_record_without_desc,
                                        formats)
    expected_formats = OrderedDict([
        ('GQ', createFormat(
            'GQ', 1, 'Integer', 'Conditional genotype quality'))])
    self.assertEqual(formats, expected_formats)

  def test_add_format_fields_reserved_field_schema_compatibility(self):
    schema_conflict_format = bigquery.TableSchema()
    calls_record = bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.CALLS,
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    calls_record.fields.append(bigquery.TableFieldSchema(
        name='GQ',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='desc'))
    schema_conflict_format.fields.append(calls_record)
    with self.assertRaises(ValueError):
      schema_converter.generate_header_fields_from_schema(
          schema_conflict_format)

    formats_allow_incompatible_schema = OrderedDict()
    schema_converter._add_format_fields(
        calls_record,
        formats_allow_incompatible_schema,
        allow_incompatible_schema=True)
    expected_formats = OrderedDict([
        ('GQ', createFormat('GQ', 1, 'String', 'desc'))])
    self.assertEqual(formats_allow_incompatible_schema, expected_formats)

  def test_add_format_fields_non_reserved_field(self):
    calls_record = bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.CALLS,
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    calls_record.fields.append(bigquery.TableFieldSchema(
        name='non_reserved_format',
        type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='bigquery desc'))
    formats = OrderedDict()
    schema_converter._add_format_fields(calls_record, formats)
    expected_formats = OrderedDict([
        ('non_reserved_format',
         createFormat('non_reserved_format', 1, 'Integer', 'bigquery desc'))])
    self.assertEqual(formats, expected_formats)

  def test_generate_header_fields_from_schema(self):
    sample_schema = bigquery_schema_util.get_sample_table_schema()
    header = schema_converter.generate_header_fields_from_schema(
        sample_schema)

    infos = OrderedDict([
        ('AF', createInfo('AF', 'A', 'Float', 'desc', None, None)),
        ('AA', createInfo('AA', 1, 'String', 'desc', None, None)),
        ('IFR', createInfo('IFR', '.', 'Float', 'desc', None, None)),
        ('IS', createInfo('IS', 1, 'String', 'desc', None, None))])
    formats = OrderedDict([
        ('FB', createFormat('FB', 1, 'String', 'desc')),
        ('GQ', createFormat('GQ', 1, 'Integer', 'desc'))])
    expected_header = vcf_header_io.VcfHeader(infos=infos, formats=formats)
    self.assertEqual(header, expected_header)

  def test_generate_header_fields_from_schema_with_annotation(self):
    sample_schema = bigquery_schema_util.get_sample_table_schema(
        with_annotation_fields=True)
    header = schema_converter.generate_header_fields_from_schema(
        sample_schema)

    infos = OrderedDict([
        ('AF', createInfo('AF', 'A', 'Float', 'desc', None, None)),
        ('CSQ', createInfo('CSQ', '.', 'String',
                           'desc Format: Consequence|IMPACT', None, None)),
        ('AA', createInfo('AA', 1, 'String', 'desc', None, None)),
        ('IFR', createInfo('IFR', '.', 'Float', 'desc', None, None)),
        ('IS', createInfo('IS', 1, 'String', 'desc', None, None))])
    formats = OrderedDict([
        ('FB', createFormat('FB', 1, 'String', 'desc')),
        ('GQ', createFormat('GQ', 1, 'Integer',
                            'desc'))])
    expected_header = vcf_header_io.VcfHeader(infos=infos, formats=formats)
    self.assertEqual(header, expected_header)

  def test_generate_header_fields_from_schema_date_type(self):
    schema = bigquery.TableSchema()
    schema.fields.append(bigquery.TableFieldSchema(
        name='partition_date_please_ignore',
        type='Date',
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='Column required by BigQuery partitioning logic.'))
    header = schema_converter.generate_header_fields_from_schema(
        schema)

    expected_header = vcf_header_io.VcfHeader(infos=OrderedDict(),
                                              formats=OrderedDict())
    self.assertEqual(header, expected_header)

  def test_generate_header_fields_from_schema_none_mode(self):
    schema_non_reserved_fields = bigquery.TableSchema()
    schema_non_reserved_fields.fields.append(bigquery.TableFieldSchema(
        name='field',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        description='desc'))
    header = schema_converter.generate_header_fields_from_schema(
        schema_non_reserved_fields)
    infos = OrderedDict([
        ('field', createInfo('field', 1, 'String', 'desc', None, None))])
    formats = OrderedDict()
    expected_header = vcf_header_io.VcfHeader(infos=infos, formats=formats)
    self.assertEqual(header, expected_header)

    schema_reserved_fields = bigquery.TableSchema()
    schema_reserved_fields.fields.append(bigquery.TableFieldSchema(
        name='AA',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        description='desc'))
    header = schema_converter.generate_header_fields_from_schema(
        schema_reserved_fields)
    infos = OrderedDict([
        ('AA', createInfo('AA', 1, 'String', 'desc', None, None))])
    formats = OrderedDict()
    expected_header = vcf_header_io.VcfHeader(infos=infos, formats=formats)
    self.assertEqual(header, expected_header)

  def test_generate_header_fields_from_schema_schema_compatibility(self):
    schema_conflict = bigquery.TableSchema()
    schema_conflict.fields.append(bigquery.TableFieldSchema(
        name='AA',
        type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='desc'))
    with self.assertRaises(ValueError):
      schema_converter.generate_header_fields_from_schema(
          schema_conflict)

    header = schema_converter.generate_header_fields_from_schema(
        schema_conflict,
        allow_incompatible_schema=True)
    infos = OrderedDict([
        ('AA', createInfo('AA', 1, 'Integer', 'desc', None, None))])
    expected_header = vcf_header_io.VcfHeader(infos=infos,
                                              formats=OrderedDict())
    self.assertEqual(header, expected_header)

  def test_generate_header_fields_from_schema_invalid_description(self):
    schema = bigquery.TableSchema()
    schema.fields.append(bigquery.TableFieldSchema(
        name='invalid_description',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='Desc\nThis is added intentionally.'))
    header = schema_converter.generate_header_fields_from_schema(
        schema)

    infos = OrderedDict([
        ('invalid_description', createInfo(
            'invalid_description', 1, 'String',
            'Desc This is added intentionally.', None, None))])
    expected_header = vcf_header_io.VcfHeader(infos=infos,
                                              formats=OrderedDict())
    self.assertEqual(header, expected_header)


class VcfHeaderAndSchemaConverterCombinationTest(unittest.TestCase):
  """Test cases for concatenating VCF header and schema converters."""

  def test_vcf_header_to_schema_to_vcf_header(self):
    infos = OrderedDict([
        ('I1', createInfo('I1', '.', 'String', 'desc', None, None)),
        ('IA', createInfo('IA', '.', 'Integer', 'desc', None, None))])
    formats = OrderedDict([
        ('F1', createFormat('F1', '.', 'String', 'desc')),
        ('F2', createFormat('F2', '.', 'Integer', 'desc')),
        ('FU', createFormat('FU', '.', 'Float', 'desc'))])
    original_header = vcf_header_io.VcfHeader(infos=infos, formats=formats)

    schema = schema_converter.generate_schema_from_header_fields(
        original_header,
        processed_variant.ProcessedVariantFactory(original_header))
    reconstructed_header = (
        schema_converter.generate_header_fields_from_schema(
            schema))

    self.assertEqual(original_header, reconstructed_header)

  def test_schema_to_vcf_header_to_schema(self):
    original_schema = bigquery_schema_util.get_sample_table_schema()
    header = schema_converter.generate_header_fields_from_schema(
        original_schema)
    reconstructed_schema = (
        schema_converter.generate_schema_from_header_fields(
            header, processed_variant.ProcessedVariantFactory(header)))

    self.assertEqual(_get_fields_from_schema(reconstructed_schema),
                     _get_fields_from_schema(original_schema))


def _get_fields_from_schema(schema, prefix=''):
  fields = []
  for field in schema.fields:
    fields.append(prefix + field.name)
    if field.type == TableFieldConstants.TYPE_RECORD:
      fields.extend(_get_fields_from_schema(field, prefix=field.name + '.'))
  return fields


def _get_fields_from_avro_type(field_or_schema, prefix):
  # type: (Union[avro.schema.Field, avro.schema.Schema], str) -> List[str]
  fields = []
  if isinstance(field_or_schema, avro.schema.PrimitiveSchema):
    return []
  t = field_or_schema.type
  if isinstance(t, avro.schema.UnionSchema):
    for s in t.schemas:
      fields.extend(_get_fields_from_avro_type(s, prefix))
  if isinstance(field_or_schema, avro.schema.ArraySchema):
    return _get_fields_from_avro_type(field_or_schema.items, prefix)
  # We need to exclude the name for the case of a UnionSchema that has
  # a RecordSchema as a type member. In this case, the name of the record
  # appears twice in the Avro schema, once at the UnionSchema level and once
  # at the child RecordSchema.
  name = field_or_schema.name
  if name and name not in fields and name != 'TBD':
    fields.extend([prefix + field_or_schema.name])
  if field_or_schema.get_prop('fields'):
    child_prefix = prefix
    if name != 'TBD':
      child_prefix = prefix + field_or_schema.name + '.'
    for f in field_or_schema.fields:
      fields.extend(_get_fields_from_avro_type(f, child_prefix))
  return fields
