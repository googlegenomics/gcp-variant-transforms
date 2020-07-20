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

"""Tests for `bigquery_vcf_data_converter` module."""



import unittest
from typing import Dict  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_schema_descriptor
from gcp_variant_transforms.libs import bigquery_vcf_data_converter
from gcp_variant_transforms.libs import bigquery_row_generator
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs.bigquery_util import ColumnKeyConstants
from gcp_variant_transforms.libs.bigquery_util import TableFieldConstants
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy
from gcp_variant_transforms.testing import vcf_header_util
from gcp_variant_transforms.testing.testdata_util import hash_name


def _get_processed_variant(variant, header_num_dict=None):
  # TODO(bashir2): To make this more of a "unit" test, we should create
  # ProcessedVariant instances directly (instead of Variant) and avoid calling
  # create_processed_variant here. Then we should also add cases that
  # have annotation fields.
  header_fields = vcf_header_util.make_header(header_num_dict or {})
  return processed_variant.ProcessedVariantFactory(
      header_fields).create_processed_variant(variant)


# TODO(allieychen): Use the sample schema in script `bigquery_schema_util`.
def _get_table_schema():
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
      name='IF',
      type=TableFieldConstants.TYPE_FLOAT,
      mode=TableFieldConstants.MODE_NULLABLE,
      description='INFO foo desc'))
  schema.fields.append(bigquery.TableFieldSchema(
      name='IF2',
      type=TableFieldConstants.TYPE_FLOAT,
      mode=TableFieldConstants.MODE_NULLABLE,
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


def _get_big_query_row():
  # type: (None) -> Dict[unicode, Any]
  """Returns one sample BigQuery row for testing."""
  return {str(ColumnKeyConstants.REFERENCE_NAME): str('chr19'),
          str(ColumnKeyConstants.START_POSITION): 11,
          str(ColumnKeyConstants.END_POSITION): 12,
          str(ColumnKeyConstants.REFERENCE_BASES): 'C',
          str(ColumnKeyConstants.NAMES): [str('rs1'), str('rs2')],
          str(ColumnKeyConstants.QUALITY): 2,
          str(ColumnKeyConstants.FILTER): [str('PASS')],
          str(ColumnKeyConstants.CALLS): [
              {str(ColumnKeyConstants.CALLS_SAMPLE_ID): (
                  hash_name('Sample1')),
               str(ColumnKeyConstants.CALLS_GENOTYPE): [0, 1],
               str(ColumnKeyConstants.CALLS_PHASESET): str('*'),
               str('GQ'): 20, str('FIR'): [10, 20]},
              {str(ColumnKeyConstants.CALLS_SAMPLE_ID): (
                  hash_name('Sample2')),
               str(ColumnKeyConstants.CALLS_GENOTYPE): [1, 0],
               str(ColumnKeyConstants.CALLS_PHASESET): None,
               str('GQ'): 10, str('FB'): True}
          ],
          str(ColumnKeyConstants.ALTERNATE_BASES): [
              {str(ColumnKeyConstants.ALTERNATE_BASES_ALT): str('A'),
               str('IFR'): 1,
               str('IFR2'): 0.2},
              {str(ColumnKeyConstants.ALTERNATE_BASES_ALT): str('TT'),
               str('IFR'): 0.2,
               str('IFR2'): 0.3}
          ],
          str('IS'): str('some data'),
          str('ISR'): [str('data1'), str('data2')]}


class _DummyVariantMergeStrategy(variant_merge_strategy.VariantMergeStrategy):
  """A dummy strategy. It just adds a new field to the schema."""

  def modify_bigquery_schema(self, schema, info_keys):
    schema.fields.append(bigquery.TableFieldSchema(
        name='ADDED_BY_MERGER',
        type=TableFieldConstants.TYPE_STRING,
        mode=TableFieldConstants.MODE_NULLABLE))


class VariantGeneratorTest(unittest.TestCase):
  """Test cases for `VariantGenerator` class."""

  def setUp(self):
    self._variant_generator = bigquery_vcf_data_converter.VariantGenerator()

  def test_alternate_bases(self):
    alternate_base_records = _get_big_query_row()[
        ColumnKeyConstants.ALTERNATE_BASES]

    expected_alternate_bases = ['A', 'TT']

    self.assertEqual(
        expected_alternate_bases,
        self._variant_generator._get_alternate_bases(alternate_base_records))

  def test_get_variant_info(self):
    row = _get_big_query_row()
    expected_variant_info = {'IFR': [1, 0.2],
                             'IFR2': [0.2, 0.3],
                             'IS': 'some data',
                             'ISR': ['data1', 'data2']}

    self.assertEqual(expected_variant_info,
                     self._variant_generator._get_variant_info(row))

  def test_get_variant_info_annotation(self):
    variant_generator = bigquery_vcf_data_converter.VariantGenerator({
        'CSQ': ['allele', 'Consequence', 'AF', 'IMPACT']
    })
    row = {
        str(ColumnKeyConstants.ALTERNATE_BASES): [
            {
                str(ColumnKeyConstants.ALTERNATE_BASES_ALT): 'G',
                str('CSQ'): [
                    {'allele': 'G',
                     'Consequence': 'upstream_gene_variant',
                     'AF': '',
                     'IMPACT': 'MODIFIER'},
                    {'allele': 'G',
                     'Consequence': 'upstream_gene_variant',
                     'AF': '0.1',
                     'IMPACT': ''}]
            },
            {
                str(ColumnKeyConstants.ALTERNATE_BASES_ALT): 'T',
                str('CSQ'): [
                    {'allele': 'T',
                     'Consequence': '',
                     'AF': '',
                     'IMPACT': 'MODIFIER'},
                    {'allele': 'T',
                     'Consequence': 'upstream_gene_variant',
                     'AF': '0.6',
                     'IMPACT': ''}]

            },
            {
                str(ColumnKeyConstants.ALTERNATE_BASES_ALT): 'TT',
                str('CSQ'): []
            }
        ]
    }

    expected_variant_info = {
        'CSQ': [
            'G|upstream_gene_variant||MODIFIER',
            'G|upstream_gene_variant|0.1|',
            'T|||MODIFIER',
            'T|upstream_gene_variant|0.6|']
    }
    self.assertEqual(expected_variant_info,
                     variant_generator._get_variant_info(row))

  def test_get_variant_calls(self):
    variant_call_records = _get_big_query_row()[ColumnKeyConstants.CALLS]

    expected_calls = [
        vcfio.VariantCall(
            sample_id=hash_name('Sample1'), genotype=[0, 1], phaseset='*',
            info={'GQ': 20, 'FIR': [10, 20]}),
        vcfio.VariantCall(
            sample_id=hash_name('Sample2'), genotype=[1, 0],
            info={'GQ': 10, 'FB': True}),
    ]

    self.assertEqual(
        expected_calls,
        self._variant_generator._get_variant_calls(variant_call_records))

  def test_convert_bq_row_to_variant(self):
    row = _get_big_query_row()
    expected_variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IFR': [1, 0.2], 'IFR2': [0.2, 0.3],
              'IS': 'some data', 'ISR': ['data1', 'data2']},
        calls=[
            vcfio.VariantCall(
                sample_id=hash_name('Sample1'), genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'FIR': [10, 20]}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample2'), genotype=[1, 0],
                info={'GQ': 10, 'FB': True})
        ]
    )

    self.assertEqual(expected_variant,
                     self._variant_generator.convert_bq_row_to_variant(row))


class ConverterCombinationTest(unittest.TestCase):
  """Test cases for combining the BigQuery VCF data converters."""

  def setUp(self):
    self._variant_generator = bigquery_vcf_data_converter.VariantGenerator()
    self._schema_descriptor = bigquery_schema_descriptor.SchemaDescriptor(
        _get_table_schema())
    self._conflict_resolver = (
        vcf_field_conflict_resolver.FieldConflictResolver())
    self._row_generator = bigquery_row_generator.VariantCallRowGenerator(
        self._schema_descriptor, self._conflict_resolver)

  def test_bq_row_to_variant_to_bq_row(self):
    row = _get_big_query_row()
    header_num_dict = {'IFR': 'A', 'IFR2': 'A', 'IS': '1', 'ISR': '2'}
    variant = self._variant_generator.convert_bq_row_to_variant(row)
    proc_variant = _get_processed_variant(variant, header_num_dict)
    converted_row = list(self._row_generator.get_rows(proc_variant))
    self.assertEqual([row], converted_row)

  def test_variant_to_bq_row_to_variant(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IFR': [0.1, 0.2],
              'IFR2': [0.2, 0.3],
              'IS': 'some data',
              'ISR': ['data1', 'data2']},
        calls=[
            vcfio.VariantCall(
                sample_id=hash_name('Sample1'), genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'FIR': [10, 20]}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample2'), genotype=[1, 0],
                info={'GQ': 10, 'FB': True}),
            vcfio.VariantCall(sample_id=hash_name('Sample3'),
                              genotype=[vcfio.MISSING_GENOTYPE_VALUE])])
    header_num_dict = {'IFR': 'A', 'IFR2': 'A', 'IS': '1', 'ISR': '2'}

    proc_variant = _get_processed_variant(variant, header_num_dict)
    row = list(self._row_generator.get_rows(proc_variant))
    converted_variant = self._variant_generator.convert_bq_row_to_variant(
        row[0])
    self.assertEqual(variant, converted_variant)
