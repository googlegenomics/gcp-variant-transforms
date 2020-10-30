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

"""Tests for `bigquery_row_generator` module."""

import json
import unittest
from typing import Any, Dict  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery
import mock

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_sanitizer
from gcp_variant_transforms.libs import bigquery_schema_descriptor
from gcp_variant_transforms.libs import bigquery_row_generator
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs.bigquery_util import ColumnKeyConstants
from gcp_variant_transforms.libs.bigquery_util import TableFieldConstants
from gcp_variant_transforms.testing import vcf_header_util
from gcp_variant_transforms.testing.testdata_util import hash_name


def _get_processed_variant(variant, header_num_dict=None):
  header_fields = vcf_header_util.make_header(header_num_dict or {})
  return processed_variant.ProcessedVariantFactory(
      header_fields).create_processed_variant(variant)


def _get_table_schema(move_hom_ref_calls=False):
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
  if move_hom_ref_calls:
    hom_ref_call_record = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.HOM_REF_CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    hom_ref_call_record.fields.append(bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS_SAMPLE_ID,
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='Unique ID (type INT64) assigned to each sample. Table '
                    'with `__sample_info` suffix contains the mapping of '
                    'sample names (as read from VCF header) to these assigned '
                    'IDs.'))
    hom_ref_call_record.fields.append(bigquery.TableFieldSchema(
          name=ColumnKeyConstants.CALLS_NAME,
          type=TableFieldConstants.TYPE_STRING,
          mode=TableFieldConstants.MODE_NULLABLE,
          description='Name of the call (sample names in the VCF Header '
                      'line).'))
    schema.fields.append(hom_ref_call_record)
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
  # type: (...) -> Dict[unicode, Any]
  """Returns one sample BigQuery row for testing."""
  row = {str(ColumnKeyConstants.REFERENCE_NAME): str('chr19'),
         str(ColumnKeyConstants.START_POSITION): 11,
         str(ColumnKeyConstants.END_POSITION): 12,
         str(ColumnKeyConstants.REFERENCE_BASES): 'C',
         str(ColumnKeyConstants.NAMES): [str('rs1'), str('rs2')],
         str(ColumnKeyConstants.QUALITY): 2,
         str(ColumnKeyConstants.FILTER): [str('PASS')],
         str(ColumnKeyConstants.CALLS): [
             {str(ColumnKeyConstants.CALLS_SAMPLE_ID): (
                 str(hash_name('Sample1'))),
             str(ColumnKeyConstants.CALLS_GENOTYPE): [0, 1],
             str(ColumnKeyConstants.CALLS_PHASESET): str('*'),
             str('GQ'): 20, str('FIR'): [10, 20]},
             {str(ColumnKeyConstants.CALLS_SAMPLE_ID): (
                 str(hash_name('Sample2'))),
             str(ColumnKeyConstants.CALLS_GENOTYPE): [0, 0],
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

  return row


class VariantCallRowGeneratorTest(unittest.TestCase):
  """Test cases for class `VariantCallRowGenerator`."""

  def setUp(self):
    self._schema_descriptor = bigquery_schema_descriptor.SchemaDescriptor(
        _get_table_schema())
    self._conflict_resolver = (
        vcf_field_conflict_resolver.FieldConflictResolver())

    self._row_generator = bigquery_row_generator.VariantCallRowGenerator(
        self._schema_descriptor, self._conflict_resolver)

  def test_all_fields(self):
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
                sample_id=hash_name('Sample1'), name='Sample1', genotype=[0, 1],
                phaseset='*', info={'GQ': 20, 'FIR': [10, 20]}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample2'), name='Sample2', genotype=[0, 0],
                info={'GQ': 10, 'FB': True}),
            vcfio.VariantCall(sample_id=hash_name('Sample3'), name='Sample3',
                              genotype=[vcfio.MISSING_GENOTYPE_VALUE])])
    header_num_dict = {'IFR': 'A', 'IFR2': 'A', 'IS': '1', 'ISR': '2'}
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
            {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample1'),
             ColumnKeyConstants.CALLS_NAME: 'Sample1',
             ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
             ColumnKeyConstants.CALLS_PHASESET: '*',
             'GQ': 20, 'FIR': [10, 20]},
            {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample2'),
             ColumnKeyConstants.CALLS_NAME: 'Sample2',
             ColumnKeyConstants.CALLS_GENOTYPE: [0, 0],
             ColumnKeyConstants.CALLS_PHASESET: None,
             'GQ': 10, 'FB': True},
            {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample3'),
             ColumnKeyConstants.CALLS_NAME: 'Sample3',
             ColumnKeyConstants.CALLS_GENOTYPE: [vcfio.MISSING_GENOTYPE_VALUE],
             ColumnKeyConstants.CALLS_PHASESET: None}],
        'IS': 'some data',
        'ISR': ['data1', 'data2']}
    proc_variant = _get_processed_variant(variant, header_num_dict)
    row_generator = bigquery_row_generator.VariantCallRowGenerator(
        self._schema_descriptor, self._conflict_resolver,
        include_call_name=True)
    self.assertEqual(
        [expected_row], list(row_generator.get_rows(proc_variant)))

  def test_all_fields_with_hom_ref(self):
    schema_descriptor = bigquery_schema_descriptor.SchemaDescriptor(
        _get_table_schema(move_hom_ref_calls=True))
    conflict_resolver = (
        vcf_field_conflict_resolver.FieldConflictResolver())

    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IFR': [0.1, 0.2],
              'IFR2': [0.2, 0.3],
              'IS': 'some data',
              'ISR': ['data1', 'data2']},
        hom_ref_calls=[
          ('Sample2', hash_name('Sample2')),
          ('Sample3', hash_name('Sample3'))
        ],
        calls=[
            vcfio.VariantCall(
                sample_id=hash_name('Sample1'), name='Sample1', genotype=[0, 1],
                phaseset='*', info={'GQ': 20, 'FIR': [10, 20]})])
    header_num_dict = {'IFR': 'A', 'IFR2': 'A', 'IS': '1', 'ISR': '2'}
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
        ColumnKeyConstants.HOM_REF_CALLS: [
          {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample2'),
           ColumnKeyConstants.CALLS_NAME: 'Sample2'},
          {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample3'),
           ColumnKeyConstants.CALLS_NAME: 'Sample3'}
        ],
        ColumnKeyConstants.CALLS: [
            {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample1'),
             ColumnKeyConstants.CALLS_NAME: 'Sample1',
             ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
             ColumnKeyConstants.CALLS_PHASESET: '*',
             'GQ': 20, 'FIR': [10, 20]}],
        'IS': 'some data',
        'ISR': ['data1', 'data2']}
    proc_variant = _get_processed_variant(variant, header_num_dict)
    row_generator = bigquery_row_generator.VariantCallRowGenerator(
        schema_descriptor, conflict_resolver, include_call_name=True,
        move_hom_ref_calls=True)
    self.assertEqual(
        [expected_row], list(row_generator.get_rows(proc_variant)))

  def test_no_alternate_bases(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=['q10'],
        info={'IS': 'some data',
              'ISR': ['data1', 'data2']})
    header_num_dict = {'IS': '1', 'ISR': '2'}
    proc_variant = _get_processed_variant(variant, header_num_dict)
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

    self.assertEqual([expected_row],
                     list(self._row_generator.get_rows(proc_variant)))

  def test_some_fields_set(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=None, end=123, reference_bases=None,
        alternate_bases=[], quality=20)
    proc_variant = _get_processed_variant(variant)
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: None,
        ColumnKeyConstants.END_POSITION: 123,
        ColumnKeyConstants.REFERENCE_BASES: None,
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.QUALITY: 20,
        ColumnKeyConstants.CALLS: []}

    self.assertEqual([expected_row],
                     list(self._row_generator.get_rows(proc_variant)))

  def test_no_field_set(self):
    variant = vcfio.Variant()
    proc_variant = _get_processed_variant(variant)
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: None,
        ColumnKeyConstants.START_POSITION: None,
        ColumnKeyConstants.END_POSITION: None,
        ColumnKeyConstants.REFERENCE_BASES: None,
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: []}
    self.assertEqual([expected_row],
                     list(self._row_generator.get_rows(proc_variant)))

  def test_null_repeated_fields(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=['q10'],
        info={'IIR': [0, 1, None],
              'IBR': [True, None, False],
              'IFR': [0.1, 0.2, None, 0.4],
              'ISR': [None, 'data1', 'data2']})
    header_num_dict = {'IIR': '3', 'IBR': '3', 'IFR': '4', 'ISR': '3'}
    proc_variant = _get_processed_variant(variant, header_num_dict)
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.FILTER: ['q10'],
        ColumnKeyConstants.CALLS: [],
        'IIR': [0,
                1,
                bigquery_sanitizer._DEFAULT_NULL_NUMERIC_VALUE_REPLACEMENT],
        'IBR': [True, False, False],
        'IFR': [0.1,
                0.2,
                bigquery_sanitizer._DEFAULT_NULL_NUMERIC_VALUE_REPLACEMENT,
                0.4],
        'ISR': ['.', 'data1', 'data2']}
    self.assertEqual([expected_row],
                     list(self._row_generator.get_rows(proc_variant)))

  def test_unicode_fields(self):
    sample_unicode_str = u'\xc3\xb6'
    sample_utf8_str = sample_unicode_str.encode('utf-8')
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[sample_unicode_str, sample_utf8_str],
        info={'IS': sample_utf8_str,
              'ISR': [sample_unicode_str, sample_utf8_str]})
    header_num_dict = {'IS': '1', 'ISR': '2'}
    proc_variant = _get_processed_variant(variant, header_num_dict)
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
    self.assertEqual([expected_row],
                     list(self._row_generator.get_rows(proc_variant)))

  def test_nonstandard_float_values(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=['A', 'C', 'T', 'TC'], filters=[],
        calls=[vcfio.VariantCall(
            sample_id=hash_name('Sample1'), genotype=[0, 1],
            phaseset='*', info={'GQ': float('inf')})],
        info={'IF': float('inf'),
              'IFR': [float('-inf'), float('nan'), 1.2],
              'IF2': float('nan'),
              'IF3': [float('-inf'), float('nan'), float('inf'), 1.2]},
    )
    header_num_dict = {'IF': '1', 'IFR': '3', 'IF2': '1', 'IF3': 'A'}
    proc_variant = _get_processed_variant(variant, header_num_dict)
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [
            {'IF3': -bigquery_sanitizer._INF_FLOAT_VALUE, 'alt': 'A'},
            {'IF3': None, 'alt': 'C'},
            {'IF3': bigquery_sanitizer._INF_FLOAT_VALUE, 'alt': 'T'},
            {'IF3': 1.2, 'alt': 'TC'}
        ],
        ColumnKeyConstants.CALLS: [
            {
                ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample1'),
                ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
                ColumnKeyConstants.CALLS_PHASESET: '*',
                'GQ': bigquery_sanitizer._INF_FLOAT_VALUE
            }
        ],
        'IF': bigquery_sanitizer._INF_FLOAT_VALUE,
        'IFR': [-bigquery_sanitizer._INF_FLOAT_VALUE,
                bigquery_sanitizer._DEFAULT_NULL_NUMERIC_VALUE_REPLACEMENT,
                1.2],
        'IF2': None
    }
    self.assertEqual([expected_row],
                     list(self._row_generator.get_rows(proc_variant)))

  def test_nonstandard_fields_names(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[],
        info={'IS': 'data1',
              '_IS': 'data2'})
    header_num_dict = {'IS': '1', '_IS': '2'}
    proc_variant = _get_processed_variant(variant, header_num_dict)
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [],
        'IS': 'data1',
        'field__IS': 'data2'}
    self.assertEqual([expected_row],
                     list(self._row_generator.get_rows(proc_variant)))

  def test_sharded_rows(self):
    """Tests splitting BigQuery rows that are larger than BigQuery limit."""
    num_calls = 10  # Number of calls for a variant in this test.
    num_first_row_calls = 6  # BigQuery row limit is adjusted accordingly.
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IFR': [0.1, 0.2],
              'IFR2': [0.2, 0.3],
              'IS': 'some data'},
        calls=[
            vcfio.VariantCall(
                sample_id=hash_name('Sample{}'.format(i)), genotype=[0, 1],
                phaseset='*', info={'GQ': 20, 'FIR': [10, 20]})
            for i in range(num_calls)])
    header_num_dict = {'IFR': 'A', 'IFR2': 'A', 'IS': '1'}
    proc_variant = _get_processed_variant(variant, header_num_dict)

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
                {ColumnKeyConstants.CALLS_SAMPLE_ID: (
                    hash_name('Sample{}'.format(i))),
                 ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
                 ColumnKeyConstants.CALLS_PHASESET: '*',
                 'GQ': 20, 'FIR': [10, 20]}
                for i in range(num_first_row_calls)],
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
                {ColumnKeyConstants.CALLS_SAMPLE_ID: (
                    hash_name('Sample{}'.format(i))),
                 ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
                 ColumnKeyConstants.CALLS_PHASESET: '*',
                 'GQ': 20, 'FIR': [10, 20]}
                for i in range(num_first_row_calls, num_calls)],
            'IS': 'some data'
        },
    ]
    with mock.patch.object(bigquery_row_generator.VariantCallRowGenerator,
                           '_MAX_BIGQUERY_ROW_SIZE_BYTES',
                           num_first_row_calls * len(json.dumps(
                               expected_rows[0][ColumnKeyConstants.CALLS][0]))):
      with mock.patch.object(bigquery_row_generator.VariantCallRowGenerator,
                             '_MIN_NUM_CALLS_FOR_ROW_SIZE_ESTIMATION',
                             num_calls):
        self.assertEqual(expected_rows,
                         list(self._row_generator.get_rows(proc_variant)))

  def test_omit_empty_sample_calls(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=[], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={},
        calls=[
            vcfio.VariantCall(
                sample_id=hash_name('Sample1'), info={'GQ': None}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample2'), genotype=[1, 0],
                info={'GQ': 10}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample3'),
                genotype=[vcfio.MISSING_GENOTYPE_VALUE,
                          vcfio.MISSING_GENOTYPE_VALUE])])
    proc_variant = _get_processed_variant(variant)
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
            {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample2'),
             ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
             ColumnKeyConstants.CALLS_PHASESET: None,
             'GQ': 10}]}

    self.assertEqual(
        [expected_row],
        list(self._row_generator.get_rows(proc_variant,
                                          omit_empty_sample_calls=True)))

  def test_schema_conflict_in_info_field_type(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[],
        info={'IB': 1,
              'II': 1.1,
              'IFR': [1, 2],
              'ISR': [1.0, 2.0]})
    header_num_dict = {'IB': '1', 'II': '1', 'IFR': '2', 'ISR': '2'}
    proc_variant = _get_processed_variant(variant, header_num_dict)
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
    self.assertEqual([expected_row],
                     list(self._row_generator.get_rows(
                         proc_variant, allow_incompatible_records=True)))

    with self.assertRaises(ValueError):
      variant = vcfio.Variant(
          reference_name='chr19', start=11, end=12, reference_bases='CT',
          alternate_bases=[], filters=[],
          # String cannot be casted to integer.
          info={'II': '1.1'})
      header_num_dict = {'II': '1'}
      # self._get_row_list_from_variant(
      #     variant, header_num_dict, allow_incompatible_records=True)
      proc_variant = _get_processed_variant(variant, header_num_dict)
      list(self._row_generator.get_rows(proc_variant,
                                        allow_incompatible_records=True))
      self.fail('String data for an integer schema must cause an exception')

  def test_schema_conflict_in_info_field_number(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[],
        info={'IB': [1, 2],
              'IBR': 1,
              'II': [10, 20],
              'IFR': 1.1,
              'ISR': 'foo'},)
    header_num_dict = {'IB': '2', 'IBR': '1', 'II': '2', 'IFR': '1', 'ISR': '1'}
    proc_variant = _get_processed_variant(variant, header_num_dict)
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
        'ISR': ['foo']
    }

    self.assertEqual(
        [expected_row],
        list(self._row_generator.get_rows(proc_variant,
                                          allow_incompatible_records=True)))

  def test_schema_conflict_in_format_field_type(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[],
        calls=[
            vcfio.VariantCall(
                sample_id=hash_name('Sample1'), genotype=[0, 1], phaseset='*',
                info={'FB': '', 'FI': 1.0, 'FSR': [1, 2]}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample2'), genotype=[1, 0],
                info={'FB': 1, 'FI': True, 'FSR': [1.0, 2.0]})])
    proc_variant = _get_processed_variant(variant)
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [
            {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample1'),
             ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
             ColumnKeyConstants.CALLS_PHASESET: '*',
             'FB': False, 'FI': 1, 'FSR': ['1', '2']},
            {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample2'),
             ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
             ColumnKeyConstants.CALLS_PHASESET: None,
             'FB': True, 'FI': 1, 'FSR': ['1.0', '2.0']}],
    }

    self.assertEqual(
        [expected_row],
        list(self._row_generator.get_rows(proc_variant,
                                          allow_incompatible_records=True)))

    with self.assertRaises(ValueError):
      variant = vcfio.Variant(
          reference_name='chr19', start=11, end=12, reference_bases='CT',
          alternate_bases=[], filters=[],
          # String cannot be casted to integer.
          calls=[
              vcfio.VariantCall(
                  sample_id=hash_name('Sample1'), genotype=[0, 1], phaseset='*',
                  info={'FI': 'string_for_int_field'})])
      proc_variant = _get_processed_variant(variant)
      list(self._row_generator.get_rows(proc_variant,
                                        allow_incompatible_records=True))
      self.fail('String data for an integer schema must cause an exception')

  def test_schema_conflict_in_format_field_number(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[], filters=[],
        calls=[
            vcfio.VariantCall(
                sample_id=hash_name('Sample1'), genotype=[0, 1], phaseset='*',
                info={'FB': [1, 2], 'FI': [1, 2], 'FSR': 'str'}),
            vcfio.VariantCall(
                sample_id=hash_name('Sample2'), genotype=[1, 0],
                info={'FB': [], 'FI': [], 'FSR': ''})])
    proc_variant = _get_processed_variant(variant)
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: [],
        ColumnKeyConstants.CALLS: [
            {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample1'),
             ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
             ColumnKeyConstants.CALLS_PHASESET: '*',
             'FB': True, 'FI': 1, 'FSR': ['str']},
            {ColumnKeyConstants.CALLS_SAMPLE_ID: hash_name('Sample2'),
             ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
             ColumnKeyConstants.CALLS_PHASESET: None,
             'FB': False, 'FI': None, 'FSR': ['']}],
    }

    self.assertEqual(
        [expected_row],
        list(self._row_generator.get_rows(proc_variant,
                                          allow_incompatible_records=True)))
