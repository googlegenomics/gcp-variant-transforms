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

"""Tests for move_to_calls_strategy."""

import unittest

from apache_beam.io.gcp.internal.clients import bigquery

from beam_io.vcfio import Variant
from beam_io.vcfio import VariantCall
from beam_io.vcfio import VariantInfo
from libs.bigquery_vcf_schema import _TableFieldConstants as TableFieldConstants
from libs.bigquery_vcf_schema import ColumnKeyConstants
from libs.variant_merge.move_to_calls_strategy import MoveToCallsStrategy


class MoveToCallsStrategyTest(unittest.TestCase):

  def _get_sample_variants(self):
    variant_1 = Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'A1': VariantInfo('some data', '1'),
              'A2': VariantInfo(['data1', 'data2'], '2')},
        calls=[
            VariantCall(name='Sample1', genotype=[0, 1],
                        info={'GQ': 20, 'HQ': [10, 20]}),
            VariantCall(name='Sample2', genotype=[1, 0],
                        info={'GQ': 10, 'FLAG1': True})])
    variant_2 = Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs3'], quality=20,
        filters=['q10'],
        info={'A1': VariantInfo('some data2', '2'),
              'A3': VariantInfo(['data3', 'data4'], '2')},
        calls=[
            VariantCall(name='Sample3', genotype=[1, 1]),
            VariantCall(name='Sample4', genotype=[1, 0], info={'GQ': 20})])
    return [variant_1, variant_2]

  def _assert_common_expected_merged_fields(self, merged_variant):
    """Asserts expected common fields in the merged variant."""
    self.assertEqual('19', merged_variant.reference_name)
    self.assertEqual(11, merged_variant.start)
    self.assertEqual(12, merged_variant.end)
    self.assertEqual('C', merged_variant.reference_bases)
    self.assertEqual(['A', 'TT'], merged_variant.alternate_bases)
    self.assertEqual(['rs1', 'rs2', 'rs3'], merged_variant.names)
    self.assertEqual(20, merged_variant.quality)
    self.assertEqual(['PASS', 'q10'], merged_variant.filters)

  def test_get_merged_variants_no_custom_options(self):
    strategy = MoveToCallsStrategy(info_keys_to_move_to_calls_regex=None,
                                   copy_quality_to_calls=False,
                                   copy_filter_to_calls=False)
    variants = self._get_sample_variants()

    # Test single variant merge.
    self.assertEqual([variants[0]], strategy.get_merged_variants([variants[0]]))

    # Test multiple variant merge.
    merged_variant = strategy.get_merged_variants(variants)[0]
    self._assert_common_expected_merged_fields(merged_variant)
    self.assertEqual(
        [VariantCall(name='Sample1', genotype=[0, 1],
                     info={'GQ': 20, 'HQ': [10, 20]}),
         VariantCall(name='Sample2', genotype=[1, 0],
                     info={'GQ': 10, 'FLAG1': True}),
         VariantCall(name='Sample3', genotype=[1, 1]),
         VariantCall(name='Sample4', genotype=[1, 0], info={'GQ': 20})],
        merged_variant.calls)
    self.assertItemsEqual(['A1', 'A2', 'A3'], merged_variant.info.keys())
    self.assertTrue(
        merged_variant.info['A1'].data in ('some data', 'some data2'))
    self.assertEqual(VariantInfo(['data1', 'data2'], '2'),
                     merged_variant.info['A2'])
    self.assertEqual(VariantInfo(['data3', 'data4'], '2'),
                     merged_variant.info['A3'])

  def test_get_merged_variants_move_quality_and_filter_to_calls(self):
    strategy = MoveToCallsStrategy(info_keys_to_move_to_calls_regex='',
                                   copy_quality_to_calls=True,
                                   copy_filter_to_calls=True)
    variants = self._get_sample_variants()

    # Test single variant merge.
    single_merged_variant = strategy.get_merged_variants([variants[0]])[0]
    self.assertEqual(
        [VariantCall(name='Sample1', genotype=[0, 1],
                     info={'GQ': 20, 'HQ': [10, 20],
                           ColumnKeyConstants.QUALITY: 2,
                           ColumnKeyConstants.FILTER: ['PASS']}),
         VariantCall(name='Sample2', genotype=[1, 0],
                     info={'GQ': 10, 'FLAG1': True,
                           ColumnKeyConstants.QUALITY: 2,
                           ColumnKeyConstants.FILTER: ['PASS']})],
        single_merged_variant.calls)

    # Test multiple variant merge.
    merged_variant = strategy.get_merged_variants(variants)[0]
    self._assert_common_expected_merged_fields(merged_variant)
    self.assertEqual(
        [VariantCall(name='Sample1', genotype=[0, 1],
                     info={'GQ': 20, 'HQ': [10, 20],
                           ColumnKeyConstants.QUALITY: 2,
                           ColumnKeyConstants.FILTER: ['PASS']}),
         VariantCall(name='Sample2', genotype=[1, 0],
                     info={'GQ': 10, 'FLAG1': True,
                           ColumnKeyConstants.QUALITY: 2,
                           ColumnKeyConstants.FILTER: ['PASS']}),
         VariantCall(name='Sample3', genotype=[1, 1],
                     info={ColumnKeyConstants.QUALITY: 20,
                           ColumnKeyConstants.FILTER: ['q10']}),
         VariantCall(name='Sample4', genotype=[1, 0],
                     info={'GQ': 20,
                           ColumnKeyConstants.QUALITY: 20,
                           ColumnKeyConstants.FILTER: ['q10']})],
        merged_variant.calls)
    self.assertItemsEqual(['A1', 'A2', 'A3'], merged_variant.info.keys())
    self.assertTrue(
        merged_variant.info['A1'].data in ('some data', 'some data2'))
    self.assertEqual(VariantInfo(['data1', 'data2'], '2'),
                     merged_variant.info['A2'])
    self.assertEqual(VariantInfo(['data3', 'data4'], '2'),
                     merged_variant.info['A3'])

  def test_get_merged_variants_move_info_to_calls(self):
    strategy = MoveToCallsStrategy(info_keys_to_move_to_calls_regex='^A1$',
                                   copy_quality_to_calls=False,
                                   copy_filter_to_calls=False)
    variants = self._get_sample_variants()

    # Test single variant merge.
    single_merged_variant = strategy.get_merged_variants([variants[0]])[0]
    self.assertEqual(
        [VariantCall(name='Sample1', genotype=[0, 1],
                     info={'GQ': 20, 'HQ': [10, 20], 'A1': 'some data'}),
         VariantCall(name='Sample2', genotype=[1, 0],
                     info={'GQ': 10, 'FLAG1': True, 'A1': 'some data'})],
        single_merged_variant.calls)

    # Test multiple variant merge.
    merged_variant = strategy.get_merged_variants(variants)[0]
    self._assert_common_expected_merged_fields(merged_variant)
    self.assertEqual(
        [VariantCall(name='Sample1', genotype=[0, 1],
                     info={'GQ': 20, 'HQ': [10, 20], 'A1': 'some data'}),
         VariantCall(name='Sample2', genotype=[1, 0],
                     info={'GQ': 10, 'FLAG1': True, 'A1': 'some data'}),
         VariantCall(name='Sample3', genotype=[1, 1],
                     info={'A1': 'some data2'}),
         VariantCall(name='Sample4', genotype=[1, 0],
                     info={'GQ': 20, 'A1': 'some data2'})],
        merged_variant.calls)
    self.assertItemsEqual(['A2', 'A3'], merged_variant.info.keys())
    self.assertEqual(VariantInfo(['data1', 'data2'], '2'),
                     merged_variant.info['A2'])
    self.assertEqual(VariantInfo(['data3', 'data4'], '2'),
                     merged_variant.info['A3'])

  def test_get_merged_variants_move_everything_to_calls(self):
    strategy = MoveToCallsStrategy(info_keys_to_move_to_calls_regex='.*',
                                   copy_quality_to_calls=True,
                                   copy_filter_to_calls=True)
    variants = self._get_sample_variants()

    # Test single variant merge.
    single_merged_variant = strategy.get_merged_variants([variants[0]])[0]
    self.assertEqual(
        [VariantCall(name='Sample1', genotype=[0, 1],
                     info={'GQ': 20, 'HQ': [10, 20],
                           'A1': 'some data', 'A2': ['data1', 'data2'],
                           ColumnKeyConstants.QUALITY: 2,
                           ColumnKeyConstants.FILTER: ['PASS']}),
         VariantCall(name='Sample2', genotype=[1, 0],
                     info={'GQ': 10, 'FLAG1': True,
                           'A1': 'some data', 'A2': ['data1', 'data2'],
                           ColumnKeyConstants.QUALITY: 2,
                           ColumnKeyConstants.FILTER: ['PASS']})],
        single_merged_variant.calls)

    merged_variant = strategy.get_merged_variants(variants)[0]
    self._assert_common_expected_merged_fields(merged_variant)
    self.assertEqual(
        [VariantCall(name='Sample1', genotype=[0, 1],
                     info={'GQ': 20, 'HQ': [10, 20],
                           'A1': 'some data', 'A2': ['data1', 'data2'],
                           ColumnKeyConstants.QUALITY: 2,
                           ColumnKeyConstants.FILTER: ['PASS']}),
         VariantCall(name='Sample2', genotype=[1, 0],
                     info={'GQ': 10, 'FLAG1': True,
                           'A1': 'some data', 'A2': ['data1', 'data2'],
                           ColumnKeyConstants.QUALITY: 2,
                           ColumnKeyConstants.FILTER: ['PASS']}),
         VariantCall(name='Sample3', genotype=[1, 1],
                     info={'A1': 'some data2', 'A3': ['data3', 'data4'],
                           ColumnKeyConstants.QUALITY: 20,
                           ColumnKeyConstants.FILTER: ['q10']}),
         VariantCall(name='Sample4', genotype=[1, 0],
                     info={'GQ': 20,
                           'A1': 'some data2', 'A3': ['data3', 'data4'],
                           ColumnKeyConstants.QUALITY: 20,
                           ColumnKeyConstants.FILTER: ['q10']})],
        merged_variant.calls)
    self.assertEqual([], merged_variant.info.keys())

  def test_get_merge_key(self):
    strategy = MoveToCallsStrategy(None, None, None)

    variant = Variant()
    self.assertEqual('::::', strategy.get_merge_key(variant))

    variant.reference_name = '19'
    self.assertEqual('19::::', strategy.get_merge_key(variant))

    variant.start = 123
    variant.end = 125
    variant.reference_bases = 'AT'
    self.assertEqual('19:123:125:AT:', strategy.get_merge_key(variant))

    variant.alternate_bases = ['A', 'C']
    self.assertEqual('19:123:125:AT:A,C', strategy.get_merge_key(variant))

  def _get_base_schema(self, info_keys):
    schema = bigquery.TableSchema()
    schema.fields.append(bigquery.TableFieldSchema(
        name=ColumnKeyConstants.REFERENCE_NAME,
        type=TableFieldConstants.TYPE_STRING))
    schema.fields.append(bigquery.TableFieldSchema(
        name=ColumnKeyConstants.QUALITY,
        type=TableFieldConstants.TYPE_FLOAT))
    schema.fields.append(bigquery.TableFieldSchema(
        name=ColumnKeyConstants.FILTER,
        type=TableFieldConstants.TYPE_STRING,
        mode=TableFieldConstants.MODE_REPEATED))
    calls_record = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED)
    calls_record.fields.append(bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS_NAME,
        type=TableFieldConstants.TYPE_STRING))
    schema.fields.append(calls_record)
    for key in info_keys:
      schema.fields.append(bigquery.TableFieldSchema(
          name=key,
          type=TableFieldConstants.TYPE_STRING))
    return schema

  def _get_fields_from_schema(self, schema, prefix=''):
    fields = []
    for field in schema.fields:
      fields.append(prefix + field.name)
      if field.type == TableFieldConstants.TYPE_RECORD:
        fields.extend(self._get_fields_from_schema(field,
                                                   prefix=field.name + '.'))
    return fields

  def test_modify_bigquery_schema_no_custom_options(self):
    strategy = MoveToCallsStrategy(info_keys_to_move_to_calls_regex=None,
                                   copy_quality_to_calls=False,
                                   copy_filter_to_calls=False)
    info_keys = ['INFO_KEY1']
    base_schema = self._get_base_schema(info_keys)
    strategy.modify_bigquery_schema(base_schema, info_keys)
    self.assertEqual(
        [ColumnKeyConstants.REFERENCE_NAME,
         ColumnKeyConstants.QUALITY,
         ColumnKeyConstants.FILTER,
         ColumnKeyConstants.CALLS,
         '.'.join([ColumnKeyConstants.CALLS, ColumnKeyConstants.CALLS_NAME]),
         'INFO_KEY1'],
        self._get_fields_from_schema(base_schema))

  def test_modify_bigquery_schema_move_quality_and_filter_to_calls(self):
    strategy = MoveToCallsStrategy(info_keys_to_move_to_calls_regex=None,
                                   copy_quality_to_calls=True,
                                   copy_filter_to_calls=True)
    info_keys = ['INFO_KEY1']
    base_schema = self._get_base_schema(info_keys)
    strategy.modify_bigquery_schema(base_schema, info_keys)
    self.assertEqual(
        [ColumnKeyConstants.REFERENCE_NAME,
         ColumnKeyConstants.QUALITY,
         ColumnKeyConstants.FILTER,
         ColumnKeyConstants.CALLS,
         '.'.join([ColumnKeyConstants.CALLS, ColumnKeyConstants.CALLS_NAME]),
         '.'.join([ColumnKeyConstants.CALLS, ColumnKeyConstants.QUALITY]),
         '.'.join([ColumnKeyConstants.CALLS, ColumnKeyConstants.FILTER]),
         'INFO_KEY1'],
        self._get_fields_from_schema(base_schema))

  def test_modify_bigquery_schema_move_info_to_calls(self):
    strategy = MoveToCallsStrategy(info_keys_to_move_to_calls_regex='INFO.*1',
                                   copy_quality_to_calls=False,
                                   copy_filter_to_calls=False)
    info_keys = ['INFO_KEY1', 'INFO_KEY2']
    base_schema = self._get_base_schema(info_keys)
    strategy.modify_bigquery_schema(base_schema, info_keys)
    self.assertEqual(
        [ColumnKeyConstants.REFERENCE_NAME,
         ColumnKeyConstants.QUALITY,
         ColumnKeyConstants.FILTER,
         ColumnKeyConstants.CALLS,
         '.'.join([ColumnKeyConstants.CALLS, ColumnKeyConstants.CALLS_NAME]),
         '.'.join([ColumnKeyConstants.CALLS, 'INFO_KEY1']),
         'INFO_KEY2'],
        self._get_fields_from_schema(base_schema))

  def test_modify_bigquery_schema_move_everything_to_calls(self):
    strategy = MoveToCallsStrategy(info_keys_to_move_to_calls_regex='.*',
                                   copy_quality_to_calls=True,
                                   copy_filter_to_calls=True)
    info_keys = ['INFO_KEY1', 'INFO_KEY2']
    base_schema = self._get_base_schema(info_keys)
    strategy.modify_bigquery_schema(base_schema, info_keys)
    self.assertEqual(
        [ColumnKeyConstants.REFERENCE_NAME,
         ColumnKeyConstants.QUALITY,
         ColumnKeyConstants.FILTER,
         ColumnKeyConstants.CALLS,
         '.'.join([ColumnKeyConstants.CALLS, ColumnKeyConstants.CALLS_NAME]),
         '.'.join([ColumnKeyConstants.CALLS, ColumnKeyConstants.QUALITY]),
         '.'.join([ColumnKeyConstants.CALLS, ColumnKeyConstants.FILTER]),
         '.'.join([ColumnKeyConstants.CALLS, 'INFO_KEY1']),
         '.'.join([ColumnKeyConstants.CALLS, 'INFO_KEY2'])],
        self._get_fields_from_schema(base_schema))

  def test_modify_bigquery_schema_duplicate_keys(self):
    strategy = MoveToCallsStrategy(info_keys_to_move_to_calls_regex='.*',
                                   copy_quality_to_calls=True,
                                   copy_filter_to_calls=True)
    info_keys = [ColumnKeyConstants.CALLS_NAME]
    base_schema = self._get_base_schema(info_keys)
    try:
      strategy.modify_bigquery_schema(base_schema, info_keys)
      self.fail('Duplicate keys should throw error.')
    except ValueError:
      pass
