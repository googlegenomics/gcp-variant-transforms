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

from __future__ import absolute_import

import unittest
from apitools.base.py import exceptions

import mock
from apache_beam.io.gcp.internal.clients import bigquery

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs.bigquery_util import ColumnKeyConstants
from gcp_variant_transforms.libs.bigquery_util import TableFieldConstants


class BigqueryUtilTest(unittest.TestCase):

  def test_get_bigquery_type_from_vcf_type(self):
    self.assertEqual(bigquery_util.TableFieldConstants.TYPE_INTEGER,
                     bigquery_util.get_bigquery_type_from_vcf_type('integer'))
    self.assertEqual(bigquery_util.TableFieldConstants.TYPE_STRING,
                     bigquery_util.get_bigquery_type_from_vcf_type('string'))
    self.assertEqual(bigquery_util.TableFieldConstants.TYPE_STRING,
                     bigquery_util.get_bigquery_type_from_vcf_type('character'))
    self.assertEqual(bigquery_util.TableFieldConstants.TYPE_FLOAT,
                     bigquery_util.get_bigquery_type_from_vcf_type('float'))
    self.assertEqual(bigquery_util.TableFieldConstants.TYPE_BOOLEAN,
                     bigquery_util.get_bigquery_type_from_vcf_type('flag'))
    self.assertRaises(
        ValueError,
        bigquery_util.get_bigquery_type_from_vcf_type, 'DUMMY')

  def test_get_python_from_bigquery_type(self):
    self.assertEqual(int, bigquery_util.get_python_type_from_bigquery_type(
        bigquery_util.TableFieldConstants.TYPE_INTEGER))
    self.assertEqual(float, bigquery_util.get_python_type_from_bigquery_type(
        bigquery_util.TableFieldConstants.TYPE_FLOAT))
    self.assertEqual(unicode,
                     bigquery_util.get_python_type_from_bigquery_type(
                         bigquery_util.TableFieldConstants.TYPE_STRING))
    self.assertEqual(bool, bigquery_util.get_python_type_from_bigquery_type(
        bigquery_util.TableFieldConstants.TYPE_BOOLEAN))
    self.assertRaises(
        ValueError,
        bigquery_util.get_python_type_from_bigquery_type, 'DUMMY')

  def test_get_vcf_type_from_bigquery_type(self):
    self.assertEqual(vcf_header_io.VcfHeaderFieldTypeConstants.INTEGER,
                     bigquery_util.get_vcf_type_from_bigquery_type(
                         bigquery_util.TableFieldConstants.TYPE_INTEGER))
    self.assertEqual(vcf_header_io.VcfHeaderFieldTypeConstants.FLOAT,
                     bigquery_util.get_vcf_type_from_bigquery_type(
                         bigquery_util.TableFieldConstants.TYPE_FLOAT))
    self.assertEqual(vcf_header_io.VcfHeaderFieldTypeConstants.FLAG,
                     bigquery_util.get_vcf_type_from_bigquery_type(
                         bigquery_util.TableFieldConstants.TYPE_BOOLEAN))
    self.assertEqual(vcf_header_io.VcfHeaderFieldTypeConstants.STRING,
                     bigquery_util.get_vcf_type_from_bigquery_type(
                         bigquery_util.TableFieldConstants.TYPE_STRING))
    self.assertRaises(
        ValueError,
        bigquery_util.get_vcf_type_from_bigquery_type, 'DUMMY')

  def test_get_vcf_num_from_bigquery_schema(self):
    self.assertEqual('.',
                     bigquery_util.get_vcf_num_from_bigquery_schema(
                         bigquery_util.TableFieldConstants.MODE_REPEATED,
                         bigquery_util.TableFieldConstants.TYPE_INTEGER))

    self.assertEqual(1,
                     bigquery_util.get_vcf_num_from_bigquery_schema(
                         bigquery_util.TableFieldConstants.MODE_NULLABLE,
                         bigquery_util.TableFieldConstants.TYPE_INTEGER))
    self.assertEqual(0,
                     bigquery_util.get_vcf_num_from_bigquery_schema(
                         bigquery_util.TableFieldConstants.MODE_NULLABLE,
                         bigquery_util.TableFieldConstants.TYPE_BOOLEAN))
    self.assertEqual(
        0,
        bigquery_util.get_vcf_num_from_bigquery_schema(
            bigquery_mode=None,
            bigquery_type=bigquery_util.TableFieldConstants.TYPE_BOOLEAN))


  def test_merge_field_schemas_no_same_id(self):
    field_schemas_1 = [
        bigquery.TableFieldSchema(
            name='II',
            type=TableFieldConstants.TYPE_INTEGER,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc'),
        bigquery.TableFieldSchema(
            name='IFR',
            type=TableFieldConstants.TYPE_FLOAT,
            mode=TableFieldConstants.MODE_REPEATED,
            description='INFO foo desc')
    ]
    field_schemas_2 = [
        bigquery.TableFieldSchema(
            name='AB',
            type=TableFieldConstants.TYPE_FLOAT,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc')
    ]
    merged_field_schemas = bigquery_util._get_merged_field_schemas(
        field_schemas_1, field_schemas_2)
    expected_merged_field_schemas = [
        bigquery.TableFieldSchema(
            name='II',
            type=TableFieldConstants.TYPE_INTEGER,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc'),
        bigquery.TableFieldSchema(
            name='IFR',
            type=TableFieldConstants.TYPE_FLOAT,
            mode=TableFieldConstants.MODE_REPEATED,
            description='INFO foo desc'),
        bigquery.TableFieldSchema(
            name='AB',
            type=TableFieldConstants.TYPE_FLOAT,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc')
    ]
    self.assertEqual(merged_field_schemas, expected_merged_field_schemas)

  def test_merge_field_schemas_same_id_no_conflicts(self):
    field_schemas_1 = [
        bigquery.TableFieldSchema(
            name='II',
            type=TableFieldConstants.TYPE_INTEGER,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc'),
        bigquery.TableFieldSchema(
            name='IFR',
            type=TableFieldConstants.TYPE_FLOAT,
            mode=TableFieldConstants.MODE_REPEATED,
            description='INFO foo desc')
    ]
    field_schemas_2 = [
        bigquery.TableFieldSchema(
            name='II',
            type=TableFieldConstants.TYPE_INTEGER,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc'),
        bigquery.TableFieldSchema(
            name='AB',
            type=TableFieldConstants.TYPE_FLOAT,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc')
    ]
    merged_field_schemas = bigquery_util._get_merged_field_schemas(
        field_schemas_1, field_schemas_2)
    expected_merged_field_schemas = [
        bigquery.TableFieldSchema(
            name='II',
            type=TableFieldConstants.TYPE_INTEGER,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc'),
        bigquery.TableFieldSchema(
            name='IFR',
            type=TableFieldConstants.TYPE_FLOAT,
            mode=TableFieldConstants.MODE_REPEATED,
            description='INFO foo desc'),
        bigquery.TableFieldSchema(
            name='AB',
            type=TableFieldConstants.TYPE_FLOAT,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc')
    ]
    self.assertEqual(merged_field_schemas, expected_merged_field_schemas)

  def test_merge_field_schemas_conflict_mode(self):
    field_schemas_1 = [
        bigquery.TableFieldSchema(
            name='II',
            type=TableFieldConstants.TYPE_INTEGER,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc')
    ]
    field_schemas_2 = [
        bigquery.TableFieldSchema(
            name='II',
            type=TableFieldConstants.TYPE_INTEGER,
            mode=TableFieldConstants.MODE_REPEATED,
            description='INFO foo desc')
    ]
    self.assertRaises(ValueError, bigquery_util._get_merged_field_schemas,
                      field_schemas_1, field_schemas_2)

  def test_merge_field_schemas_conflict_type(self):
    field_schemas_1 = [
        bigquery.TableFieldSchema(
            name='II',
            type=TableFieldConstants.TYPE_INTEGER,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc')
    ]
    field_schemas_2 = [
        bigquery.TableFieldSchema(
            name='II',
            type=TableFieldConstants.TYPE_FLOAT,
            mode=TableFieldConstants.MODE_NULLABLE,
            description='INFO foo desc')
    ]
    self.assertRaises(ValueError, bigquery_util._get_merged_field_schemas,
                      field_schemas_1, field_schemas_2)

  def test_merge_field_schemas_conflict_record_fields(self):
    call_record_1 = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    call_record_1.fields.append(bigquery.TableFieldSchema(
        name='FB',
        type=TableFieldConstants.TYPE_BOOLEAN,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='FORMAT foo desc'))
    field_schemas_1 = [call_record_1]

    call_record_2 = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    call_record_2.fields.append(bigquery.TableFieldSchema(
        name='FB',
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='FORMAT foo desc'))
    field_schemas_2 = [call_record_2]
    self.assertRaises(ValueError, bigquery_util._get_merged_field_schemas,
                      field_schemas_1, field_schemas_2)

  def test_merge_field_schemas_same_record(self):
    call_record_1 = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    call_record_1.fields.append(bigquery.TableFieldSchema(
        name='FB',
        type=TableFieldConstants.TYPE_BOOLEAN,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='FORMAT foo desc'))

    field_schemas_1 = [call_record_1]
    field_schemas_2 = [call_record_1]

    expected_merged_field_schemas = [call_record_1]
    self.assertEqual(
        bigquery_util._get_merged_field_schemas(field_schemas_1,
                                                field_schemas_2),
        expected_merged_field_schemas)

  def test_merge_field_schemas_merge_record_fields(self):
    call_record_1 = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    call_record_1.fields.append(bigquery.TableFieldSchema(
        name='FB',
        type=TableFieldConstants.TYPE_BOOLEAN,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='FORMAT foo desc'))

    field_schemas_1 = [call_record_1]

    call_record_2 = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    call_record_2.fields.append(bigquery.TableFieldSchema(
        name='GQ',
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='FORMAT foo desc'))
    field_schemas_2 = [call_record_2]

    call_record_3 = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    call_record_3.fields.append(bigquery.TableFieldSchema(
        name='FB',
        type=TableFieldConstants.TYPE_BOOLEAN,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='FORMAT foo desc'))
    call_record_3.fields.append(bigquery.TableFieldSchema(
        name='GQ',
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_NULLABLE,
        description='FORMAT foo desc'))

    expected_merged_field_schemas = [call_record_3]
    self.assertEqual(
        bigquery_util._get_merged_field_schemas(field_schemas_1,
                                                field_schemas_2),
        expected_merged_field_schemas)

  def test_merge_field_schemas_conflict_inner_record_fields(self):
    record_1 = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    inner_record_1 = bigquery.TableFieldSchema(
        name='inner record',
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    inner_record_1.fields.append(bigquery.TableFieldSchema(
        name='FB',
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='FORMAT foo desc'))
    record_1.fields.append(inner_record_1)
    field_schemas_1 = [record_1]

    record_2 = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    inner_record_2 = bigquery.TableFieldSchema(
        name='inner record',
        type=TableFieldConstants.TYPE_INTEGER,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    inner_record_2.fields.append(bigquery.TableFieldSchema(
        name='FB',
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='FORMAT foo desc'))
    record_2.fields.append(inner_record_2)
    field_schemas_2 = [record_2]
    self.assertRaises(ValueError, bigquery_util._get_merged_field_schemas,
                      field_schemas_1, field_schemas_2)

  def test_merge_field_schemas_merge_inner_record_fields(self):
    record_1 = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    inner_record_1 = bigquery.TableFieldSchema(
        name='inner record',
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    inner_record_1.fields.append(bigquery.TableFieldSchema(
        name='FB',
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='FORMAT foo desc'))
    record_1.fields.append(inner_record_1)
    field_schemas_1 = [record_1]

    record_2 = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    inner_record_2 = bigquery.TableFieldSchema(
        name='inner record',
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    inner_record_2.fields.append(bigquery.TableFieldSchema(
        name='AB',
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='FORMAT foo desc'))
    record_2.fields.append(inner_record_2)
    field_schemas_2 = [record_2]

    merged_record = bigquery.TableFieldSchema(
        name=ColumnKeyConstants.CALLS,
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    merged_inner_record = bigquery.TableFieldSchema(
        name='inner record',
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='One record for each call.')
    merged_inner_record.fields.append(bigquery.TableFieldSchema(
        name='FB',
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='FORMAT foo desc'))
    merged_inner_record.fields.append(bigquery.TableFieldSchema(
        name='AB',
        type=TableFieldConstants.TYPE_RECORD,
        mode=TableFieldConstants.MODE_REPEATED,
        description='FORMAT foo desc'))
    merged_record.fields.append(merged_inner_record)
    expected_merged_field_schemas = [merged_record]
    self.assertEqual(
        bigquery_util._get_merged_field_schemas(field_schemas_1,
                                                field_schemas_2),
        expected_merged_field_schemas)

  def test_raise_error_if_table_exists(self):
    client = mock.Mock()
    client.tables.Get.return_value = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId='project', datasetId='dataset', tableId='table'))
    self.assertRaises(ValueError,
                      bigquery_util.raise_error_if_table_exists,
                      client, 'project', 'dataset', 'table')

    client.tables.Get.side_effect = exceptions.HttpError(
        response={'status': '404'}, url='', content='')
    bigquery_util.raise_error_if_table_exists(client,
                                              'project',
                                              'dataset',
                                              'table')

    client.tables.Get.side_effect = exceptions.HttpError(
        response={'status': '401'}, url='', content='')
    self.assertRaises(exceptions.HttpError,
                      bigquery_util.raise_error_if_table_exists,
                      client, 'project', 'dataset', 'table')

  def test_raise_error_if_dataset_not_exists(self):
    client = mock.Mock()
    client.datasets.Get.return_value = bigquery.Dataset(
        datasetReference=bigquery.DatasetReference(
            projectId='project', datasetId='dataset'))
    bigquery_util.raise_error_if_dataset_not_exists(client,
                                                    'project',
                                                    'dataset')

    client.datasets.Get.side_effect = exceptions.HttpError(
        response={'status': '404'}, url='', content='')
    self.assertRaises(ValueError,
                      bigquery_util.raise_error_if_dataset_not_exists,
                      client, 'project', 'dataset')

    client.datasets.Get.side_effect = exceptions.HttpError(
        response={'status': '401'}, url='', content='')
    self.assertRaises(exceptions.HttpError,
                      bigquery_util.raise_error_if_dataset_not_exists,
                      client, 'project', 'dataset')
