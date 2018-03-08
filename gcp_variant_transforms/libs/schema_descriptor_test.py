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

"""Tests for infer_variant_header module."""


from __future__ import absolute_import


import unittest


from apache_beam.io.gcp.internal.clients import bigquery
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import schema_descriptor


class SchemaDescriptorTest(unittest.TestCase):
  """Test case for :class:`SchemaDescriptor`"""

  def setUp(self):
    self._boolean = bigquery_util.TableFieldConstants.TYPE_BOOLEAN
    self._float = bigquery_util.TableFieldConstants.TYPE_FLOAT
    self._integer = bigquery_util.TableFieldConstants.TYPE_INTEGER
    self._record = bigquery_util.TableFieldConstants.TYPE_RECORD
    self._string = bigquery_util.TableFieldConstants.TYPE_STRING

    self._nullable = bigquery_util.TableFieldConstants.MODE_NULLABLE
    self._repeated = bigquery_util.TableFieldConstants.MODE_REPEATED

  def _get_table_schema(self):
    # type (None) -> bigquery.TableSchema
    schema = bigquery.TableSchema()
    schema.fields.append(bigquery.TableFieldSchema(
        name='field_1', type=self._string, mode=self._nullable,
        description='foo desc'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='field_2', type=self._integer, mode=self._repeated,
        description='foo desc'))
    # Record field.
    record_field = bigquery.TableFieldSchema(
        name='record_1', type=self._record, mode=self._repeated,
        description='foo desc')
    record_field.fields.append(bigquery.TableFieldSchema(
        name='record_1-field_1', type=self._boolean, mode=self._nullable,
        description='foo desc'))
    record_field.fields.append(bigquery.TableFieldSchema(
        name='record_1-field_2', type=self._float, mode=self._repeated,
        description='foo desc'))
    # Record field, two level deep.
    deep_record_field = bigquery.TableFieldSchema(
        name='record_1-record_2', type=self._record, mode=self._repeated,
        description='foo desc')
    deep_record_field.fields.append(bigquery.TableFieldSchema(
        name='record_1-record_2-field_1', type=self._boolean,
        mode=self._nullable, description='foo desc'))

    record_field.fields.append(deep_record_field)
    schema.fields.append(record_field)
    return schema

  def _get_schema_descriptor(self):
    return schema_descriptor.SchemaDescriptor(self._get_table_schema())

  def test_non_existence_field(self):
    schema = self._get_schema_descriptor()
    with self.assertRaises(ValueError):
      schema.get_field_descriptor('non_existence_field')
      self.fail('Non existence field should throw an exceprion')

  def test_non_existence_record(self):
    schema = self._get_schema_descriptor()
    with self.assertRaises(ValueError):
      schema.get_record_schema_descriptor('non_existence_record')
      self.fail('Non existence field should throw an exceprion')


  def test_field_descriptor_at_first_level(self):
    print self._get_table_schema()
    schema = self._get_schema_descriptor()

    self.assertEqual(
        schema.get_field_descriptor('field_1'),
        schema_descriptor.FieldDescriptor(
            type=self._string, mode=self._nullable))
    self.assertEqual(
        schema.get_field_descriptor('field_2'),
        schema_descriptor.FieldDescriptor(
            type=self._integer, mode=self._repeated))

  def test_field_descriptor_at_second_level(self):
    main_schema = self._get_schema_descriptor()
    record_schema = main_schema.get_record_schema_descriptor('record_1')

    self.assertEqual(
        record_schema.get_field_descriptor('record_1-field_1'),
        schema_descriptor.FieldDescriptor(
            type=self._boolean, mode=self._nullable))
    self.assertEqual(
        record_schema.get_field_descriptor('record_1-field_2'),
        schema_descriptor.FieldDescriptor(
            type=self._float, mode=self._repeated))

  def test_field_descriptor_at_third_level(self):
    main_schema = self._get_schema_descriptor()
    parent_record_schema = main_schema.get_record_schema_descriptor(
        'record_1')
    child_record_schema = parent_record_schema.get_record_schema_descriptor(
        'record_1-record_2')

    self.assertEqual(
        child_record_schema.get_field_descriptor(
            'record_1-record_2-field_1'),
        schema_descriptor.FieldDescriptor(
            type=self._boolean, mode=self._nullable))
