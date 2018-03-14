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

"""Tests for bigquery_schema_descriptor module."""


from __future__ import absolute_import

import unittest

from apache_beam.io.gcp.internal.clients import bigquery
from gcp_variant_transforms.libs.bigquery_util import TableFieldConstants as Consts
from gcp_variant_transforms.libs import bigquery_schema_descriptor


class SchemaDescriptorTest(unittest.TestCase):
  """Test case for :class:`SchemaDescriptor`."""

  def _get_table_schema(self):
    # type (None) -> bigquery.TableSchema
    schema = bigquery.TableSchema()
    schema.fields.append(bigquery.TableFieldSchema(
        name='field_1', type=Consts.TYPE_STRING, mode=Consts.MODE_NULLABLE))
    schema.fields.append(bigquery.TableFieldSchema(
        name='field_2', type=Consts.TYPE_INTEGER, mode=Consts.MODE_REPEATED))
    # Record field.
    record_field = bigquery.TableFieldSchema(
        name='record_1', type=Consts.TYPE_RECORD, mode=Consts.MODE_REPEATED,
        )
    record_field.fields.append(bigquery.TableFieldSchema(
        name='record_1_field_1', type=Consts.TYPE_BOOLEAN,
        mode=Consts.MODE_NULLABLE, ))
    record_field.fields.append(bigquery.TableFieldSchema(
        name='record_1_field_2', type=Consts.TYPE_FLOAT,
        mode=Consts.MODE_REPEATED))
    # Record field, two level deep.
    deep_record_field = bigquery.TableFieldSchema(
        name='record_1-record_2', type=Consts.TYPE_RECORD,
        mode=Consts.MODE_REPEATED)
    deep_record_field.fields.append(bigquery.TableFieldSchema(
        name='record_1-record_2_field_1', type=Consts.TYPE_BOOLEAN,
        mode=Consts.MODE_NULLABLE))

    record_field.fields.append(deep_record_field)
    schema.fields.append(record_field)
    return schema

  def _get_schema_descriptor(self):
    return bigquery_schema_descriptor.SchemaDescriptor(
        self._get_table_schema())

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
    schema = self._get_schema_descriptor()

    self.assertEqual(
        schema.get_field_descriptor('field_1'),
        bigquery_schema_descriptor.FieldDescriptor(
            type=Consts.TYPE_STRING, mode=Consts.MODE_NULLABLE))
    self.assertEqual(
        schema.get_field_descriptor('field_2'),
        bigquery_schema_descriptor.FieldDescriptor(
            type=Consts.TYPE_INTEGER, mode=Consts.MODE_REPEATED))

  def test_field_descriptor_at_second_level(self):
    main_schema = self._get_schema_descriptor()
    record_schema = main_schema.get_record_schema_descriptor('record_1')

    self.assertEqual(
        record_schema.get_field_descriptor('record_1_field_1'),
        bigquery_schema_descriptor.FieldDescriptor(
            type=Consts.TYPE_BOOLEAN, mode=Consts.MODE_NULLABLE))
    self.assertEqual(
        record_schema.get_field_descriptor('record_1_field_2'),
        bigquery_schema_descriptor.FieldDescriptor(
            type=Consts.TYPE_FLOAT, mode=Consts.MODE_REPEATED))

  def test_field_descriptor_at_third_level(self):
    main_schema = self._get_schema_descriptor()
    parent_record_schema = main_schema.get_record_schema_descriptor(
        'record_1')
    child_record_schema = parent_record_schema.get_record_schema_descriptor(
        'record_1-record_2')

    self.assertEqual(
        child_record_schema.get_field_descriptor(
            'record_1-record_2_field_1'),
        bigquery_schema_descriptor.FieldDescriptor(
            type=Consts.TYPE_BOOLEAN, mode=Consts.MODE_NULLABLE))
