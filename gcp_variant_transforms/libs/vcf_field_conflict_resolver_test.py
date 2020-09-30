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

"""Test cases for get_merged_headers module."""

import unittest

from collections import namedtuple

from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeaderFieldTypeConstants
from gcp_variant_transforms.beam_io.vcf_header_io import VcfParserHeaderKeyConstants
from gcp_variant_transforms.libs import bigquery_schema_descriptor
from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs.bigquery_util import TableFieldConstants


SchemaTestConfig = namedtuple('SchemaTestConfig',
                              ['schema_type', 'schema_mode', 'field_data',
                               'expected_resolved_field_data'])


class ConflictResolverTest(unittest.TestCase):
  """Test case for :class:`FieldConflictResolver`."""

  def setUp(self):
    self._resolver = vcf_field_conflict_resolver.FieldConflictResolver()
    self._resolver_allele = vcf_field_conflict_resolver.FieldConflictResolver(
        split_alternate_allele_info_fields=True)
    self._resolver_always = vcf_field_conflict_resolver.FieldConflictResolver(
        resolve_always=True)

  def _run_resolve_schema_conflict_tests(self, test_data_configs):
    for config in test_data_configs:
      self.assertEqual(
          self._resolver.resolve_schema_conflict(
              bigquery_schema_descriptor.FieldDescriptor(
                  type=config.schema_type,
                  mode=config.schema_mode),
              config.field_data),
          config.expected_resolved_field_data)


  def test_resolving_schema_conflict_type(self):
    test_data_configs = [
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_BOOLEAN,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data=1,
                         expected_resolved_field_data=True),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_BOOLEAN,
                         schema_mode=TableFieldConstants.MODE_REPEATED,
                         field_data=['1', '2'],
                         expected_resolved_field_data=[True, True]),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_INTEGER,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data='1',
                         expected_resolved_field_data=1),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_INTEGER,
                         schema_mode=TableFieldConstants.MODE_REPEATED,
                         field_data=['1', '2'],
                         expected_resolved_field_data=[1, 2]),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_FLOAT,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data=1,
                         expected_resolved_field_data=float(1)),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_FLOAT,
                         schema_mode=TableFieldConstants.MODE_REPEATED,
                         field_data=[1, 2],
                         expected_resolved_field_data=[float(1), float(2)]),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_STRING,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data=1,
                         expected_resolved_field_data='1'),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_STRING,
                         schema_mode=TableFieldConstants.MODE_REPEATED,
                         field_data=[1, 2],
                         expected_resolved_field_data=['1', '2']),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_BOOLEAN,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data='',
                         expected_resolved_field_data=False),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_BOOLEAN,
                         schema_mode=TableFieldConstants.MODE_REPEATED,
                         field_data=['', ''],
                         expected_resolved_field_data=[False, False]),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_STRING,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data=[],
                         expected_resolved_field_data=None),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_STRING,
                         schema_mode=TableFieldConstants.MODE_REPEATED,
                         field_data=[],
                         expected_resolved_field_data=[]),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_BOOLEAN,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data=[],
                         expected_resolved_field_data=False),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_BOOLEAN,
                         schema_mode=TableFieldConstants.MODE_REPEATED,
                         field_data=[],
                         expected_resolved_field_data=[]),
    ]

    self._run_resolve_schema_conflict_tests(test_data_configs)

    with self.assertRaises(ValueError):
      self._resolver.resolve_schema_conflict(
          bigquery_schema_descriptor.FieldDescriptor(
              type=TableFieldConstants.TYPE_INTEGER,
              mode=TableFieldConstants.MODE_NULLABLE),
          'foo')
      self.fail('Should raise exception for converting str to int')

  def test_resolving_schema_conflict_number(self):
    test_data_configs = [
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_INTEGER,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data=[1, 2, 3],
                         expected_resolved_field_data=1),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_INTEGER,
                         schema_mode=TableFieldConstants.MODE_REPEATED,
                         field_data=[1],
                         expected_resolved_field_data=[1]),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_BOOLEAN,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data=['1', '2'],
                         expected_resolved_field_data=True),
    ]

    self._run_resolve_schema_conflict_tests(test_data_configs)

  def test_resolving_schema_conflict_type_and_number(self):
    test_data_configs = [
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_FLOAT,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data=[1, 2, 3],
                         expected_resolved_field_data=float(1)),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_STRING,
                         schema_mode=TableFieldConstants.MODE_REPEATED,
                         field_data=1,
                         expected_resolved_field_data=[str(1)]),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_BOOLEAN,
                         schema_mode=TableFieldConstants.MODE_NULLABLE,
                         field_data=['1', '2'],
                         expected_resolved_field_data=True),
        SchemaTestConfig(schema_type=TableFieldConstants.TYPE_BOOLEAN,
                         schema_mode=TableFieldConstants.MODE_REPEATED,
                         field_data='1',
                         expected_resolved_field_data=[True]),
    ]

    self._run_resolve_schema_conflict_tests(test_data_configs)

  def test_resolving_attribute_conflict_type(self):
    self.assertEqual(
        self._resolver.resolve_attribute_conflict(
            VcfParserHeaderKeyConstants.TYPE,
            VcfHeaderFieldTypeConstants.INTEGER,
            VcfHeaderFieldTypeConstants.FLOAT),
        VcfHeaderFieldTypeConstants.FLOAT)
    with self.assertRaises(ValueError):
      self._resolver.resolve_attribute_conflict(
          VcfParserHeaderKeyConstants.TYPE,
          VcfHeaderFieldTypeConstants.INTEGER,
          VcfHeaderFieldTypeConstants.STRING)
      self.fail('Should raise exception for unresolvable types')

  def test_resolving_attribute_conflict_number(self):
    self.assertEqual(
        self._resolver.resolve_attribute_conflict(
            VcfParserHeaderKeyConstants.NUM, 2, 3),
        '.')
    self.assertEqual(
        self._resolver.resolve_attribute_conflict(
            VcfParserHeaderKeyConstants.NUM, 2, '.'),
        '.')
    # Unresolvable cases.
    for i in [0, 1]:
      for j in ['R', 'G', 'A', 2, '.']:
        with self.assertRaises(ValueError):
          self._resolver.resolve_attribute_conflict(
              VcfParserHeaderKeyConstants.NUM, i, j)
          self.fail(
              'Should raise exception for unresolvable number: %d vs %d'%(i, j))

  def test_resolving_attribute_conflict_in_number_allele(self):
    self.assertEqual(
        self._resolver_allele.resolve_attribute_conflict(
            VcfParserHeaderKeyConstants.NUM, 2, 3),
        '.')
    self.assertEqual(
        self._resolver_allele.resolve_attribute_conflict(
            VcfParserHeaderKeyConstants.NUM, 2, '.'),
        '.')
    # Unresolvable cases.
    for i in ['A']:
      for j in ['R', 'G', 0, 1, 2, '.']:
        with self.assertRaises(ValueError):
          self._resolver_allele.resolve_attribute_conflict(
              VcfParserHeaderKeyConstants.NUM, i, j)
          self.fail(
              'Should raise exception for unresolvable number: {} vs {}'.format(
                  i, j))

  def test_resolving_all_field_definition_conflict_in_type(self):
    self.assertEqual(
        self._resolver_always.resolve_attribute_conflict(
            VcfParserHeaderKeyConstants.TYPE,
            VcfHeaderFieldTypeConstants.INTEGER,
            VcfHeaderFieldTypeConstants.FLOAT),
        VcfHeaderFieldTypeConstants.FLOAT)
    for i in [VcfHeaderFieldTypeConstants.FLOAT,
              VcfHeaderFieldTypeConstants.INTEGER,
              VcfHeaderFieldTypeConstants.STRING,
              VcfHeaderFieldTypeConstants.CHARACTER]:
      for j in [VcfHeaderFieldTypeConstants.FLAG,
                VcfHeaderFieldTypeConstants.STRING]:
        self.assertEqual(
            self._resolver_always.resolve_attribute_conflict(
                VcfParserHeaderKeyConstants.TYPE, i, j),
            VcfHeaderFieldTypeConstants.STRING)

  def test_resolving_all_field_definition_conflict_in_number(self):
    self.assertEqual(
        self._resolver_always.resolve_attribute_conflict(
            VcfParserHeaderKeyConstants.NUM, 2, 3), '.')
    self.assertEqual(
        self._resolver_always.resolve_attribute_conflict(
            VcfParserHeaderKeyConstants.NUM, 2, '.'), '.')

    for i in [0, 1]:
      for j in ['R', 'G', 'A', 2, '.']:
        self.assertEqual(
            self._resolver_always.resolve_attribute_conflict(
                VcfParserHeaderKeyConstants.NUM, i, j), '.')
