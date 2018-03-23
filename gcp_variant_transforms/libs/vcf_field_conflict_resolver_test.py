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
import vcf

from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs.vcf_field_conflict_resolver import VcfParserConstants


class ConflictResolverTest(unittest.TestCase):
  """Test case for :class:`FieldConflictResolver`"""

  def setUp(self):
    self._resolver = vcf_field_conflict_resolver.FieldConflictResolver()
    self._resolver_allele = vcf_field_conflict_resolver.FieldConflictResolver(
        split_alternate_allele_info_fields=True)
    self._resolver_always = vcf_field_conflict_resolver.FieldConflictResolver(
        resolve_always=True)

  def _field_count(self, symbol):
    # type: (str) -> int
    # symbol = {'A', 'G', 'R'}.
    return vcf.parser.field_counts[symbol]

  def test_resolving_field_definition_conflict_in_type(self):
    self.assertEqual(
        self._resolver.resolve(VcfParserConstants.TYPE,
                               VcfParserConstants.INTEGER,
                               VcfParserConstants.FLOAT),
        VcfParserConstants.FLOAT)
    with self.assertRaises(ValueError):
      self._resolver.resolve(VcfParserConstants.TYPE,
                             VcfParserConstants.INTEGER, 'String')
      self.fail('Should raise exception for unresolvable types')

  def test_resolving_field_definition_conflict_in_number(self):
    self.assertEqual(
        self._resolver.resolve(VcfParserConstants.NUM, 2, 3), None)
    self.assertEqual(
        self._resolver.resolve(VcfParserConstants.NUM, 2, None), None)
    # Unresolvable cases.
    for i in [0, 1]:
      for j in [self._field_count('R'), self._field_count('G'),
                self._field_count('A'), 2, None]:
        with self.assertRaises(ValueError):
          self._resolver.resolve(VcfParserConstants.NUM, i, j)
          self.fail(
              'Should raise exception for unresolvable number: %d vs %d'%(i, j))

  def test_resolving_field_definition_conflict_in_number_allele(self):
    self.assertEqual(
        self._resolver_allele.resolve(VcfParserConstants.NUM, 2, 3), None)
    self.assertEqual(
        self._resolver_allele.resolve(VcfParserConstants.NUM, 2, None), None)
    # Unresolvable cases.
    for i in [self._field_count('A')]:
      for j in [self._field_count('R'), self._field_count('G'), 0, 1, 2, None]:
        with self.assertRaises(ValueError):
          self._resolver_allele.resolve(VcfParserConstants.NUM, i, j)
          self.fail(
              'Should raise exception for unresolvable number: %d vs %d'%(i, j))

  def test_resolving_all_field_definition_conflict_in_type(self):
    self.assertEqual(
        self._resolver_always.resolve(VcfParserConstants.TYPE,
                                      VcfParserConstants.INTEGER,
                                      VcfParserConstants.FLOAT),
        VcfParserConstants.FLOAT)
    for i in [VcfParserConstants.FLOAT, VcfParserConstants.INTEGER,
              VcfParserConstants.STRING, VcfParserConstants.CHARACTER]:
      for j in [VcfParserConstants.FLAG, VcfParserConstants.STRING]:
        self.assertEqual(
            self._resolver_always.resolve(VcfParserConstants.TYPE, i, j),
            VcfParserConstants.STRING)

  def test_resolving_all_field_definition_conflict_in_number(self):
    self.assertEqual(
        self._resolver_always.resolve(VcfParserConstants.NUM, 2, 3), None)
    self.assertEqual(
        self._resolver_always.resolve(VcfParserConstants.NUM, 2, None), None)

    for i in [0, 1]:
      for j in [self._field_count('R'), self._field_count('G'),
                self._field_count('A'), 2, None]:
        self.assertEqual(
            self._resolver_always.resolve(VcfParserConstants.NUM, i, j), None)
