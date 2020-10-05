# -*- coding: utf-8 -*-
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

"""Tests for bigquery_sanitizer module."""

import unittest

from gcp_variant_transforms.libs import bigquery_sanitizer

_BigQuerySchemaSanitizer = bigquery_sanitizer.SchemaSanitizer


class BigQuerySanitizerTest(unittest.TestCase):

  def test_decode_utf8_string(self):
    self.assertEqual('BÑD',
                     bigquery_sanitizer._decode_utf8_string('BÑD'))
    self.assertEqual('BD',
                     bigquery_sanitizer._decode_utf8_string('BD'))

  def test_get_sanitized_field_name(self):
    self.assertEqual('AA',
                     _BigQuerySchemaSanitizer.get_sanitized_field_name('AA'))
    self.assertEqual('field__AA',
                     _BigQuerySchemaSanitizer.get_sanitized_field_name('_AA'))
    self.assertEqual('field_1A1A',
                     _BigQuerySchemaSanitizer.get_sanitized_field_name('1A1A'))

  def test_get_sanitized_field(self):
    sanitizer = bigquery_sanitizer.FieldSanitizer(None)
    self.assertEqual('valid',
                     sanitizer.get_sanitized_field('valid'))
    self.assertRaises(ValueError,
                      sanitizer.get_sanitized_field, b'\x81DUMMY')
    self.assertEqual([1, 2],
                     sanitizer.get_sanitized_field([1, 2]))
    self.assertEqual(
        [1, bigquery_sanitizer._DEFAULT_NULL_NUMERIC_VALUE_REPLACEMENT, 2],
        sanitizer.get_sanitized_field([1, None, 2]))

    sanitizer_2 = bigquery_sanitizer.FieldSanitizer(-1)
    self.assertEqual([1, -1, 2],
                     sanitizer_2.get_sanitized_field([1, None, 2]))
