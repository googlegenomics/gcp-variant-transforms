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

from gcp_variant_transforms.libs import bigquery_util

class BigqueryUtilTest(unittest.TestCase):

  def test_get_bigquery_sanitized_field_name(self):
    self.assertEqual('AA',
                     bigquery_util.get_bigquery_sanitized_field_name('AA'))
    self.assertEqual('field__AA',
                     bigquery_util.get_bigquery_sanitized_field_name('_AA'))
    self.assertEqual('field_1A1A',
                     bigquery_util.get_bigquery_sanitized_field_name('1A1A'))

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

  def test_get_bigquery_sanitized_field(self):
    self.assertEqual(u'valid',
                     bigquery_util.get_bigquery_sanitized_field('valid'))
    self.assertRaises(
        ValueError,
        bigquery_util.get_bigquery_sanitized_field, '\x81DUMMY')
    self.assertEqual([1, 2],
                     bigquery_util.get_bigquery_sanitized_field([1, 2]))
    self.assertEqual([1, -1, 2],
                     bigquery_util.get_bigquery_sanitized_field(
                         [1, None, 2], null_numeric_value_replacement=-1))
