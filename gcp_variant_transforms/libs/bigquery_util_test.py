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

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import bigquery_util


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
    self.assertEqual(None,
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
