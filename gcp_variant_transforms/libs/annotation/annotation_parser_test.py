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

"""Tests for the annotaiton_parser module.

NOTE(bashir2): Most of the real unit-tests for annotation_parser module are
through unit-testing of processed_variant module.
"""

from __future__ import absolute_import

import unittest

from gcp_variant_transforms.libs.annotation import annotation_parser


class AnnotationParserTest(unittest.TestCase):

  def test_extract_annotation_names(self):
    annotation_str = 'some desc|Consequence|IMPACT|SYMBOL|Gene'
    name_list = annotation_parser.extract_annotation_names(annotation_str)
    self.assertEqual(name_list, ['Consequence', 'IMPACT', 'SYMBOL', 'Gene'])

  def test_extract_annotation_names_error(self):
    annotation_str = 'some desc-Consequence-IMPACT-SYMBOL-Gene'
    with self.assertRaisesRegexp(ValueError, 'Expected at least one.*'):
      annotation_parser.extract_annotation_names(annotation_str)
