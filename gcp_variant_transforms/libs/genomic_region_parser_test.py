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

"""Unit tests for `genomic_region_parser` module."""


import unittest

from gcp_variant_transforms.libs import genomic_region_parser


class GenomicRegionParserTest(unittest.TestCase):

  def test_parse_genomic_regions(self):
    self.assertEqual(
        genomic_region_parser.parse_genomic_region('chr1:1,000,000-2,000,000'),
        ('chr1', 1000000, 2000000)
    )
    self.assertEqual(
        genomic_region_parser.parse_genomic_region('  chrY:1000000-2000000 '),
        ('chrY', 1000000, 2000000)
    )
    self.assertEqual(
        genomic_region_parser.parse_genomic_region('chrY : 1000000 - 2000000'),
        ('chrY', 1000000, 2000000)
    )
    self.assertEqual(
        genomic_region_parser.parse_genomic_region(' chrY : 1,000 - 2000,000 '),
        ('chrY', 1000, 2000000)
    )
    self.assertEqual(
        genomic_region_parser.parse_genomic_region('  chr '),
        ('chr', 0, genomic_region_parser._DEFAULT_END_POSITION)
    )
    self.assertEqual(
        genomic_region_parser.parse_genomic_region('cHrM123XY'),
        ('cHrM123XY', 0, genomic_region_parser._DEFAULT_END_POSITION)
    )
    with self.assertRaises(ValueError):
      genomic_region_parser.parse_genomic_region('chr1:5-5')
