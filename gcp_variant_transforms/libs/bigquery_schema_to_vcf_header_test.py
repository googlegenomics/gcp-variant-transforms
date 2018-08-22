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

"""Tests for `bigquery_schema_to_vcf_header` module."""

import unittest

from collections import OrderedDict

from vcf import parser

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import bigquery_schema_to_vcf_header
from gcp_variant_transforms.testing import bigquery_schema_util


class VcfHeaderSourceTest(unittest.TestCase):
  """Test cases for the `generate_header_fields_from_schema` function."""

  def test_generate_header_fields_from_schema(self):
    header = bigquery_schema_to_vcf_header.generate_header_fields_from_schema(
        bigquery_schema_util.get_sample_table_schema())
    infos = OrderedDict([
        ('II', parser._Info('II', '.', 'Integer', 'desc', 'src', 'v')),
        ('IFR', parser._Info('IFR', '.', 'Float', 'desc', 'src', 'v')),
        ('IS', parser._Info('IS', '.', 'String', 'desc', 'src', 'v')),
        ('AF', parser._Info('AF', 'A', 'Float', 'desc', 'src', 'v'))
    ])

    formats = OrderedDict([
        ('FB', parser._Format('FB', '.', 'Flag', 'desc')),
        ('GQ', parser._Format('GQ', '.', 'Integer', 'desc'))
    ])
    expected_header = vcf_header_io.VcfHeader(infos=infos, formats=formats)
    self.assertEqual(dict(expected_header.infos), dict(header.infos))
    self.assertEqual(dict(expected_header.formats), dict(header.formats))
