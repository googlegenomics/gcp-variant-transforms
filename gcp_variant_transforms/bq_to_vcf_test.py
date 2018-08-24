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

"""Tests for `bq_to_vcf` module."""

import unittest

from apache_beam.io import filesystems

from gcp_variant_transforms import bq_to_vcf
from gcp_variant_transforms.testing import temp_dir


class BqToVcfTest(unittest.TestCase):
  """Test cases for the `bq_to_vcf` module."""

  def test_write_vcf_data_header(self):
    with temp_dir.TempDir() as tempdir:
      file_path = filesystems.FileSystems.join(tempdir.get_path(),
                                               'data_header')
      bq_to_vcf._write_vcf_data_header(['Sample 1', 'Sample 2'],
                                       ['#CHROM', 'POS', 'ID', 'REF', 'ALT'],
                                       file_path)
      expected_content = '#CHROM\tPOS\tID\tREF\tALT\tSample 1\tSample 2\n'
      with filesystems.FileSystems.open(file_path) as f:
        content = f.readlines()
        self.assertEqual(content, [expected_content])
