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

import collections
import unittest

from apache_beam.io import filesystems

from gcp_variant_transforms import bq_to_vcf
from gcp_variant_transforms.testing import temp_dir


class BqToVcfTest(unittest.TestCase):
  """Test cases for the `bq_to_vcf` module."""

  def _create_mock_args(self, **args):
    return collections.namedtuple('MockArgs', args.keys())(*args.values())

  def test_write_vcf_data_header(self):
    lines = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n',
        '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="GQ">\n',
        '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	\n']
    with temp_dir.TempDir() as tempdir:
      representative_header = tempdir.create_temp_file(lines=lines)
      file_path = filesystems.FileSystems.join(tempdir.get_path(),
                                               'data_header')
      bq_to_vcf._write_vcf_header_with_call_names(
          ['Sample 1', 'Sample 2'],
          ['#CHROM', 'POS', 'ID', 'REF', 'ALT'],
          representative_header,
          file_path)
      expected_content = [
          '##fileformat=VCFv4.2\n',
          '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
          '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
          '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n',
          '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="GQ">\n',
          '#CHROM\tPOS\tID\tREF\tALT\tSample 1\tSample 2\n'
      ]
      with filesystems.FileSystems.open(file_path) as f:
        content = f.readlines()
        self.assertEqual(content, expected_content)

