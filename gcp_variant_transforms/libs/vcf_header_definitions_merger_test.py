# Copyright 2019 Google Inc.  All Rights Reserved.
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

"""Test cases for definitions_merger module."""

import unittest
import vcf

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs.vcf_header_definitions_merger import DefinitionsMerger
from gcp_variant_transforms.libs.vcf_header_definitions_merger import Definition
from gcp_variant_transforms.libs.vcf_header_definitions_merger import VcfHeaderDefinitions

class VcfHeaderDefinitionsMergerTest(unittest.TestCase):

  def _get_vcf_header_from_reader(self, reader, file_name):
    return vcf_header_io.VcfHeader(infos=reader.infos,
                                   filters=reader.filters,
                                   alts=reader.alts,
                                   formats=reader.formats,
                                   contigs=reader.contigs,
                                   file_name=file_name)

  def _create_definitions_from_lines(self, lines, file_name):
    vcf_reader = vcf.Reader(fsock=iter(lines))
    header = self._get_vcf_header_from_reader(vcf_reader, file_name)
    return VcfHeaderDefinitions(header)

  def test_create_empty_header_defitions(self):
    expected_info = {}
    expected_format = {}
    header_definitions = VcfHeaderDefinitions()

    self.assertDictEqual(header_definitions.infos, expected_info)
    self.assertDictEqual(header_definitions.formats, expected_format)

  def test_create_definitions_with_format(self):
    lines = [
        '##FORMAT=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]

    vcf_reader = vcf.Reader(fsock=iter(lines))
    header = self._get_vcf_header_from_reader(vcf_reader, 'file1')

    expected_info = {}
    expected_format = {'NS': {Definition(1, 'Integer'): ['file1']}}
    header_definitions = VcfHeaderDefinitions(header)

    self.assertDictEqual(header_definitions.infos, expected_info)
    self.assertDictEqual(header_definitions.formats, expected_format)

  def test_create_definitions_with_info(self):
    lines = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]

    vcf_reader = vcf.Reader(fsock=iter(lines))
    header = self._get_vcf_header_from_reader(vcf_reader, 'file1')

    expected_info = {'NS': {Definition(1, 'Float'): ['file1']}}
    expected_format = {}
    header_definitions = VcfHeaderDefinitions(header)

    self.assertDictEqual(header_definitions.infos, expected_info)
    self.assertDictEqual(header_definitions.formats, expected_format)

  def test_create_definitions_multi(self):
    lines = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '##INFO=<ID=DP,Number=2,Type=Float,Description="Number samples">\n',
        '##FORMAT=<ID=NS,Number=3,Type=Integer,Description="Number samples">\n',
        '##FORMAT=<ID=DP,Number=4,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]

    vcf_reader = vcf.Reader(fsock=iter(lines))
    header = self._get_vcf_header_from_reader(vcf_reader, 'file1')

    expected_info = {
        'NS': {Definition(1, 'Integer'): ['file1']},
        'DP': {Definition(2, 'Float'): ['file1']}
    }
    expected_format = {
        'NS': {Definition(3, 'Integer'): ['file1']},
        'DP': {Definition(4, 'Float'): ['file1']}
    }
    header_definitions = VcfHeaderDefinitions(header)

    self.assertDictEqual(header_definitions.infos, expected_info)
    self.assertDictEqual(header_definitions.formats, expected_format)

  def test_type_check(self):
    merger = DefinitionsMerger()
    empty_header_definitions = VcfHeaderDefinitions()
    with self.assertRaises(NotImplementedError):
      merger.merge(empty_header_definitions, None)
    with self.assertRaises(NotImplementedError):
      merger.merge(None, empty_header_definitions)

  def test_merge_same_definition(self):
    merger = DefinitionsMerger()
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '##FORMAT=<ID=DP,Number=3,Type=Char,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples2">\n',
        '##FORMAT=<ID=DP,Number=3,Type=Char,Description="Number samples2">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]
    main_definitions = self._create_definitions_from_lines(lines_1, 'file1')
    secondary_definitions = self._create_definitions_from_lines(
        lines_2, 'file2')

    expected_infos = {'NS': {Definition(1, 'Float'): ['file1', 'file2']}}
    expected_formats = {'DP': {Definition(3, 'Char'): ['file1', 'file2']}}
    merger.merge(main_definitions, secondary_definitions)

    self.assertDictEqual(expected_infos, main_definitions.infos)
    self.assertDictEqual(expected_formats, main_definitions.formats)

  def test_merge_different_type(self):
    merger = DefinitionsMerger()
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '##FORMAT=<ID=DP,Number=3,Type=Char,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples2">\n',
        '##FORMAT=<ID=DP,Number=3,Type=String,Description="Number samples2">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]
    main_definitions = self._create_definitions_from_lines(lines_1, 'file1')
    secondary_definitions = self._create_definitions_from_lines(
        lines_2, 'file2')

    expected_infos = {
        'NS': {
            Definition(1, 'Float'): ['file1'],
            Definition(1, 'Integer'): ['file2']
        }
    }
    expected_formats = {
        'DP': {
            Definition(3, 'Char'): ['file1'],
            Definition(3, 'String'): ['file2']
        }
    }
    merger.merge(main_definitions, secondary_definitions)

    self.assertDictEqual(expected_infos, main_definitions.infos)
    self.assertDictEqual(expected_formats, main_definitions.formats)

  def test_merge_different_number(self):
    merger = DefinitionsMerger()
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '##FORMAT=<ID=DP,Number=3,Type=Char,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##INFO=<ID=NS,Number=2,Type=Float,Description="Number samples2">\n',
        '##FORMAT=<ID=DP,Number=4,Type=Char,Description="Number samples2">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]
    main_definitions = self._create_definitions_from_lines(lines_1, 'file1')
    secondary_definitions = self._create_definitions_from_lines(
        lines_2, 'file2')

    expected_infos = {
        'NS': {
            Definition(1, 'Float'): ['file1'],
            Definition(2, 'Float'): ['file2']
        }
    }
    expected_formats = {
        'DP': {
            Definition(3, 'Char'): ['file1'],
            Definition(4, 'Char'): ['file2']
        }
    }
    merger.merge(main_definitions, secondary_definitions)

    self.assertDictEqual(expected_infos, main_definitions.infos)
    self.assertDictEqual(expected_formats, main_definitions.formats)

  def test_merge_different_id(self):
    merger = DefinitionsMerger()
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '##FORMAT=<ID=DP,Number=3,Type=Char,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##INFO=<ID=NK,Number=1,Type=Float,Description="Number samples2">\n',
        '##FORMAT=<ID=DL,Number=3,Type=Char,Description="Number samples2">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]
    main_definitions = self._create_definitions_from_lines(lines_1, 'file1')
    secondary_definitions = self._create_definitions_from_lines(
        lines_2, 'file2')

    expected_infos = {
        'NS': {Definition(1, 'Float'): ['file1']},
        'NK': {Definition(1, 'Float'): ['file2']}
    }
    expected_formats = {
        'DP': {Definition(3, 'Char'): ['file1']},
        'DL': {Definition(3, 'Char'): ['file2']}
    }
    merger.merge(main_definitions, secondary_definitions)

    self.assertDictEqual(expected_infos, main_definitions.infos)
    self.assertDictEqual(expected_formats, main_definitions.formats)
