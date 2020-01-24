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

"""Test cases for merge_header_definitions module."""

import unittest
from pysam import libcbcf

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.transforms import merge_header_definitions
from gcp_variant_transforms.libs.vcf_header_definitions_merger import Definition
from gcp_variant_transforms.libs.vcf_header_definitions_merger import VcfHeaderDefinitions


class MergeHeadersTest(unittest.TestCase):

  def _get_header_from_lines(self, lines, file_path):
    header = libcbcf.VariantHeader()
    for line in lines[:-1]:
      header.add_line(line)
    return vcf_header_io.VcfHeader(infos=header.info,
                                   filters=header.filters,
                                   alts=header.alts,
                                   formats=header.formats,
                                   contigs=header.contigs,
                                   file_path=file_path)

  def test_merge_header_definitions_one_header(self):
    lines = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]

    headers = self._get_header_from_lines(lines, 'file1')
    pipeline = TestPipeline()
    merged_definitions = (
        pipeline
        | Create([headers])
        | 'MergeDefinitions' >> merge_header_definitions.MergeDefinitions())

    expected = VcfHeaderDefinitions()
    expected._infos = {'NS': {Definition(1, 'Integer'): ['file1']}}
    assert_that(merged_definitions, equal_to([expected]))
    pipeline.run()

  def test_merge_header_definitions_two_conflicting_headers(self):
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]

    headers_1 = self._get_header_from_lines(lines_1, 'file1')
    headers_2 = self._get_header_from_lines(lines_2, 'file2')
    pipeline = TestPipeline()
    merged_definitions = (
        pipeline
        | Create([headers_1, headers_2])
        | 'MergeDefinitions' >> merge_header_definitions.MergeDefinitions())

    expected = VcfHeaderDefinitions()
    expected._infos = {'NS': {Definition(1, 'Integer'): ['file1'],
                              Definition(1, 'Float'): ['file2']}}
    assert_that(merged_definitions, equal_to([expected]))
    pipeline.run()

  def test_merge_header_definitions_no_conflicting_headers(self):
    lines_1 = [
        '##FORMAT=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##FORMAT=<ID=DP,Number=2,Type=Float,Description="Total Depth">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]

    headers_1 = self._get_header_from_lines(lines_1, 'file1')
    headers_2 = self._get_header_from_lines(lines_2, 'file2')
    pipeline = TestPipeline()
    merged_definitions = (
        pipeline
        | Create([headers_1, headers_2])
        | 'MergeDefinitions' >> merge_header_definitions.MergeDefinitions())

    expected = VcfHeaderDefinitions()
    expected._formats = {'NS': {Definition(1, 'Float'): ['file1']},
                         'DP': {Definition(2, 'Float'): ['file2']}}
    assert_that(merged_definitions, equal_to([expected]))
    pipeline.run()

  def test_merge_header_definitions_same_id_in_info_and_format_headers(self):
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##FORMAT=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]

    headers_1 = self._get_header_from_lines(lines_1, 'file1')
    headers_2 = self._get_header_from_lines(lines_2, 'file2')
    pipeline = TestPipeline()
    merged_definitions = (
        pipeline
        | Create([headers_1, headers_2])
        | 'MergeDefinitions' >> merge_header_definitions.MergeDefinitions())

    expected = VcfHeaderDefinitions()
    expected._infos = {'NS': {Definition(1, 'Integer'): ['file1']}}
    expected._formats = {'NS': {Definition(1, 'Float'): ['file2']}}

    assert_that(merged_definitions, equal_to([expected]))
    pipeline.run()

  def test_merge_header_definitions_save_five_copies(self):
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]

    file_names = ['file1', 'file2', 'file3', 'file4', 'file5', 'file6']
    headers = []
    for file_name in file_names:
      headers.append(self._get_header_from_lines(lines_1, file_name))
    headers.append(self._get_header_from_lines(lines_2, 'file7'))

    pipeline = TestPipeline()
    merged_definitions = (
        pipeline
        | Create(headers, reshuffle=False)
        | 'MergeDefinitions' >> merge_header_definitions.MergeDefinitions())

    expected = VcfHeaderDefinitions()
    expected._infos = {
        'NS': {Definition(1, 'Float'):
                   ['file1', 'file2', 'file3', 'file4', 'file5'],
               Definition(1, 'Integer'): ['file7']}}
    assert_that(merged_definitions, equal_to([expected]))
    pipeline.run()
