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

"""Test cases for merge_conflicts module."""

import unittest
import vcf

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.transforms import merge_conflicts


class MergeHeadersTest(unittest.TestCase):

  def _get_vcf_header_from_reader(self, reader, file_name=None):
    return vcf_header_io.VcfHeader(infos=reader.infos,
                                   filters=reader.filters,
                                   alts=reader.alts,
                                   formats=reader.formats,
                                   contigs=reader.contigs,
                                   file_name=file_name)

  def _get_conflicts_combiner_fn(self):
    conflicts_merger = merge_conflicts._ConflictsMerger()
    combiner_fn = merge_conflicts._MergeConflictsFn(conflicts_merger)
    return combiner_fn

  def test_merge_conflicts_one_header(self):
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']

    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    conflicts_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    combiner_fn = self._get_conflicts_combiner_fn()

    merged_conflicts = combiner_fn.create_accumulator()
    merged_conflicts = combiner_fn.add_input(merged_conflicts, conflicts_1)

    conflicts = combiner_fn.extract_output(merged_conflicts)

    info_conflicts = conflicts.info_conflicts
    format_conflicts = conflicts.format_conflicts
    self.assertEqual(info_conflicts['NS'].conflicts['num=1 type=Integer'],
                     list(['file1']))
    self.assertEqual(len(format_conflicts), 0)

  def test_merge_conflicts_two_conflicting_headers(self):
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    vcf_reader_2 = vcf.Reader(fsock=iter(lines_2))
    conflicts_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    conflicts_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')
    combiner_fn = self._get_conflicts_combiner_fn()

    merged_conflicts = combiner_fn.create_accumulator()
    merged_conflicts = combiner_fn.add_input(merged_conflicts, conflicts_1)
    merged_conflicts = combiner_fn.add_input(merged_conflicts, conflicts_2)
    conflicts = combiner_fn.extract_output(merged_conflicts)

    info_conflicts = conflicts.info_conflicts
    format_conflicts = conflicts.format_conflicts
    self.assertEqual(info_conflicts['NS'].conflicts['num=1 type=Integer'],
                     list(['file1']))
    self.assertEqual(info_conflicts['NS'].conflicts['num=1 type=Float'],
                     list(['file2']))
    self.assertEqual(len(format_conflicts), 0)

  def test_merge_conflicts_no_conflicting_headers(self):
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##FORMAT=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Total Depth">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    vcf_reader_2 = vcf.Reader(fsock=iter(lines_2))
    conflicts_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    conflicts_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')
    combiner_fn = self._get_conflicts_combiner_fn()

    merged_conflicts = combiner_fn.create_accumulator()
    merged_conflicts = combiner_fn.add_input(merged_conflicts, conflicts_1)
    merged_conflicts = combiner_fn.add_input(merged_conflicts, conflicts_2)
    conflicts = combiner_fn.extract_output(merged_conflicts)
    info_conflicts = conflicts.info_conflicts
    format_conflicts = conflicts.format_conflicts

    self.assertEqual(format_conflicts['NS'].conflicts['num=1 type=Float'],
                     list(['file1']))
    self.assertEqual(format_conflicts['DP'].conflicts['num=1 type=Integer'],
                     list(['file2']))
    self.assertEqual(len(info_conflicts), 0)

  def test_merge_conflicts_no_conflicting_headers_save_five_copies(self):
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    vcf_reader_2 = vcf.Reader(fsock=iter(lines_2))
    headers_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    headers_2 = self._get_vcf_header_from_reader(vcf_reader_1, 'file2')
    headers_3 = self._get_vcf_header_from_reader(vcf_reader_1, 'file3')
    headers_4 = self._get_vcf_header_from_reader(vcf_reader_1, 'file4')
    headers_5 = self._get_vcf_header_from_reader(vcf_reader_1, 'file5')
    headers_6 = self._get_vcf_header_from_reader(vcf_reader_1, 'file6')
    headers_7 = self._get_vcf_header_from_reader(vcf_reader_2, 'file7')
    combiner_fn = self._get_conflicts_combiner_fn()

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers_1)
    merged_headers = combiner_fn.add_input(merged_headers, headers_2)
    merged_headers = combiner_fn.add_input(merged_headers, headers_3)
    merged_headers = combiner_fn.add_input(merged_headers, headers_4)
    merged_headers = combiner_fn.add_input(merged_headers, headers_5)
    merged_headers = combiner_fn.add_input(merged_headers, headers_6)
    merged_headers = combiner_fn.add_input(merged_headers, headers_7)

    conflicts = combiner_fn.extract_output(merged_headers)
    info_conflicts = conflicts.info_conflicts
    format_conflicts = conflicts.format_conflicts
    self.assertEqual(info_conflicts.keys(), ['NS'])
    self.assertEqual(info_conflicts['NS'].conflicts['num=1 type=Float'],
                     list(['file1', 'file2', 'file3', 'file4', 'file5']))
    self.assertEqual(info_conflicts['NS'].conflicts['num=1 type=Integer'],
                     list(['file7']))
    self.assertEqual(len(format_conflicts), 0)

  def test_merge_conflicts_same_id_in_info_and_format_headers(self):
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##FORMAT=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    vcf_reader_2 = vcf.Reader(fsock=iter(lines_2))
    conflicts_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    conflicts_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')
    combiner_fn = self._get_conflicts_combiner_fn()

    merged_conflicts = combiner_fn.create_accumulator()
    merged_conflicts = combiner_fn.add_input(merged_conflicts, conflicts_1)
    merged_conflicts = combiner_fn.add_input(merged_conflicts, conflicts_2)
    conflicts = combiner_fn.extract_output(merged_conflicts)
    info_conflicts = conflicts.info_conflicts
    format_conflicts = conflicts.format_conflicts

    self.assertEqual(info_conflicts['NS'].conflicts['num=1 type=Integer'],
                     list(['file1']))
    self.assertEqual(format_conflicts['NS'].conflicts['num=1 type=Float'],
                     list(['file2']))
