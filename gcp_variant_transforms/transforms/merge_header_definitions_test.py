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
import vcf

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.transforms import merge_header_definitions
from gcp_variant_transforms.transforms.merge_header_definitions import HeaderFields

FILE_1_LINES = [
    '##fileformat=VCFv4.2\n',
    '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
    '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n',
    '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="GQ">\n',
    '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2\n']
FILE_2_LINES = [
    '##fileformat=VCFv4.2\n',
    '##INFO=<ID=NS2,Number=1,Type=Integer,Description="Number samples">\n',
    '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n',
    '##FORMAT=<ID=GQ2,Number=1,Type=Integer,Description="GQ">\n',
    '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample3\n']


class MergeHeadersTest(unittest.TestCase):

  def _get_vcf_header_from_reader(self, reader, file_name=None):
    return vcf_header_io.VcfHeader(infos=reader.infos,
                                   filters=reader.filters,
                                   alts=reader.alts,
                                   formats=reader.formats,
                                   contigs=reader.contigs,
                                   file_name=file_name)

  def _get_definitions_combiner_fn(self):
    merger = merge_header_definitions._DefinitionsMerger()
    combiner_fn = merge_header_definitions._MergeDefinitionsFn(merger)
    return combiner_fn

  def test_merge_header_definitions_one_header(self):
    lines = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']

    vcf_reader = vcf.Reader(fsock=iter(lines))
    headers = self._get_vcf_header_from_reader(vcf_reader, 'file1')
    combiner_fn = self._get_definitions_combiner_fn()

    merged_definitions = combiner_fn.create_accumulator()
    merged_definitions = combiner_fn.add_input(merged_definitions, headers)

    definitions = combiner_fn.extract_output(merged_definitions)

    info_definitions = definitions.info
    format_definitions = definitions.format
    self.assertEqual(
        info_definitions['NS'].definitions_to_files[HeaderFields(1, 'Integer')],
        list(['file1']))
    self.assertEqual(len(format_definitions), 0)

  def test_merge_header_definitions_two_conflicting_headers(self):
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
    headers_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    headers_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')
    combiner_fn = self._get_definitions_combiner_fn()

    merged_definitions = combiner_fn.create_accumulator()
    merged_definitions = combiner_fn.add_input(merged_definitions, headers_1)
    merged_definitions = combiner_fn.add_input(merged_definitions, headers_2)
    definitions = combiner_fn.extract_output(merged_definitions)

    info_definitions = definitions.info
    format_definitions = definitions.format
    self.assertEqual(
        info_definitions['NS'].definitions_to_files[HeaderFields(1, 'Integer')],
        list(['file1']))
    self.assertEqual(
        info_definitions['NS'].definitions_to_files[HeaderFields(1, 'Float')],
        list(['file2']))
    self.assertEqual(len(format_definitions), 0)

  def test_merge_header_definitions_no_conflicting_headers(self):
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##FORMAT=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##FORMAT=<ID=DP,Number=2,Type=Float,Description="Total Depth">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    vcf_reader_2 = vcf.Reader(fsock=iter(lines_2))
    headers_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    headers_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')
    combiner_fn = self._get_definitions_combiner_fn()

    merged_definitions = combiner_fn.create_accumulator()
    merged_definitions = combiner_fn.add_input(merged_definitions, headers_1)
    merged_definitions = combiner_fn.add_input(merged_definitions, headers_2)
    definitions = combiner_fn.extract_output(merged_definitions)
    info_definitions = definitions.info
    format_definitions = definitions.format

    self.assertEqual(
        format_definitions['NS'].definitions_to_files[HeaderFields(1, 'Float')],
        list(['file1']))
    self.assertEqual(
        format_definitions['DP'].definitions_to_files[HeaderFields(2, 'Float')],
        list(['file2']))
    self.assertEqual(len(info_definitions), 0)

  def test_merge_header_definitions_same_id_in_info_and_format_headers(self):
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
    headers_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    headers_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')
    combiner_fn = self._get_definitions_combiner_fn()

    merged_definitions = combiner_fn.create_accumulator()
    merged_definitions = combiner_fn.add_input(merged_definitions, headers_1)
    merged_definitions = combiner_fn.add_input(merged_definitions, headers_2)
    definitions = combiner_fn.extract_output(merged_definitions)
    info_definitions = definitions.info
    format_definitions = definitions.format

    self.assertEqual(
        info_definitions['NS'].definitions_to_files[HeaderFields(1, 'Integer')],
        list(['file1']))
    self.assertEqual(
        format_definitions['NS'].definitions_to_files[HeaderFields(1, 'Float')],
        list(['file2']))

  def test_merge_header_definitions_save_five_copies(self):
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
    combiner_fn = self._get_definitions_combiner_fn()

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers_1)
    merged_headers = combiner_fn.add_input(merged_headers, headers_2)
    merged_headers = combiner_fn.add_input(merged_headers, headers_3)
    merged_headers = combiner_fn.add_input(merged_headers, headers_4)
    merged_headers = combiner_fn.add_input(merged_headers, headers_5)
    merged_headers = combiner_fn.add_input(merged_headers, headers_6)
    merged_headers = combiner_fn.add_input(merged_headers, headers_7)

    definitions = combiner_fn.extract_output(merged_headers)
    info_definitions = definitions.info
    format_definitions = definitions.format
    self.assertEqual(info_definitions.keys(), ['NS'])
    self.assertEqual(
        info_definitions['NS'].definitions_to_files[HeaderFields(1, 'Float')],
        list(['file1', 'file2', 'file3', 'file4', 'file5']))
    self.assertEqual(
        info_definitions['NS'].definitions_to_files[HeaderFields(1, 'Integer')],
        list(['file7']))
    self.assertEqual(len(format_definitions), 0)

  def test_combine_pipeline(self):
    vcf_reader_1 = vcf.Reader(fsock=iter(FILE_1_LINES))
    vcf_reader_2 = vcf.Reader(fsock=iter(FILE_2_LINES))
    headers_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    headers_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')

    conflicts_merger = merge_header_definitions._DefinitionsMerger()
    expected = merge_header_definitions.VcfHeaderDefinitions()
    conflicts_merger.merge(
        expected, merge_header_definitions.VcfHeaderDefinitions(headers_1))
    conflicts_merger.merge(
        expected, merge_header_definitions.VcfHeaderDefinitions(headers_2))

    pipeline = TestPipeline()
    merged_definitions = (
        pipeline
        | Create([headers_1, headers_2])
        | 'MergeDefinitions' >> merge_header_definitions.MergeDefinitions())

    assert_that(merged_definitions, equal_to([expected]))
    pipeline.run()
