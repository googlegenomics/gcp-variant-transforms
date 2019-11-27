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

"""Test cases for header_merger module."""

from collections import OrderedDict
import unittest
from pysam import libcbcf

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs.header_merger import HeaderMerger

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

class HeaderMergerTest(unittest.TestCase):
  """Test cases for HeaderMerger module."""

  def _get_header_merger(self, split_alternate_allele_info_fields=True):
    resolver = vcf_field_conflict_resolver.FieldConflictResolver(
        split_alternate_allele_info_fields)
    merger = HeaderMerger(resolver)
    return merger

  def _get_header_from_lines(self, lines):
    header = libcbcf.VariantHeader()
    for line in lines[:-1]:
      header.add_line(line)
    return vcf_header_io.VcfHeader(infos=header.info,
                                   filters=header.filters,
                                   alts=header.alts,
                                   formats=header.formats,
                                   contigs=header.contigs)

  def test_merge_header_with_empty_one(self):
    merger = self._get_header_merger()
    header_1 = self._get_header_from_lines(FILE_1_LINES)
    header_2 = vcf_header_io.VcfHeader()

    merger.merge(header_1, header_2)
    merger.merge(header_2, header_1)

    self.assertItemsEqual(header_1.infos.keys(), ['NS', 'AF'])
    self.assertItemsEqual(header_1.formats.keys(), ['GT', 'GQ'])
    self.assertItemsEqual(header_2.infos.keys(), ['NS', 'AF'])
    self.assertItemsEqual(header_2.formats.keys(), ['GT', 'GQ'])

  def test_merge_two_headers(self):
    main_header = self._get_header_from_lines(FILE_1_LINES)
    secondary_header = self._get_header_from_lines(FILE_2_LINES)

    merger = self._get_header_merger()
    merger.merge(main_header, secondary_header)

    self.assertItemsEqual(main_header.infos.keys(), ['NS', 'AF', 'NS2'])
    self.assertItemsEqual(main_header.formats.keys(), ['GT', 'GQ', 'GQ2'])

  def test_merge_two_type_conflicting_but_resolvable_headers(self):
    # These two headers have type conflict (Integer vs Float), however pipeline
    # doesn't raise error because the type conflict is resolvable.
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    main_header = self._get_header_from_lines(lines_1)
    secondary_header = self._get_header_from_lines(lines_2)

    merger = self._get_header_merger()

    merger.merge(main_header, secondary_header)

    self.assertItemsEqual(main_header.infos.keys(), ['NS'])
    self.assertItemsEqual(main_header.infos['NS'],
                          OrderedDict([('id', 'NS'),
                                       ('num', 1),
                                       ('type', 'Float'),
                                       ('desc', 'Number samples'),
                                       ('source', None),
                                       ('version', None)]))

  def test_merge_two_num_conflicting_but_resolvable_headers_1(self):
    # These two headers have conflict in Number field (2 vs dot), however
    # pipeline doesn't raise error because the conflict is resolvable.
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=2,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=.,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    main_header = self._get_header_from_lines(lines_1)
    secondary_header = self._get_header_from_lines(lines_2)

    merger = self._get_header_merger()

    merger.merge(main_header, secondary_header)

    self.assertItemsEqual(main_header.infos.keys(), ['NS'])
    self.assertItemsEqual(main_header.infos['NS'],
                          OrderedDict([('id', 'NS'),
                                       ('num', '.'),
                                       ('type', 'Integer'),
                                       ('desc', 'Number samples'),
                                       ('source', None),
                                       ('version', None)]))

  def test_merge_two_num_conflicting_but_resolvable_headers_2(self):
    # These two headers have conflict in Number field (2 vs 3), however
    # pipeline doesn't raise error because the conflict is resolvable.
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=2,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=3,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    main_header = self._get_header_from_lines(lines_1)
    secondary_header = self._get_header_from_lines(lines_2)

    merger = self._get_header_merger()

    merger.merge(main_header, secondary_header)

    self.assertItemsEqual(main_header.infos.keys(), ['NS'])
    self.assertItemsEqual(main_header.infos['NS'],
                          OrderedDict([('id', 'NS'),
                                       ('num', '.'),
                                       ('type', 'Integer'),
                                       ('desc', 'Number samples'),
                                       ('source', None),
                                       ('version', None)]))

  def test_merge_two_num_conflicting_but_not_resolvable_headers(self):
    # Test with split_alternate_allele_info_fields=True
    #
    # These two headers have incompable Number field (A vs dot).
    # `Number=A` is incompatible with dot when flag
    # split_alternate_allele_info_fields is set.

    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=A,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=.,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    main_header = self._get_header_from_lines(lines_1)
    secondary_header = self._get_header_from_lines(lines_2)

    merger = self._get_header_merger()

    with self.assertRaises(ValueError):
      merger.merge(main_header, secondary_header)

  def test_merge_two_headers_with_bad_conflict(self):
    # Type mistmach (String vs Float) cannot be resolved..
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=String,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    main_header = self._get_header_from_lines(lines_1)
    secondary_header = self._get_header_from_lines(lines_2)

    merger = self._get_header_merger()

    with self.assertRaises(ValueError):
      merger.merge(main_header, secondary_header)
