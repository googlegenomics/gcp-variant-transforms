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

"""Test cases for merge_headers module."""

from collections import OrderedDict
import unittest
from pysam import libcbcf

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs.header_merger import HeaderMerger
from gcp_variant_transforms.transforms import merge_headers

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
  """Test cases for GetMergeHeaders `PTransform`."""

  def _get_header_from_lines(self, lines):
    """Extracts header from lines."""
    header = libcbcf.VariantHeader()
    for line in lines[:-1]:
      header.add_line(line)
    return vcf_header_io.VcfHeader(infos=header.info,
                                   filters=header.filters,
                                   alts=header.alts,
                                   formats=header.formats,
                                   contigs=header.contigs)

  def _get_combiner_fn(self, split_alternate_allele_info_fields=True):
    resolver = vcf_field_conflict_resolver.FieldConflictResolver(
        split_alternate_allele_info_fields)
    header_merger = HeaderMerger(resolver)
    combiner_fn = merge_headers._MergeHeadersFn(header_merger)
    return combiner_fn

  def test_combine_single_header(self):
    headers = self._get_header_from_lines(FILE_1_LINES)
    combiner_fn = self._get_combiner_fn()

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers)
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(list(merged_headers.infos.keys()), ['NS', 'AF'])
    self.assertItemsEqual(list(merged_headers.formats.keys()), ['GT', 'GQ'])

  def test_combine_multiple_headers_as_inputs(self):
    headers_1 = self._get_header_from_lines(FILE_1_LINES)
    headers_2 = self._get_header_from_lines(FILE_2_LINES)

    combiner_fn = self._get_combiner_fn()

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers_1)
    merged_headers = combiner_fn.add_input(merged_headers, headers_2)
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(list(merged_headers.infos.keys()), ['NS', 'AF', 'NS2'])
    self.assertItemsEqual(list(merged_headers.formats.keys()), ['GT', 'GQ', 'GQ2'])

  def test_combine_multiple_headers_as_accumulators(self):
    headers_1 = self._get_header_from_lines(FILE_1_LINES)
    headers_2 = self._get_header_from_lines(FILE_2_LINES)

    combiner_fn = self._get_combiner_fn()

    merged_headers_1 = combiner_fn.create_accumulator()
    merged_headers_1 = combiner_fn.add_input(merged_headers_1, headers_1)
    merged_headers_2 = combiner_fn.create_accumulator()
    merged_headers_2 = combiner_fn.add_input(merged_headers_2, headers_2)
    merged_headers = combiner_fn.merge_accumulators([merged_headers_1,
                                                     merged_headers_2])
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(list(merged_headers.infos.keys()), ['NS', 'AF', 'NS2'])
    self.assertItemsEqual(list(merged_headers.formats.keys()), ['GT', 'GQ', 'GQ2'])

  def test_combine_two_type_conflicting_but_resolvable_headers(self):
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

    headers_1 = self._get_header_from_lines(lines_1)
    headers_2 = self._get_header_from_lines(lines_2)

    combiner_fn = self._get_combiner_fn()

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers_1)
    merged_headers = combiner_fn.add_input(merged_headers, headers_2)
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(list(merged_headers.infos.keys()), ['NS'])
    self.assertItemsEqual(merged_headers.infos['NS'],
                          OrderedDict([('id', 'NS'),
                                       ('num', 1),
                                       ('type', 'Float'),
                                       ('desc', 'Number samples'),
                                       ('source', None),
                                       ('version', None)]))

  def test_none_type_defaults_to_string(self):
    # This header's type is `None`, so we convert it to `String` while merging.
    lines = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']

    headers = self._get_header_from_lines(lines)
    headers.infos['NS']['type'] = None

    combiner_fn = self._get_combiner_fn()
    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers)
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(list(merged_headers.infos.keys()), ['NS'])
    self.assertItemsEqual(merged_headers.infos['NS'],
                          OrderedDict([('id', 'NS'),
                                       ('num', 1),
                                       ('type', 'String'),
                                       ('desc', 'Number samples'),
                                       ('source', None),
                                       ('version', None)]))

  def test_combine_two_num_conflicting_but_resolvable_headers_1(self):
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

    headers_1 = self._get_header_from_lines(lines_1)
    headers_2 = self._get_header_from_lines(lines_2)

    combiner_fn = self._get_combiner_fn()

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers_1)
    merged_headers = combiner_fn.add_input(merged_headers, headers_2)
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(list(merged_headers.infos.keys()), ['NS'])
    self.assertItemsEqual(merged_headers.infos['NS'],
                          OrderedDict([('id', 'NS'),
                                       ('num', '.'),
                                       ('type', 'Integer'),
                                       ('desc', 'Number samples'),
                                       ('source', None),
                                       ('version', None)]))

  def test_combine_two_num_conflicting_but_resolvable_headers_2(self):
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

    headers_1 = self._get_header_from_lines(lines_1)
    headers_2 = self._get_header_from_lines(lines_2)

    combiner_fn = self._get_combiner_fn()

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers_1)
    merged_headers = combiner_fn.add_input(merged_headers, headers_2)
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(list(merged_headers.infos.keys()), ['NS'])
    self.assertItemsEqual(merged_headers.infos['NS'],
                          OrderedDict([('id', 'NS'),
                                       ('num', '.'),
                                       ('type', 'Integer'),
                                       ('desc', 'Number samples'),
                                       ('source', None),
                                       ('version', None)]))

  def test_combine_two_num_conflicting_but_resolvable_headers_3(self):
    # Test with split_alternate_allele_info_fields=False
    #
    # These two headers have incompable Number field (A vs dot).
    # `Number=A` is compatible with dot when flag
    # split_alternate_allele_info_fields is off.
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=A,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=.,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    headers_1 = self._get_header_from_lines(lines_1)
    headers_2 = self._get_header_from_lines(lines_2)

    combiner_fn = self._get_combiner_fn(
        split_alternate_allele_info_fields=False)

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers_1)
    merged_headers = combiner_fn.add_input(merged_headers, headers_2)
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(list(merged_headers.infos.keys()), ['NS'])
    self.assertItemsEqual(merged_headers.infos['NS'],
                          OrderedDict([('id', 'NS'),
                                       ('num', '.'),
                                       ('type', 'Integer'),
                                       ('desc', 'Number samples'),
                                       ('source', None),
                                       ('version', None)]))

  def test_combine_two_num_conflicting_but_not_resolvable_headers(self):
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

    headers_1 = self._get_header_from_lines(lines_1)
    headers_2 = self._get_header_from_lines(lines_2)

    combiner_fn = self._get_combiner_fn()

    with self.assertRaises(ValueError):
      merged_headers = combiner_fn.create_accumulator()
      merged_headers = combiner_fn.add_input(merged_headers, headers_1)
      merged_headers = combiner_fn.add_input(merged_headers, headers_2)
      merged_headers = combiner_fn.extract_output(merged_headers)

  def test_combine_two_headers_with_bad_conflict(self):
    # Type mistmach (String vs Float) cannot be resolved..
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=String,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    headers_1 = self._get_header_from_lines(lines_1)
    headers_2 = self._get_header_from_lines(lines_2)

    combiner_fn = self._get_combiner_fn()

    with self.assertRaises(ValueError):
      merged_headers = combiner_fn.create_accumulator()
      merged_headers = combiner_fn.add_input(merged_headers, headers_1)
      merged_headers = combiner_fn.add_input(merged_headers, headers_2)
      merged_headers = combiner_fn.extract_output(merged_headers)


  def test_combine_pipeline(self):
    headers_1 = self._get_header_from_lines(FILE_1_LINES)
    headers_2 = self._get_header_from_lines(FILE_2_LINES)

    # TODO(nmousavi): Either use TestPipeline or combiner_fn.* everywhere.
    # After moving out _HeaderMerger to its file, it makes sense to use
    # TestPipeline everywhere.
    header_merger = HeaderMerger(
        vcf_field_conflict_resolver.FieldConflictResolver(
            split_alternate_allele_info_fields=True))
    expected = vcf_header_io.VcfHeader()
    header_merger.merge(expected, headers_1)
    header_merger.merge(expected, headers_2)

    pipeline = TestPipeline()
    merged_headers = (
        pipeline
        | Create([headers_1, headers_2])
        | 'MergeHeaders' >> merge_headers.MergeHeaders())

    assert_that(merged_headers, equal_to([expected]))
