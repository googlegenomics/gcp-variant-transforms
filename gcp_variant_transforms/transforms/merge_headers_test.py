# Copyright 2017 Google Inc.  All Rights Reserved.
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

"""Test cases for get_merged_headers module."""

from collections import OrderedDict
import unittest
import vcf

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcf_header_io
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

  def _get_header_from_reader(self, reader):
    """Extracts values from a pyVCF reader into a VcfHeader object."""
    return vcf_header_io.VcfHeader(
        infos=reader.infos,
        filters=reader.filters,
        alts=reader.alts,
        formats=reader.formats,
        contigs=reader.contigs)

  def test_combine_single_header(self):
    vcf_reader = vcf.Reader(fsock=iter(FILE_1_LINES))
    headers = self._get_header_from_reader(vcf_reader)
    combiner_fn = merge_headers._MergeHeadersFn(force_merge_conflicts=False)

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers)
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(merged_headers.infos.keys(), ['NS', 'AF'])
    self.assertItemsEqual(merged_headers.formats.keys(), ['GT', 'GQ'])

  def test_combine_multiple_headers_as_inputs(self):
    vcf_reader_1 = vcf.Reader(fsock=iter(FILE_1_LINES))
    vcf_reader_2 = vcf.Reader(fsock=iter(FILE_2_LINES))
    headers_1 = self._get_header_from_reader(vcf_reader_1)
    headers_2 = self._get_header_from_reader(vcf_reader_2)

    combiner_fn = merge_headers._MergeHeadersFn(force_merge_conflicts=False)

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers_1)
    merged_headers = combiner_fn.add_input(merged_headers, headers_2)
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(merged_headers.infos.keys(), ['NS', 'AF', 'NS2'])
    self.assertItemsEqual(merged_headers.formats.keys(), ['GT', 'GQ', 'GQ2'])

  def test_combine_multiple_headers_as_accumulators(self):
    vcf_reader_1 = vcf.Reader(fsock=iter(FILE_1_LINES))
    vcf_reader_2 = vcf.Reader(fsock=iter(FILE_2_LINES))
    headers_1 = self._get_header_from_reader(vcf_reader_1)
    headers_2 = self._get_header_from_reader(vcf_reader_2)

    combiner_fn = merge_headers._MergeHeadersFn(force_merge_conflicts=False)

    merged_headers_1 = combiner_fn.create_accumulator()
    merged_headers_1 = combiner_fn.add_input(merged_headers_1, headers_1)
    merged_headers_2 = combiner_fn.create_accumulator()
    merged_headers_2 = combiner_fn.add_input(merged_headers_2, headers_2)
    merged_headers = combiner_fn.merge_accumulators([merged_headers_1,
                                                     merged_headers_2])
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(merged_headers.infos.keys(), ['NS', 'AF', 'NS2'])
    self.assertItemsEqual(merged_headers.formats.keys(), ['GT', 'GQ', 'GQ2'])

  def test_combine_two_conflicting_headers(self):
    # These two headers have type conflict (Integer vs Float).
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
    headers_1 = self._get_header_from_reader(vcf_reader_1)
    headers_2 = self._get_header_from_reader(vcf_reader_2)

    combiner_fn = merge_headers._MergeHeadersFn(force_merge_conflicts=False)

    with self.assertRaises(ValueError):
      merged_headers = combiner_fn.create_accumulator()
      merged_headers = combiner_fn.add_input(merged_headers, headers_1)
      merged_headers = combiner_fn.add_input(merged_headers, headers_2)
      merged_headers = combiner_fn.extract_output(merged_headers)
      self.fail('Incompatible VCF headers must throw an exception.')

  def test_combine_two_conflicting_headers_force_merge(self):
    # These two headers have type conflict (Integer vs Float), however pipeline
    # doesn't raise error because the type conflict is resolvable and
    # flag '--force_merge_conflicts' is set.
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
    headers_1 = self._get_header_from_reader(vcf_reader_1)
    headers_2 = self._get_header_from_reader(vcf_reader_2)

    combiner_fn = merge_headers._MergeHeadersFn(force_merge_conflicts=True)

    merged_headers = combiner_fn.create_accumulator()
    merged_headers = combiner_fn.add_input(merged_headers, headers_1)
    merged_headers = combiner_fn.add_input(merged_headers, headers_2)
    merged_headers = combiner_fn.extract_output(merged_headers)

    self.assertItemsEqual(merged_headers.infos.keys(), ['NS'])
    self.assertItemsEqual(merged_headers.infos['NS'],
                          OrderedDict([('id', 'NS'),
                                       ('num', 1),
                                       ('type', 'Float'),
                                       ('desc', 'Number samples'),
                                       ('source', None),
                                       ('version', None)]))


  def test_combine_two_headers_with_bad_conflict(self):
    # Type mistmach (String vs Float) cannot be resolved even with
    # flag '--force_merge_conflict' set to True.
    lines_1 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=String,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n']
    lines_2 = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n']

    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    vcf_reader_2 = vcf.Reader(fsock=iter(lines_2))
    headers_1 = self._get_header_from_reader(vcf_reader_1)
    headers_2 = self._get_header_from_reader(vcf_reader_2)

    combiner_fn = merge_headers._MergeHeadersFn(force_merge_conflicts=True)

    with self.assertRaises(ValueError):
      merged_headers = combiner_fn.create_accumulator()
      merged_headers = combiner_fn.add_input(merged_headers, headers_1)
      merged_headers = combiner_fn.add_input(merged_headers, headers_2)
      merged_headers = combiner_fn.extract_output(merged_headers)

  def test_combine_pipeline(self):
    vcf_reader_1 = vcf.Reader(fsock=iter(FILE_1_LINES))
    vcf_reader_2 = vcf.Reader(fsock=iter(FILE_2_LINES))
    headers_1 = self._get_header_from_reader(vcf_reader_1)
    headers_2 = self._get_header_from_reader(vcf_reader_2)
    expected = vcf_header_io.VcfHeader()
    expected.update(headers_1)
    expected.update(headers_2)

    pipeline = TestPipeline()
    merged_headers = (
        pipeline
        | Create([headers_1, headers_2])
        | 'MergeHeaders' >> merge_headers.MergeHeaders(
            force_merge_conflicts=False))

    assert_that(merged_headers, equal_to([expected]))
