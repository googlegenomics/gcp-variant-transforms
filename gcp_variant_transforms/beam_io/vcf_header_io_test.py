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

"""Tests for vcfio_header_io module."""

import collections
import os
import unittest

import vcf

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam.io.source_test_utils as source_test_utils
from apache_beam.testing.test_pipeline import TestPipeline
from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeaderSource
from gcp_variant_transforms.beam_io.vcf_header_io import ReadAllVcfHeaders
from gcp_variant_transforms.beam_io.vcf_header_io import ReadVcfHeaders
from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeader
from gcp_variant_transforms.beam_io.vcf_header_io import _WriteVcfHeaderFn
from gcp_variant_transforms.beam_io.vcf_header_io import WriteVcfHeaders
from gcp_variant_transforms.testing import asserts
from gcp_variant_transforms.testing import temp_dir
from gcp_variant_transforms.testing import testdata_util


def _get_header_from_reader(vcf_reader):
  return VcfHeader(infos=vcf_reader.infos,
                   filters=vcf_reader.filters,
                   alts=vcf_reader.alts,
                   formats=vcf_reader.formats,
                   contigs=vcf_reader.contigs)


def _get_vcf_header_from_lines(lines):
  return _get_header_from_reader(vcf.Reader(iter(lines)))


class VcfHeaderSourceTest(unittest.TestCase):

  # TODO(msaul): Replace get_full_dir() with function from utils.
  # Distribution should skip tests that need VCF files due to large size
  VCF_FILE_DIR_MISSING = not os.path.exists(testdata_util.get_full_dir())

  def setUp(self):
    self.lines = testdata_util.get_sample_vcf_header_lines()

  def _create_file_and_read_headers(self):
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(suffix='.vcf', lines=self.lines)
      headers = source_test_utils.read_from_source(VcfHeaderSource(filename))
      return headers[0]

  def test_vcf_header_eq(self):
    header_1 = _get_vcf_header_from_lines(self.lines)
    header_2 = _get_vcf_header_from_lines(self.lines)
    self.assertEqual(header_1, header_2)

  def test_read_file_headers(self):
    headers = self.lines
    self.lines = testdata_util.get_sample_vcf_file_lines()
    header = self._create_file_and_read_headers()
    self.assertEqual(header, _get_vcf_header_from_lines(headers))

  def test_all_fields(self):
    self.lines = [
        '##contig=<ID=M,length=16,assembly=B37,md5=c6,species="Homosapiens">\n',
        '##contig=<ID=P,length=16,assembly=B37,md5=c6,species="Homosapiens">\n',
        '##ALT=<ID=CGA_CNVWIN,Description="Copy number analysis window">\n',
        '##ALT=<ID=INS:ME:MER,Description="Insertion of MER element">\n',
        '##FILTER=<ID=MPCBT,Description="Mate pair count below 10">\n',
        '##INFO=<ID=CGA_MIRB,Number=.,Type=String,Description="miRBaseId">\n',
        '##FORMAT=<ID=FT,Number=1,Type=String,Description="Genotype filter">\n',
        '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	GS000016676-ASM\n',
    ]
    header = self._create_file_and_read_headers()
    self.assertItemsEqual(header.contigs.keys(), ['M', 'P'])
    self.assertItemsEqual(header.alts.keys(), ['CGA_CNVWIN', 'INS:ME:MER'])
    self.assertItemsEqual(header.filters.keys(), ['MPCBT'])
    self.assertItemsEqual(header.infos.keys(), ['CGA_MIRB'])
    self.assertItemsEqual(header.formats.keys(), ['FT'])

  def test_empty_header_raises_error(self):
    self.lines = testdata_util.get_sample_vcf_record_lines()
    with self.assertRaises(ValueError):
      self._create_file_and_read_headers()

  def test_read_file_pattern(self):
    with temp_dir.TempDir() as tempdir:
      headers_1 = [self.lines[1], self.lines[-1]]
      headers_2 = [self.lines[2], self.lines[3], self.lines[-1]]
      headers_3 = [self.lines[4], self.lines[-1]]
      tempdir.create_temp_file(suffix='.vcf', lines=headers_1)
      tempdir.create_temp_file(suffix='.vcf', lines=headers_2)
      tempdir.create_temp_file(suffix='.vcf', lines=headers_3)

      actual = source_test_utils.read_from_source(VcfHeaderSource(
          os.path.join(tempdir.get_path(), '*.vcf')))

      expected = [_get_vcf_header_from_lines(h) for h in [headers_1,
                                                          headers_2,
                                                          headers_3]]

      asserts.header_vars_equal(expected)(actual)

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_read_single_file_large(self):
    test_data_conifgs = [
        {'file': 'valid-4.0.vcf', 'num_infos': 6, 'num_formats': 4},
        {'file': 'valid-4.0.vcf.gz', 'num_infos': 6, 'num_formats': 4},
        {'file': 'valid-4.0.vcf.bz2', 'num_infos': 6, 'num_formats': 4},
        {'file': 'valid-4.1-large.vcf', 'num_infos': 21, 'num_formats': 33},
        {'file': 'valid-4.2.vcf', 'num_infos': 8, 'num_formats': 5},
    ]
    for config in test_data_conifgs:
      read_data = source_test_utils.read_from_source(VcfHeaderSource(
          testdata_util.get_full_file_path(config['file'])))
      self.assertEqual(config['num_infos'], len(read_data[0].infos))
      self.assertEqual(config['num_formats'], len(read_data[0].formats))

  def test_pipeline_read_file_headers(self):
    headers = self.lines
    self.lines = testdata_util.get_sample_vcf_file_lines()

    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(suffix='.vcf', lines=self.lines)

      pipeline = TestPipeline()
      pcoll = pipeline | 'ReadHeaders' >> ReadVcfHeaders(filename)

      assert_that(pcoll, equal_to([_get_vcf_header_from_lines(headers)]))
      pipeline.run()

  def test_pipeline_read_all_file_headers(self):
    headers = self.lines
    self.lines = testdata_util.get_sample_vcf_file_lines()

    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(suffix='.vcf', lines=self.lines)

      pipeline = TestPipeline()
      pcoll = (pipeline
               | 'Create' >> beam.Create([filename])
               | 'ReadHeaders' >> ReadAllVcfHeaders())

      assert_that(pcoll, equal_to([_get_vcf_header_from_lines(headers)]))
      pipeline.run()

  def test_pipeline_read_file_pattern(self):
    with temp_dir.TempDir() as tempdir:
      headers_1 = [self.lines[1], self.lines[-1]]
      headers_2 = [self.lines[2], self.lines[3], self.lines[-1]]
      headers_3 = [self.lines[4], self.lines[-1]]

      tempdir.create_temp_file(suffix='.vcf', lines=headers_1)
      tempdir.create_temp_file(suffix='.vcf', lines=headers_2)
      tempdir.create_temp_file(suffix='.vcf', lines=headers_3)

      pipeline = TestPipeline()
      pcoll = pipeline | 'ReadHeaders' >> ReadVcfHeaders(
          os.path.join(tempdir.get_path(), '*.vcf'))

      expected = [_get_vcf_header_from_lines(h) for h in [headers_1,
                                                          headers_2,
                                                          headers_3]]
      assert_that(pcoll, asserts.header_vars_equal(expected))
      pipeline.run()

  def test_pipeline_read_all_file_pattern(self):
    with temp_dir.TempDir() as tempdir:
      headers_1 = [self.lines[1], self.lines[-1]]
      headers_2 = [self.lines[2], self.lines[3], self.lines[-1]]
      headers_3 = [self.lines[4], self.lines[-1]]

      tempdir.create_temp_file(suffix='.vcf', lines=headers_1)
      tempdir.create_temp_file(suffix='.vcf', lines=headers_2)
      tempdir.create_temp_file(suffix='.vcf', lines=headers_3)

      pipeline = TestPipeline()
      pcoll = (pipeline
               | 'Create' >> beam.Create(
                   [os.path.join(tempdir.get_path(), '*.vcf')])
               | 'ReadHeaders' >> ReadAllVcfHeaders())

      expected = [_get_vcf_header_from_lines(h) for h in [headers_1,
                                                          headers_2,
                                                          headers_3]]
      assert_that(pcoll, asserts.header_vars_equal(expected))
      pipeline.run()


class WriteVcfHeadersTest(unittest.TestCase):

  def setUp(self):
    self.lines = testdata_util.get_sample_vcf_header_lines()

  def test_to_vcf_header_line(self):
    header_fn = _WriteVcfHeaderFn('')
    header = collections.OrderedDict([
        ('id', 'NS'),
        ('num', 1),
        ('type', 'Integer'),
        ('desc', 'Number samples'),
    ])

    expected = ('##INFO=<ID=NS,Number=1,Type=Integer,'
                'Description="Number samples">\n')
    self.assertEqual(header_fn._to_vcf_header_line('INFO', header),
                     expected)

  def test_raises_error_for_invalid_key(self):
    header_fn = _WriteVcfHeaderFn('')
    header = collections.OrderedDict([('number', 0)])

    with self.assertRaises(ValueError):
      header_fn._format_header_key_value('number', header['number'])

  def test_raises_error_for_invalid_num(self):
    header_fn = _WriteVcfHeaderFn('')
    header = collections.OrderedDict([('num', -4)])

    with self.assertRaises(ValueError):
      header_fn._format_header_key_value('num', header['num'])

  def test_info_source_and_version(self):
    self.lines = [
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="Total Depth",'
        'Source="source",Version="version">\n',
        self.lines[-1]
    ]
    header = _get_vcf_header_from_lines(self.lines)
    header_fn = _WriteVcfHeaderFn('')
    actual = header_fn._to_vcf_header_line('INFO', header.infos.values()[0])
    expected = self.lines[0]
    self.assertEqual(actual, expected)

  def test_write_contig(self):
    self.lines = [
        '##contig=<ID=M,length=16,assembly=B37,md5=c6,species="Homosapiens">\n',
        self.lines[-1],
    ]
    header = _get_vcf_header_from_lines(self.lines)
    header_fn = _WriteVcfHeaderFn('')
    actual = header_fn._to_vcf_header_line('contig', header.contigs.values()[0])
    expected = '##contig=<ID=M,length=16>\n'
    self.assertEqual(actual, expected)

  def test_write_info_number_types(self):
    self.lines = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
        '##INFO=<ID=HG,Number=G,Type=Integer,Description="IntInfo_G">\n',
        '##INFO=<ID=HR,Number=R,Type=Character,Description="ChrInfo_R">\n',
        self.lines[-1],
    ]
    header = _get_vcf_header_from_lines(self.lines)
    header_fn = _WriteVcfHeaderFn('')
    actual = []
    for info in header.infos.values():
      actual.append(header_fn._to_vcf_header_line('INFO', info))
    expected = self.lines[:-1]
    self.assertItemsEqual(actual, expected)

  def test_write_headers(self):
    header = _get_vcf_header_from_lines(self.lines)
    with temp_dir.TempDir() as tempdir:
      tempfile = tempdir.create_temp_file(suffix='.vcf')
      header_fn = _WriteVcfHeaderFn(tempfile)
      header_fn.process(header)
      self._assert_file_contents_equal(tempfile, self.lines)

  def _remove_sample_names(self, line):
    # Return line with all columns except sample names.
    return '\t'.join(line.split('\t')[:9])

  def _assert_file_contents_equal(self, file_name, lines):
    with open(file_name, 'rb') as f:
      actual = f.read().splitlines()
      expected = [s.strip() for s in lines[1:]]
      expected[-1] = self._remove_sample_names(expected[-1])
      self.assertItemsEqual(actual, expected)

  def test_write_dataflow(self):
    header = _get_vcf_header_from_lines(self.lines)
    with temp_dir.TempDir() as tempdir:
      tempfile = tempdir.create_temp_file(suffix='.vcf')
      pipeline = TestPipeline()
      pcoll = pipeline | beam.core.Create([header])
      _ = pcoll | 'Write' >> WriteVcfHeaders(tempfile)
      pipeline.run()
      self._assert_file_contents_equal(tempfile, self.lines)
