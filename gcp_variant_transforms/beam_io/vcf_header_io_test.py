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

import os
import unittest

import vcf

from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam.io.source_test_utils as source_test_utils
from apache_beam.testing.test_pipeline import TestPipeline
from gcp_variant_transforms.beam_io.vcf_header_io import _VcfHeaderSource
from gcp_variant_transforms.beam_io.vcf_header_io import ReadVcfHeaders
from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeader
from gcp_variant_transforms.testing import temp_dir
from gcp_variant_transforms.testing import testdata_util

_SAMPLE_HEADER_LINES = [
    '##fileformat=VCFv4.2\n',
    '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
    '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\r\n',
    '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">\n',
    '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2\r\n',
]

_SAMPLE_TEXT_LINES = [
    '20	14370	.	G	A	29	PASS	AF=0.5	GT:GQ	0|0:48 1|0:48\n',
    '20	17330	.	T	A	3	q10	AF=0.017	GT:GQ	0|0:49	0|1:3\n',
]


class VcfHeaderSourceTest(unittest.TestCase):

  # TODO(msaul): Replace get_full_dir() with function from utils.
  # Distribution should skip tests that need VCF files due to large size
  VCF_FILE_DIR_MISSING = not os.path.exists(testdata_util.get_full_dir())

  def _get_header_from_reader(self, vcf_reader):
    return VcfHeader(infos=vcf_reader.infos,
                     filters=vcf_reader.filters,
                     alts=vcf_reader.alts,
                     formats=vcf_reader.formats,
                     contigs=vcf_reader.contigs)

  def test_vcf_header_eq(self):
    vcf_reader_1 = vcf.Reader(fsock=iter(_SAMPLE_HEADER_LINES))
    vcf_reader_2 = vcf.Reader(fsock=iter(_SAMPLE_HEADER_LINES))
    header_1 = self._get_header_from_reader(vcf_reader_1)
    header_2 = self._get_header_from_reader(vcf_reader_2)
    self.assertEqual(header_1, header_2)

  def test_read_file_headers(self):
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(
          suffix='.vcf', lines=_SAMPLE_HEADER_LINES+_SAMPLE_TEXT_LINES)

      headers = source_test_utils.read_from_source(_VcfHeaderSource(filename))
      vcf_reader = vcf.Reader(fsock=iter(_SAMPLE_HEADER_LINES))
      self.assertEqual(headers[0], self._get_header_from_reader(vcf_reader))

  def test_all_fields(self):
    lines = [
        '##contig=<ID=M,length=16,assembly=B37,md5=c6,species="Homosapiens">\n',
        '##contig=<ID=P,length=16,assembly=B37,md5=c6,species="Homosapiens">\n',
        '##ALT=<ID=CGA_CNVWIN,Description="Copy number analysis window">\n',
        '##ALT=<ID=INS:ME:MER,Description="Insertion of MER element">\n',
        '##FILTER=<ID=MPCBT,Description="Mate pair count below 10">\n',
        '##INFO=<ID=CGA_MIRB,Number=.,Type=String,Description="miRBaseId">\n',
        '##FORMAT=<ID=FT,Number=1,Type=String,Description="Genotype filter">\n',
        '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	GS000016676-ASM\n',
    ]
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(suffix='.vcf', lines=lines)

      header = source_test_utils.read_from_source(_VcfHeaderSource(filename))[0]
      self.assertItemsEqual(header.contigs.keys(), ['M', 'P'])
      self.assertItemsEqual(header.alts.keys(), ['CGA_CNVWIN', 'INS:ME:MER'])
      self.assertItemsEqual(header.filters.keys(), ['MPCBT'])
      self.assertItemsEqual(header.infos.keys(), ['CGA_MIRB'])
      self.assertItemsEqual(header.formats.keys(), ['FT'])

  def test_empty_header_raises_error(self):
    with temp_dir.TempDir() as tempdir, self.assertRaises(ValueError):
      filename = tempdir.create_temp_file(
          suffix='.vcf', lines=_SAMPLE_TEXT_LINES)

      source_test_utils.read_from_source(_VcfHeaderSource(filename))

  def test_read_file_pattern(self):
    with temp_dir.TempDir() as tempdir:
      headers_1 = [_SAMPLE_HEADER_LINES[1], _SAMPLE_HEADER_LINES[5]]
      headers_2 = [_SAMPLE_HEADER_LINES[2],
                   _SAMPLE_HEADER_LINES[3],
                   _SAMPLE_HEADER_LINES[5]]
      headers_3 = [_SAMPLE_HEADER_LINES[4], _SAMPLE_HEADER_LINES[5]]

      tempdir.create_temp_file(suffix='.vcf', lines=headers_1)
      tempdir.create_temp_file(suffix='.vcf', lines=headers_2)
      tempdir.create_temp_file(suffix='.vcf', lines=headers_3)

      headers = source_test_utils.read_from_source(_VcfHeaderSource(
          os.path.join(tempdir.get_path(), '*.vcf')))

      vcf_reader_1 = vcf.Reader(fsock=iter(headers_1))
      vcf_reader_2 = vcf.Reader(fsock=iter(headers_2))
      vcf_reader_3 = vcf.Reader(fsock=iter(headers_3))

      expected = []
      for reader in [vcf_reader_1, vcf_reader_2, vcf_reader_3]:
        expected.append(self._get_header_from_reader(reader))

      actual_vars = [vars(header) for header in headers]
      expected_vars = [vars(header) for header in expected]
      self.assertItemsEqual(actual_vars, expected_vars)

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
      read_data = source_test_utils.read_from_source(_VcfHeaderSource(
          testdata_util.get_full_file_path(config['file'])))
      self.assertEqual(config['num_infos'], len(read_data[0].infos))
      self.assertEqual(config['num_formats'], len(read_data[0].formats))

  def test_pipeline_read_file_headers(self):
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(
          suffix='.vcf', lines=_SAMPLE_HEADER_LINES+_SAMPLE_TEXT_LINES)

      pipeline = TestPipeline()
      pcoll = pipeline | 'ReadHeaders' >> ReadVcfHeaders(filename)
      vcf_reader = vcf.Reader(fsock=iter(_SAMPLE_HEADER_LINES))
      assert_that(pcoll, equal_to([self._get_header_from_reader(vcf_reader)]))

  def test_pipeline_read_file_pattern(self):
    with temp_dir.TempDir() as tempdir:
      headers_1 = [_SAMPLE_HEADER_LINES[1], _SAMPLE_HEADER_LINES[5]]
      headers_2 = [_SAMPLE_HEADER_LINES[2],
                   _SAMPLE_HEADER_LINES[3],
                   _SAMPLE_HEADER_LINES[5]]
      headers_3 = [_SAMPLE_HEADER_LINES[4], _SAMPLE_HEADER_LINES[5]]

      tempdir.create_temp_file(suffix='.vcf', lines=headers_1)
      tempdir.create_temp_file(suffix='.vcf', lines=headers_2)
      tempdir.create_temp_file(suffix='.vcf', lines=headers_3)
      pipeline = TestPipeline()
      pcoll = pipeline | 'ReadHeaders' >> ReadVcfHeaders(
          os.path.join(tempdir.get_path(), '*.vcf'))

      vcf_reader_1 = vcf.Reader(fsock=iter(headers_1))
      vcf_reader_2 = vcf.Reader(fsock=iter(headers_2))
      vcf_reader_3 = vcf.Reader(fsock=iter(headers_3))
      expected = []
      for reader in [vcf_reader_1, vcf_reader_2, vcf_reader_3]:
        expected.append(self._get_header_from_reader(reader))
      assert_that(pcoll, equal_to(expected))
