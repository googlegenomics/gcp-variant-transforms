# This Python file uses the following encoding: utf-8
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

"""Tests for vcfio module."""

from __future__ import absolute_import

import glob
import gzip
import logging
import os
import tempfile
import unittest

import apache_beam as beam
from apache_beam.io.filesystem import CompressionTypes
import apache_beam.io.source_test_utils as source_test_utils
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from gcp_variant_transforms.testing import asserts
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io.vcfio import _VcfSource as VcfSource
from gcp_variant_transforms.beam_io.vcfio import ReadAllFromVcf
from gcp_variant_transforms.beam_io.vcfio import ReadFromVcf
from gcp_variant_transforms.beam_io.vcfio import Variant
from gcp_variant_transforms.beam_io.vcfio import VariantCall
from gcp_variant_transforms.beam_io.vcfio import SampleNameEncoding
from gcp_variant_transforms.testing import testdata_util
from gcp_variant_transforms.testing.temp_dir import TempDir

# Note: mixing \n and \r\n to verify both behaviors.
_SAMPLE_HEADER_LINES = [
    '##fileformat=VCFv4.2\n',
    '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
    '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\r\n',
    '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">\n',
    '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample1\tSample2\r'
    '\n',
]

_SAMPLE_TEXT_LINES = [
    '20\t14370\t.\tG\tA\t29\tPASS\tAF=0.5\tGT:GQ\t0|0:48\t1|0:48\n',
    '20\t17330\t.\tT\tA\t3\tq10\tAF=0.017\tGT:GQ\t0|0:49\t0|1:3\n',
    '20\t1110696\t.\tA\tG,T\t67\tPASS\tAF=0.3,0.7\tGT:GQ\t1|2:21\t2|1:2\n',
    '20\t1230237\t.\tT\t.\t47\tPASS\t.\tGT:GQ\t0|0:54\t0|0:48\n',
    '19\t1234567\t.\tGTCT\tG,GTACT\t50\tPASS\t.\tGT:GQ\t0/1:35\t0/2:17\n',
    '20\t1234\trs123\tC\tA,T\t50\tPASS\tAF=0.5\tGT:GQ\t0/0:48\t1/0:20\n',
    '19\t123\trs1234\tGTC\t.\t40\tq10;s50\tNS=2\tGT:GQ\t1|0:48\t0/1:.\n',
    '19\t12\t.\tC\t<SYMBOLIC>\t49\tq10\tAF=0.5\tGT:GQ\t0|1:45\t.:.\n'
]

hash_name = testdata_util.hash_name

VCF_LINE_1 = ('20	1234	rs123;rs2	C	A,T	50	'
              'PASS	AF=0.5,0.1;NS=1;SVTYPE=BÑD	GT:GQ	0/0:48	1/0:20\n')
VCF_LINE_2 = '19	123	rs1234	GTC	.	40	q10;s50	NS=2	GT:GQ	.|0:48	0/.:.\n'
VCF_LINE_3 = (
    '19	12	.	C	<SYMBOLIC>	49	q10	AF=0.5	GT:PS:GQ	0|1:1:45	.:.:.\n')
GVCF_LINE = '19	1234	.	C	<NON_REF>	50	.	END=1236	GT:GQ	0/0:99\n'

def _get_hashing_function(file_name, use_hashing):
  def _hash_name_method(sample_name):
    return sample_name if not use_hashing else hash_name(sample_name, file_name)
  return _hash_name_method

def _get_sample_variant_1(file_name='', use_1_based_coordinate=False,
                          use_hashing=True):
  """Get first sample variant.

  Features:
    multiple alternates
    not phased
    multiple names
    utf-8 encoded
  """
  hash_name_method = _get_hashing_function(file_name, use_hashing)
  variant = vcfio.Variant(
      reference_name='20', start=1233 + use_1_based_coordinate, end=1234,
      reference_bases='C', alternate_bases=['A', 'T'], names=['rs123', 'rs2'],
      quality=50, filters=['PASS'],
      info={'AF': [0.5, 0.1], 'NS': 1, 'SVTYPE': ['BÑD']})
  variant.calls.append(
      vcfio.VariantCall(sample_id=hash_name_method('Sample1'), genotype=[0, 0],
                        info={'GQ': 48}))
  variant.calls.append(
      vcfio.VariantCall(sample_id=hash_name_method('Sample2'), genotype=[1, 0],
                        info={'GQ': 20}))

  return variant

def _get_sample_variant_2(file_name='', use_1_based_coordinate=False,
                          use_hashing=True):
  """Get second sample variant.

  Features:
    multiple references
    no alternate
    phased
    multiple filters
    missing format field
  """
  hash_name_method = _get_hashing_function(file_name, use_hashing)
  variant = vcfio.Variant(
      reference_name='19',
      start=122 + use_1_based_coordinate, end=125, reference_bases='GTC',
      alternate_bases=[], names=['rs1234'], quality=40,
      filters=['q10', 's50'], info={'NS': 2})
  variant.calls.append(
      vcfio.VariantCall(sample_id=hash_name_method('Sample1'), genotype=[-1, 0],
                        phaseset=vcfio.DEFAULT_PHASESET_VALUE, info={'GQ': 48}))
  variant.calls.append(
      vcfio.VariantCall(sample_id=hash_name_method('Sample2'), genotype=[0, -1],
                        info={'GQ': None}))
  return variant


def _get_sample_variant_3(file_name='', use_1_based_coordinate=False,
                          use_hashing=True):
  """Get third sample variant.

  Features:
    symbolic alternate
    no calls for sample 2
    alternate phaseset
  """
  hash_name_method = _get_hashing_function(file_name, use_hashing)
  variant = vcfio.Variant(
      reference_name='19', start=11 + use_1_based_coordinate, end=12,
      reference_bases='C', alternate_bases=['<SYMBOLIC>'], quality=49,
      filters=['q10'], info={'AF': [0.5]})
  variant.calls.append(
      vcfio.VariantCall(sample_id=hash_name_method('Sample1'), genotype=[0, 1],
                        phaseset='1', info={'GQ': 45}))
  variant.calls.append(
      vcfio.VariantCall(sample_id=hash_name_method('Sample2'),
                        genotype=[vcfio.MISSING_GENOTYPE_VALUE],
                        info={'GQ': None}))
  return variant


def _get_sample_non_variant(use_1_based_coordinate=False):
  """Get sample non variant."""
  non_variant = vcfio.Variant(
      reference_name='19', start=1233 + use_1_based_coordinate, end=1236,
      reference_bases='C', alternate_bases=['<NON_REF>'], quality=50)
  non_variant.calls.append(
      vcfio.VariantCall(sample_id=hash_name('Sample1'), genotype=[0, 0],
                        info={'GQ': 99}))

  return non_variant


class VcfSourceTest(unittest.TestCase):

  VCF_FILE_DIR_MISSING = not os.path.exists(testdata_util.get_full_dir())

  def _create_temp_vcf_file(
      self, lines, tempdir, compression_type=CompressionTypes.UNCOMPRESSED):
    if compression_type in (CompressionTypes.UNCOMPRESSED,
                            CompressionTypes.AUTO):
      suffix = '.vcf'
    elif compression_type == CompressionTypes.GZIP:
      suffix = '.vcf.gz'
    elif compression_type == CompressionTypes.BZIP2:
      suffix = '.vcf.bz2'
    else:
      raise ValueError('Unrecognized compression type {}'.format(
          compression_type))
    return tempdir.create_temp_file(
        suffix=suffix, lines=lines, compression_type=compression_type)

  def _read_records(self, file_or_pattern, representative_header_lines=None,
                    sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH,
                    **kwargs):
    return source_test_utils.read_from_source(
        VcfSource(file_or_pattern,
                  representative_header_lines=representative_header_lines,
                  sample_name_encoding=sample_name_encoding,
                  **kwargs))

  def _create_temp_file_and_read_records(
      self, lines, representative_header_lines=None,
      sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH):
    return self._create_temp_file_and_return_records_with_file_name(
        lines, representative_header_lines, sample_name_encoding)[0]

  def _create_temp_file_and_return_records_with_file_name(
      self, lines, representative_header_lines=None,
      sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH):
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file(suffix='.vcf', lines=lines)
      return (self._read_records(file_name, representative_header_lines,
                                 sample_name_encoding), file_name)

  def _assert_variants_equal(self, actual, expected):
    self.assertEqual(
        sorted(expected),
        sorted(actual))

  def _get_invalid_file_contents(self):
    """Gets sample invalid files contents.

    Returns:
       A `tuple` where the first element is contents that are invalid because
       of record errors and the second element is contents that are invalid
       because of header errors.
    """
    malformed_vcf_records = [
        # POS should be an integer.
        [
            '##FILTER=<ID=PASS,Description="All filters passed">\n',
            '##FILTER=<ID=q10,Description="Quality is less than 10.">\n',
            '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample\n',
            '19\tabc\trs12345\tT\tC\t9\tq10\tAF=0.2;NS=2\tGT:GQ\t1|0:48\n',
        ]
    ]

    return malformed_vcf_records #, malformed_header_lines

  def _assert_pipeline_read_files_record_count_equal(
      self, input_pattern, expected_count, use_read_all=False):
    """Helper method for verifying total records read.

    Args:
      input_pattern (str): Input file pattern to read.
      expected_count (int): Expected number of reacords that was read.
      use_read_all (bool): Whether to use the scalable ReadAllFromVcf transform
        instead of ReadFromVcf.
    """
    pipeline = TestPipeline()
    if use_read_all:
      pcoll = (pipeline
               | 'Create' >> beam.Create([input_pattern])
               | 'Read' >> ReadAllFromVcf())
    else:
      pcoll = pipeline | 'Read' >> ReadFromVcf(input_pattern)
    assert_that(pcoll, asserts.count_equals_to(expected_count))
    pipeline.run()

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_read_single_file_large(self):
    test_data_conifgs = [
        {'file': 'valid-4.0.vcf', 'num_records': 5},
        {'file': 'valid-4.0.vcf.gz', 'num_records': 5},
        {'file': 'valid-4.0.vcf.bz2', 'num_records': 5},
        {'file': 'valid-4.1-large.vcf', 'num_records': 9882},
        {'file': 'valid-4.2.vcf', 'num_records': 13},
    ]
    for config in test_data_conifgs:
      read_data = self._read_records(
          testdata_util.get_full_file_path(config['file']))
      self.assertEqual(config['num_records'], len(read_data))

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_read_file_pattern_large(self):
    read_data = self._read_records(
        os.path.join(testdata_util.get_full_dir(), 'valid-*.vcf'))
    self.assertEqual(9900, len(read_data))
    read_data_gz = self._read_records(
        os.path.join(testdata_util.get_full_dir(), 'valid-*.vcf.gz'))
    self.assertEqual(9900, len(read_data_gz))

  def test_single_file_no_records(self):
    for content in [[''], [' '], ['', ' ', '\n'], ['\n', '\r\n', '\n']]:
      self.assertEqual([], self._create_temp_file_and_read_records(
          content, _SAMPLE_HEADER_LINES))

  def test_single_file_1_based_verify_details(self):
    variant = _get_sample_variant_1(use_1_based_coordinate=True)
    read_data = None
    with TempDir() as tempdir:
      file_name = tempdir.create_temp_file(
          suffix='.vcf', lines=_SAMPLE_HEADER_LINES + [VCF_LINE_1])
      read_data = source_test_utils.read_from_source(
          VcfSource(file_name,
                    representative_header_lines=None,
                    sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH,
                    use_1_based_coordinate=True))

    self.assertEqual(1, len(read_data))
    self.assertEqual(variant, read_data[0])

  def test_file_pattern_1_based_verify_details(self):
    variant_1 = _get_sample_variant_1(use_1_based_coordinate=True)
    variant_2 = _get_sample_variant_2(use_1_based_coordinate=True)
    variant_3 = _get_sample_variant_3(use_1_based_coordinate=True)
    with TempDir() as tempdir:
      _ = tempdir.create_temp_file(
          suffix='.vcf', lines=_SAMPLE_HEADER_LINES + [VCF_LINE_1])
      _ = tempdir.create_temp_file(
          suffix='.vcf', lines=_SAMPLE_HEADER_LINES + [VCF_LINE_2, VCF_LINE_3])
      read_data = source_test_utils.read_from_source(
          VcfSource(os.path.join(tempdir.get_path(), '*.vcf'),
                    representative_header_lines=None,
                    sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH,
                    use_1_based_coordinate=True))
      self.assertEqual(3, len(read_data))
      self._assert_variants_equal([variant_1, variant_2, variant_3], read_data)

  def test_single_file_verify_details(self):
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [VCF_LINE_1])
    variant_1 = _get_sample_variant_1()
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [VCF_LINE_1])
    self.assertEqual(1, len(read_data))
    self.assertEqual(variant_1, read_data[0])

    variant_2 = _get_sample_variant_2()
    variant_3 = _get_sample_variant_3()
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [VCF_LINE_1, VCF_LINE_2, VCF_LINE_3])
    self.assertEqual(3, len(read_data))
    self._assert_variants_equal([variant_1, variant_2, variant_3], read_data)

  def test_file_pattern_verify_details(self):
    variant_1 = _get_sample_variant_1()
    variant_2 = _get_sample_variant_2()
    variant_3 = _get_sample_variant_3()
    with TempDir() as tempdir:
      self._create_temp_vcf_file(_SAMPLE_HEADER_LINES + [VCF_LINE_1], tempdir)
      self._create_temp_vcf_file((_SAMPLE_HEADER_LINES +
                                  [VCF_LINE_2, VCF_LINE_3]),
                                 tempdir)
      read_data = self._read_records(os.path.join(tempdir.get_path(), '*.vcf'))
      self.assertEqual(3, len(read_data))
      self._assert_variants_equal([variant_1, variant_2, variant_3], read_data)

  def test_single_file_verify_details_encoded_sample_name_without_file(self):
    variant_1 = _get_sample_variant_1()
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [VCF_LINE_1],
        sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH)
    self.assertEqual(1, len(read_data))
    self.assertEqual(variant_1, read_data[0])

    variant_2 = _get_sample_variant_2()
    variant_3 = _get_sample_variant_3()
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [VCF_LINE_1, VCF_LINE_2, VCF_LINE_3],
        sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH)
    self.assertEqual(3, len(read_data))
    self._assert_variants_equal([variant_1, variant_2, variant_3], read_data)

  def test_single_file_verify_details_encoded_sample_name_with_file(self):
    read_data, file_name = (
        self._create_temp_file_and_return_records_with_file_name(
            _SAMPLE_HEADER_LINES + [VCF_LINE_1],
            sample_name_encoding=SampleNameEncoding.WITH_FILE_PATH))

    variant_1 = _get_sample_variant_1(file_name)
    self.assertEqual(1, len(read_data))
    self.assertEqual(variant_1, read_data[0])
    read_data, file_name = (
        self._create_temp_file_and_return_records_with_file_name(
            _SAMPLE_HEADER_LINES + [VCF_LINE_1, VCF_LINE_2, VCF_LINE_3],
            sample_name_encoding=SampleNameEncoding.WITH_FILE_PATH))

    variant_1 = _get_sample_variant_1(file_name)
    variant_2 = _get_sample_variant_2(file_name)
    variant_3 = _get_sample_variant_3(file_name)

    self.assertEqual(3, len(read_data))
    self._assert_variants_equal([variant_1, variant_2, variant_3], read_data)

  def test_single_file_verify_details_without_encoding(self):
    read_data, file_name = (
        self._create_temp_file_and_return_records_with_file_name(
            _SAMPLE_HEADER_LINES + [VCF_LINE_1],
            sample_name_encoding=SampleNameEncoding.NONE))

    variant_1 = _get_sample_variant_1(file_name='', use_hashing=False)
    self.assertEqual(1, len(read_data))
    self.assertEqual(variant_1, read_data[0])
    read_data, file_name = (
        self._create_temp_file_and_return_records_with_file_name(
            _SAMPLE_HEADER_LINES + [VCF_LINE_1, VCF_LINE_2, VCF_LINE_3],
            sample_name_encoding=SampleNameEncoding.NONE))

    variant_1 = _get_sample_variant_1(file_name='', use_hashing=False)
    variant_2 = _get_sample_variant_2(file_name='Name1', use_hashing=False)
    variant_3 = _get_sample_variant_3(file_name=file_name, use_hashing=False)

    self.assertEqual(3, len(read_data))
    self._assert_variants_equal([variant_1, variant_2, variant_3], read_data)


  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_read_after_splitting(self):
    file_name = testdata_util.get_full_file_path('valid-4.1-large.vcf')
    source = VcfSource(file_name)
    splits = [p for p in source.split(desired_bundle_size=500)]
    self.assertGreater(len(splits), 1)
    sources_info = ([
        (split.source, split.start_position, split.stop_position) for
        split in splits])
    self.assertGreater(len(sources_info), 1)
    split_records = []
    for source_info in sources_info:
      split_records.extend(source_test_utils.read_from_source(*source_info))
    self.assertEqual(9882, len(split_records))

  def test_invalid_file(self):
    invalid_file_contents = self._get_invalid_file_contents()

    for content in invalid_file_contents:
      with TempDir() as tempdir, self.assertRaises(ValueError):
        self._read_records(self._create_temp_vcf_file(content, tempdir))
        self.fail('Invalid VCF file must throw an exception')
    # Try with multiple files (any one of them will throw an exception).

    with TempDir() as tempdir, self.assertRaises(ValueError):
      for content in invalid_file_contents:
        self._create_temp_vcf_file(content, tempdir)
        self._read_records(os.path.join(tempdir.get_path(), '*.vcf'))

  def test_allow_malformed_records(self):
    invalid_records = self._get_invalid_file_contents()

    # Invalid records should not raise errors
    for content in invalid_records:
      with TempDir() as tempdir:
        self._read_records(self._create_temp_vcf_file(content, tempdir),
                           allow_malformed_records=True)

  def test_no_samples(self):
    header_line = '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n'
    record_line = '19	123	.	G	A	.	PASS	AF=0.2'
    expected_variant = Variant(
        reference_name='19', start=122, end=123, reference_bases='G',
        alternate_bases=['A'], filters=['PASS'], info={'AF': [0.2]})
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES[:-1] + [header_line, record_line])
    self.assertEqual(1, len(read_data))
    self.assertEqual(expected_variant, read_data[0])

  def test_no_info(self):
    record_line = 'chr19	123	.	.	.	.	.	.	GT	.	.'
    expected_variant = Variant(reference_name='chr19', start=122, end=123)
    expected_variant.calls.append(
        VariantCall(sample_id=hash_name('Sample1'),
                    genotype=[vcfio.MISSING_GENOTYPE_VALUE]))
    expected_variant.calls.append(
        VariantCall(sample_id=hash_name('Sample2'),
                    genotype=[vcfio.MISSING_GENOTYPE_VALUE]))
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [record_line])
    self.assertEqual(1, len(read_data))
    self.assertEqual(expected_variant, read_data[0])

  def test_info_numbers_and_types(self):
    info_headers = [
        '##INFO=<ID=HA,Number=A,Type=String,Description="StringInfo_A">\n',
        '##INFO=<ID=HG,Number=G,Type=Integer,Description="IntInfo_G">\n',
        '##INFO=<ID=HR,Number=R,Type=Character,Description="ChrInfo_R">\n',
        '##INFO=<ID=HF,Number=0,Type=Flag,Description="FlagInfo">\n',
        '##INFO=<ID=HU,Number=.,Type=Float,Description="FloatInfo_variable">\n']
    record_lines = [
        '19	2	.	A	T,C	.	.	HA=a1,a2;HG=1,2,3;HR=a,b,c;HF;HU=0.1	GT	1/0	0/1\n',
        '19	124	.	A	T	.	.	HG=3,4,5;HR=d,e;HU=1.1,1.2	GT	0/0	0/1']
    variant_1 = Variant(
        reference_name='19', start=1, end=2, reference_bases='A',
        alternate_bases=['T', 'C'],
        info={'HA': ['a1', 'a2'], 'HG': [1, 2, 3], 'HR': ['a', 'b', 'c'],
              'HF': True, 'HU': [0.1]})
    variant_1.calls.append(VariantCall(sample_id=hash_name('Sample1'),
                                       genotype=[1, 0]))
    variant_1.calls.append(VariantCall(sample_id=hash_name('Sample2'),
                                       genotype=[0, 1]))
    variant_2 = Variant(
        reference_name='19', start=123, end=124, reference_bases='A',
        alternate_bases=['T'],
        info={'HG': [3, 4, 5], 'HR': ['d', 'e'], 'HU': [1.1, 1.2]})
    variant_2.calls.append(VariantCall(sample_id=hash_name('Sample1'),
                                       genotype=[0, 0]))
    variant_2.calls.append(VariantCall(sample_id=hash_name('Sample2'),
                                       genotype=[0, 1]))
    read_data = self._create_temp_file_and_read_records(
        info_headers + _SAMPLE_HEADER_LINES[1:] + record_lines)
    self.assertEqual(2, len(read_data))
    self._assert_variants_equal([variant_1, variant_2], read_data)

  def test_use_of_representative_header(self):
    # Info field `HU` is defined as Float in file header while data is String.
    # This results in parser failure. We test if parser completes successfully
    # when a representative headers with String definition for field `HU` is
    # given.
    file_content = [
        '##INFO=<ID=HU,Number=.,Type=Float,Description="Info">\n',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\r\n',
        '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2\r\n',
        '19	2	.	A	T	.	.	HU=a,b	GT	0/0	0/1\n',]
    representative_header_lines = [
        '##INFO=<ID=HU,Number=.,Type=String,Description="Info">\n',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\r\n',]
    variant = Variant(
        reference_name='19', start=1, end=2, reference_bases='A',
        alternate_bases=['T'], info={'HU': ['a', 'b']})
    variant.calls.append(VariantCall(sample_id=hash_name('Sample1'),
                                     genotype=[0, 0]))
    variant.calls.append(VariantCall(sample_id=hash_name('Sample2'),
                                     genotype=[0, 1]))

    # `file_headers` is used.
    read_data = self._create_temp_file_and_read_records(file_content)
    # Pysam expects Float value for HU, and returns Nones when list is given.
    self.assertEqual([None, None], read_data[0].info['HU'])

    # `representative_header` is used.
    read_data = self._create_temp_file_and_read_records(
        file_content, representative_header_lines)
    self.assertEqual(1, len(read_data))
    self._assert_variants_equal([variant], read_data)

  def test_use_of_representative_header_two_files(self):
    # Info field `HU` is defined as Float in file header while data is String.
    # This results in parser failure. We test if parser completes successfully
    # when a representative headers with String definition for field `HU` is
    # given.
    file_content_1 = [
        '##INFO=<ID=HU,Number=.,Type=Float,Descri\n',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\r\n',
        '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample1\r\n',
        '9\t2\t.\tA\tT\t.\t.\tHU=a,b\tGT\t0/0']
    file_content_2 = [
        '##INFO=<ID=HU,Number=.,Type=Float,Descri\n',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\r\n',
        '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSample2\r\n',
        '19\t2\t.\tA\tT\t.\t.\tHU=a,b\tGT\t0/1\n',]
    representative_header_lines = [
        '##INFO=<ID=HU,Number=.,Type=String,Description="Info">\n',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\r\n',]

    variant_1 = Variant(
        reference_name='9', start=1, end=2, reference_bases='A',
        alternate_bases=['T'], info={'HU': ['a', 'b']})
    variant_1.calls.append(VariantCall(sample_id=hash_name('Sample1'),
                                       genotype=[0, 0]))

    variant_2 = Variant(
        reference_name='19', start=1, end=2, reference_bases='A',
        alternate_bases=['T'], info={'HU': ['a', 'b']})
    variant_2.calls.append(VariantCall(sample_id=hash_name('Sample2'),
                                       genotype=[0, 1]))

    read_data_1 = self._create_temp_file_and_read_records(
        file_content_1, representative_header_lines)
    self.assertEqual(1, len(read_data_1))
    self._assert_variants_equal([variant_1], read_data_1)

    read_data_2 = self._create_temp_file_and_read_records(
        file_content_2, representative_header_lines)
    self.assertEqual(1, len(read_data_2))
    self._assert_variants_equal([variant_2], read_data_2)

  def test_end_info_key(self):
    end_info_header_line = (
        '##INFO=<ID=END,Number=1,Type=Integer,Description="End of record.">\n')
    record_lines = ['19	123	.	A	T	.	.	END=1111	GT	1/0	0/1\n',
                    '19	123	.	A	T	.	.	.	GT	0/1	1/1\n']
    variant_1 = Variant(
        reference_name='19', start=122, end=1111, reference_bases='A',
        alternate_bases=['T'])
    variant_1.calls.append(VariantCall(sample_id=hash_name('Sample1'),
                                       genotype=[1, 0]))
    variant_1.calls.append(VariantCall(sample_id=hash_name('Sample2'),
                                       genotype=[0, 1]))
    variant_2 = Variant(
        reference_name='19', start=122, end=123, reference_bases='A',
        alternate_bases=['T'])
    variant_2.calls.append(VariantCall(sample_id=hash_name('Sample1'),
                                       genotype=[0, 1]))
    variant_2.calls.append(VariantCall(sample_id=hash_name('Sample2'),
                                       genotype=[1, 1]))
    read_data = self._create_temp_file_and_read_records(
        [end_info_header_line] + _SAMPLE_HEADER_LINES[1:] + record_lines)
    self.assertEqual(2, len(read_data))
    self._assert_variants_equal([variant_1, variant_2], read_data)

  def test_end_info_key_unknown_number(self):
    end_info_header_line = (
        '##INFO=<ID=END,Number=.,Type=Integer,Description="End of record.">\n')
    record_lines = ['19	123	.	A	T	.	.	END=1111	GT	1/0	0/1\n']
    variant_1 = Variant(
        reference_name='19', start=122, end=1111, reference_bases='A',
        alternate_bases=['T'])
    variant_1.calls.append(VariantCall(sample_id=hash_name('Sample1'),
                                       genotype=[1, 0]))
    variant_1.calls.append(VariantCall(sample_id=hash_name('Sample2'),
                                       genotype=[0, 1]))
    read_data = self._create_temp_file_and_read_records(
        [end_info_header_line] + _SAMPLE_HEADER_LINES[1:] + record_lines)
    self.assertEqual(1, len(read_data))
    self._assert_variants_equal([variant_1], read_data)

  def test_end_info_key_unknown_number_invalid(self):
    end_info_header_line = (
        '##INFO=<ID=END,Number=.,Type=Integer,Description="End of record.">\n')
    # PySam should only take first END field.
    variant = Variant(
        reference_name='19', start=122, end=150, reference_bases='A',
        alternate_bases=['T'])
    variant.calls.append(VariantCall(sample_id=hash_name('Sample1'),
                                     genotype=[1, 0]))
    variant.calls.append(VariantCall(sample_id=hash_name('Sample2'),
                                     genotype=[0, 1]))
    read_data = self._create_temp_file_and_read_records(
        [end_info_header_line] + _SAMPLE_HEADER_LINES[1:] +
        ['19	123	.	A	T	.	.	END=150,160	GT	1/0	0/1\n'])

    self.assertEqual(1, len(read_data))
    self._assert_variants_equal([variant], read_data)

    # END should be rounded down.
    read_data = self._create_temp_file_and_read_records(
        [end_info_header_line] + _SAMPLE_HEADER_LINES[1:] +
        ['19	123	.	A	T	.	.	END=150.9	GT	1/0	0/1\n'])

    self.assertEqual(1, len(read_data))
    self._assert_variants_equal([variant], read_data)

    # END should not be a string.
    with self.assertRaises(ValueError):
      self._create_temp_file_and_read_records(
          [end_info_header_line] + _SAMPLE_HEADER_LINES[1:] +
          ['19	123	.	A	T	.	.	END=text	GT	1/0	0/1\n'])

  def test_custom_phaseset(self):
    phaseset_header_line = (
        '##FORMAT=<ID=PS,Number=1,Type=Integer,Description="Phaseset">\n')
    record_lines = ['19	123	.	A	T	.	.	.	GT:PS	1|0:1111	0/1:.\n',
                    '19	121	.	A	T	.	.	.	GT:PS	1|0:2222	0/1:2222\n']
    variant_1 = Variant(
        reference_name='19', start=122, end=123, reference_bases='A',
        alternate_bases=['T'])
    variant_1.calls.append(
        VariantCall(sample_id=hash_name('Sample1'), genotype=[1, 0],
                    phaseset='1111'))
    variant_1.calls.append(VariantCall(sample_id=hash_name('Sample2'),
                                       genotype=[0, 1]))
    variant_2 = Variant(
        reference_name='19', start=120, end=121, reference_bases='A',
        alternate_bases=['T'])
    variant_2.calls.append(
        VariantCall(sample_id=hash_name('Sample1'), genotype=[1, 0],
                    phaseset='2222'))
    variant_2.calls.append(
        VariantCall(sample_id=hash_name('Sample2'), genotype=[0, 1],
                    phaseset='2222'))
    read_data = self._create_temp_file_and_read_records(
        [phaseset_header_line] + _SAMPLE_HEADER_LINES[1:] + record_lines)
    self.assertEqual(2, len(read_data))
    self._assert_variants_equal([variant_1, variant_2], read_data)

  def test_format_numbers(self):
    format_headers = [
        '##FORMAT=<ID=FU,Number=.,Type=String,Description="Format_variable">\n',
        '##FORMAT=<ID=F1,Number=1,Type=Integer,Description="Format_1">\n',
        '##FORMAT=<ID=F2,Number=2,Type=Character,Description="Format_2">\n',
        '##FORMAT=<ID=AO,Number=A,Type=Integer,Description="Format_3">\n',
        '##FORMAT=<ID=AD,Number=G,Type=Integer,Description="Format_4">\n',]

    record_lines = [
        ('19	2	.	A	T,C	.	.	.	'
         'GT:FU:F1:F2:AO:AD	1/0:a1:3:a,b:1:3,4	'
         '0/1:a2,a3:4:b,c:1,2:3')]
    expected_variant = Variant(
        reference_name='19', start=1, end=2, reference_bases='A',
        alternate_bases=['T', 'C'])
    expected_variant.calls.append(VariantCall(
        sample_id=hash_name('Sample1'),
        genotype=[1, 0],
        info={'FU': ['a1'], 'F1': 3, 'F2': ['a', 'b'], 'AO': [1],
              'AD': [3, 4]}))
    expected_variant.calls.append(VariantCall(
        sample_id=hash_name('Sample2'),
        genotype=[0, 1],
        info={'FU': ['a2', 'a3'], 'F1': 4, 'F2': ['b', 'c'], 'AO': [1, 2],
              'AD':[3]}))
    read_data = self._create_temp_file_and_read_records(
        format_headers + _SAMPLE_HEADER_LINES[1:] + record_lines)
    self.assertEqual(1, len(read_data))
    self.assertEqual(expected_variant, read_data[0])

  def test_pipeline_read_single_file(self):
    with TempDir() as tempdir:
      file_name = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir)
      self._assert_pipeline_read_files_record_count_equal(
          file_name, len(_SAMPLE_TEXT_LINES))

  def test_pipeline_read_all_single_file(self):
    with TempDir() as tempdir:
      file_name = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir)
      self._assert_pipeline_read_files_record_count_equal(
          file_name, len(_SAMPLE_TEXT_LINES), use_read_all=True)

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_pipeline_read_single_file_large(self):
    self._assert_pipeline_read_files_record_count_equal(
        testdata_util.get_full_file_path('valid-4.1-large.vcf'), 9882)

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_pipeline_read_all_single_file_large(self):
    self._assert_pipeline_read_files_record_count_equal(
        testdata_util.get_full_file_path('valid-4.1-large.vcf'), 9882)

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_pipeline_read_file_pattern_large(self):
    self._assert_pipeline_read_files_record_count_equal(
        os.path.join(testdata_util.get_full_dir(), 'valid-*.vcf'), 9900)

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_pipeline_read_all_file_pattern_large(self):
    self._assert_pipeline_read_files_record_count_equal(
        os.path.join(testdata_util.get_full_dir(), 'valid-*.vcf'), 9900)

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_pipeline_read_all_gzip_large(self):
    self._assert_pipeline_read_files_record_count_equal(
        os.path.join(testdata_util.get_full_dir(), 'valid-*.vcf.gz'), 9900,
        use_read_all=True)

  @unittest.skipIf(VCF_FILE_DIR_MISSING, 'VCF test file directory is missing')
  def test_pipeline_read_all_multiple_files_large(self):
    pipeline = TestPipeline()
    pcoll = (pipeline
             | 'Create' >> beam.Create(
                 [testdata_util.get_full_file_path('valid-4.0.vcf'),
                  testdata_util.get_full_file_path('valid-4.1-large.vcf'),
                  testdata_util.get_full_file_path('valid-4.2.vcf')])
             | 'Read' >> ReadAllFromVcf())
    assert_that(pcoll, asserts.count_equals_to(9900))
    pipeline.run()

  def test_pipeline_read_all_gzip(self):
    with TempDir() as tempdir:
      file_name_1 = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir,
          compression_type=CompressionTypes.GZIP)
      file_name_2 = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir,
          compression_type=CompressionTypes.GZIP)
      pipeline = TestPipeline()
      pcoll = (pipeline
               | 'Create' >> beam.Create([file_name_1, file_name_2])
               | 'Read' >> ReadAllFromVcf())
      assert_that(pcoll, asserts.count_equals_to(2 * len(_SAMPLE_TEXT_LINES)))
      pipeline.run()

  def test_pipeline_read_all_bzip2(self):
    with TempDir() as tempdir:
      file_name_1 = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir,
          compression_type=CompressionTypes.BZIP2)
      file_name_2 = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir,
          compression_type=CompressionTypes.BZIP2)
      pipeline = TestPipeline()
      pcoll = (pipeline
               | 'Create' >> beam.Create([file_name_1, file_name_2])
               | 'Read' >> ReadAllFromVcf())
      assert_that(pcoll, asserts.count_equals_to(2 * len(_SAMPLE_TEXT_LINES)))
      pipeline.run()

  def test_pipeline_read_all_multiple_files(self):
    with TempDir() as tempdir:
      file_name_1 = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir)
      file_name_2 = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir)
      pipeline = TestPipeline()
      pcoll = (pipeline
               | 'Create' >> beam.Create([file_name_1, file_name_2])
               | 'Read' >> ReadAllFromVcf())
      assert_that(pcoll, asserts.count_equals_to(2 * len(_SAMPLE_TEXT_LINES)))
      pipeline.run()

  def test_read_reentrant_without_splitting(self):
    with TempDir() as tempdir:
      file_name = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir)
      source = VcfSource(file_name)
      source_test_utils.assert_reentrant_reads_succeed((source, None, None))

  def test_read_reentrant_after_splitting(self):
    with TempDir() as tempdir:
      file_name = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir)
      source = VcfSource(file_name)
      splits = [split for split in source.split(desired_bundle_size=100000)]
      assert len(splits) == 1
      source_test_utils.assert_reentrant_reads_succeed(
          (splits[0].source, splits[0].start_position, splits[0].stop_position))

  def test_dynamic_work_rebalancing(self):
    with TempDir() as tempdir:
      file_name = self._create_temp_vcf_file(
          _SAMPLE_HEADER_LINES + _SAMPLE_TEXT_LINES, tempdir)
      source = VcfSource(file_name)
      splits = [split for split in source.split(desired_bundle_size=100000)]
      assert len(splits) == 1
      source_test_utils.assert_split_at_fraction_exhaustive(
          splits[0].source, splits[0].start_position, splits[0].stop_position)


class VcfSinkTest(unittest.TestCase):

  def setUp(self):
    super(VcfSinkTest, self).setUp()
    self.path = tempfile.NamedTemporaryFile(suffix='.vcf').name
    self.variants, self.variant_lines = zip(
        (_get_sample_variant_1(), VCF_LINE_1),
        (_get_sample_variant_2(), VCF_LINE_2),
        (_get_sample_variant_3(), VCF_LINE_3),
        (_get_sample_non_variant(), GVCF_LINE))

  def _assert_variant_lines_equal(self, actual, expected):
    actual_fields = actual.strip().split('\t')
    expected_fields = expected.strip().split('\t')

    self.assertEqual(len(actual_fields), len(expected_fields))
    self.assertEqual(actual_fields[0], expected_fields[0])
    self.assertEqual(actual_fields[1], expected_fields[1])
    self.assertItemsEqual(actual_fields[2].split(';'),
                          expected_fields[2].split(';'))
    self.assertEqual(actual_fields[3], expected_fields[3])
    self.assertItemsEqual(actual_fields[4].split(','),
                          expected_fields[4].split(','))
    self.assertEqual(actual_fields[5], actual_fields[5])
    self.assertItemsEqual(actual_fields[6].split(';'),
                          expected_fields[6].split(';'))
    self.assertItemsEqual(actual_fields[7].split(';'),
                          expected_fields[7].split(';'))
    self.assertItemsEqual(actual_fields[8].split(':'),
                          expected_fields[8].split(':'))

    # Assert calls are the same
    for call, expected_call in zip(actual_fields[9:], expected_fields[9:]):
      actual_split = call.split(':')
      expected_split = expected_call.split(':')
      # Compare the first and third values of the GT field
      self.assertEqual(actual_split[0], expected_split[0])
      # Compare the rest of the items ignoring order
      self.assertItemsEqual(actual_split[1:], expected_split[1:])

  def _get_coder(self, bq_uses_1_based_coordinate=False):
    return vcfio._ToVcfRecordCoder(bq_uses_1_based_coordinate)

  def test_to_vcf_line_0_based(self):
    coder = self._get_coder()
    for variant, line in zip(self.variants, self.variant_lines):
      self._assert_variant_lines_equal(
          coder.encode(variant), line)
    empty_variant = vcfio.Variant()
    empty_line = '\t'.join(['.' for _ in range(9)])
    self._assert_variant_lines_equal(
        coder.encode(empty_variant), empty_line)

  def test_to_vcf_line_1_based(self):
    coder = self._get_coder(bq_uses_1_based_coordinate=True)
    variants = [
        _get_sample_variant_1(use_1_based_coordinate=True),
        _get_sample_variant_2(use_1_based_coordinate=True),
        _get_sample_variant_3(use_1_based_coordinate=True),
        _get_sample_non_variant(use_1_based_coordinate=True)]
    for variant, line in zip(variants, self.variant_lines):
      self._assert_variant_lines_equal(
          coder.encode(variant), line)
    empty_variant = vcfio.Variant()
    empty_line = '\t'.join(['.' for _ in range(9)])
    self._assert_variant_lines_equal(
        coder.encode(empty_variant), empty_line)

  def test_missing_info_key(self):
    coder = self._get_coder()
    variant = Variant()
    variant.calls.append(VariantCall(sample_id=hash_name('Sample1'),
                                     genotype=[0, 1],
                                     info={'GQ': 10, 'AF': 20}))
    variant.calls.append(VariantCall(
        sample_id=hash_name('Sample2'), genotype=[0, 1], info={'AF': 20}))
    expected = ('.	.	.	.	.	.	.	.	GT:AF:GQ	0/1:20:10	'
                '0/1:20:.\n')

    self._assert_variant_lines_equal(coder.encode(variant), expected)

  def test_info_list(self):
    coder = self._get_coder()
    variant = Variant()
    variant.calls.append(VariantCall(sample_id=hash_name('Sample'),
                                     genotype=[0, 1],
                                     info={'LI': [1, None, 3]}))
    expected = '.	.	.	.	.	.	.	.	GT:LI	0/1:1,.,3\n'

    self._assert_variant_lines_equal(coder.encode(variant), expected)

  def test_info_field_count(self):
    coder = self._get_coder()
    variant = Variant()
    variant.info['NS'] = 3
    variant.info['AF'] = [0.333, 0.667]
    variant.info['DB'] = True
    variant.info['CSQ'] = ['G|upstream_gene_variant||MODIFIER',
                           'T|||MODIFIER']
    expected = ('.	.	.	.	.	.	.	NS=3;AF=0.333,0.667;DB;'
                'CSQ=G|upstream_gene_variant||MODIFIER,T|||MODIFIER	.\n')

    self._assert_variant_lines_equal(coder.encode(variant), expected)

  def test_empty_sample_calls(self):
    coder = self._get_coder()
    variant = Variant()
    variant.calls.append(
        VariantCall(sample_id=hash_name('Sample2'), genotype=-1))
    expected = '.	.	.	.	.	.	.	.	GT	.\n'
    self._assert_variant_lines_equal(coder.encode(variant), expected)

  def test_missing_genotype(self):
    coder = self._get_coder()
    variant = Variant()
    variant.calls.append(
        VariantCall(sample_id=hash_name('Sample'),
                    genotype=[1, vcfio.MISSING_GENOTYPE_VALUE]))
    expected = '.	.	.	.	.	.	.	.	GT	1/.\n'

    self._assert_variant_lines_equal(coder.encode(variant), expected)

  def test_triploid_genotype(self):
    coder = self._get_coder()
    variant = Variant()
    variant.calls.append(VariantCall(
        sample_id=hash_name('Sample'), genotype=[1, 0, 1]))
    expected = '.	.	.	.	.	.	.	.	GT	1/0/1\n'

    self._assert_variant_lines_equal(coder.encode(variant), expected)

  def test_write_dataflow_0_based(self):
    pipeline = TestPipeline()
    pcoll = pipeline | beam.Create(self.variants, reshuffle=False)
    _ = pcoll | 'Write' >> vcfio.WriteToVcf(
        self.path, bq_uses_1_based_coordinate=False)
    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with open(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    for actual, expected in zip(read_result, self.variant_lines):
      self._assert_variant_lines_equal(actual, expected)

  def test_write_dataflow_1_based(self):
    variants = [
        _get_sample_variant_1(use_1_based_coordinate=True),
        _get_sample_variant_2(use_1_based_coordinate=True),
        _get_sample_variant_3(use_1_based_coordinate=True),
        _get_sample_non_variant(use_1_based_coordinate=True)]
    pipeline = TestPipeline()
    pcoll = pipeline | beam.Create(variants, reshuffle=False)
    _ = pcoll | 'Write' >> vcfio.WriteToVcf(self.path)
    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with open(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    for actual, expected in zip(read_result, self.variant_lines):
      self._assert_variant_lines_equal(actual, expected)

  def test_write_dataflow_auto_compression(self):
    pipeline = TestPipeline()
    pcoll = pipeline | beam.Create(self.variants, reshuffle=False)
    _ = pcoll | 'Write' >> vcfio.WriteToVcf(
        self.path + '.gz',
        compression_type=CompressionTypes.AUTO,
        bq_uses_1_based_coordinate=False)
    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with gzip.GzipFile(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    for actual, expected in zip(read_result, self.variant_lines):
      self._assert_variant_lines_equal(actual, expected)

  def test_write_dataflow_header(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'Create' >> beam.Create(self.variants, reshuffle=False)
    headers = ['foo\n']
    _ = pcoll | 'Write' >> vcfio.WriteToVcf(
        self.path + '.gz',
        compression_type=CompressionTypes.AUTO,
        headers=headers,
        bq_uses_1_based_coordinate=False)
    pipeline.run()

    read_result = []
    for file_name in glob.glob(self.path + '*'):
      with gzip.GzipFile(file_name, 'r') as f:
        read_result.extend(f.read().splitlines())

    self.assertEqual(read_result[0], 'foo')
    for actual, expected in zip(read_result[1:], self.variant_lines):
      self._assert_variant_lines_equal(actual, expected)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
