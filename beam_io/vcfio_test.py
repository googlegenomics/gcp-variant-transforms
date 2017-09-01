"""Tests for vcfio module."""

import logging
import os
import shutil
import tempfile
import unittest

import apache_beam.io.source_test_utils as source_test_utils
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import BeamAssertException

from beam_io.vcfio import _VcfSource as VcfSource
from beam_io.vcfio import ReadFromVcf
from beam_io.vcfio import Variant

from testing import testdata_util

# Note: mixing \n and \r\n to verify both behaviors.
_SAMPLE_HEADER_LINES = [
    '##fileformat=VCFv4.2\n',
    '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
    '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
    '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\r\n',
    '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">\n',
    '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\r\n',
]


class _TestCaseWithTempDirCleanUp(unittest.TestCase):
  """Base class for TestCases that deals with TempDir clean-up.

  Inherited test cases will call self._new_tempdir() to start a temporary dir
  which will be deleted at the end of the tests (when tearDown() is called).
  """

  def setUp(self):
    self._tempdirs = []

  def tearDown(self):
    for path in self._tempdirs:
      if os.path.exists(path):
        shutil.rmtree(path)
    self._tempdirs = []

  def _new_tempdir(self):
    result = tempfile.mkdtemp()
    self._tempdirs.append(result)
    return result

  def _create_temp_file(self, name='', suffix='', tmpdir=None):
    if not name:
      name = tempfile.template
    return tempfile.NamedTemporaryFile(
        delete=False, prefix=name,
        dir=tmpdir or self._new_tempdir(), suffix=suffix)


# Helper method for comparing variants.
def _variant_comparator(v1, v2):
  if v1.reference_name == v2.reference_name:
    if v1.start == v2.start:
      return cmp(v1.end, v2.end)
    return cmp(v1.start, v2.start)
  return cmp(v1.reference_name, v2.reference_name)


# Helper method for verifying equal count on PCollection.
def _count_equals_to(expected_count):
  def _count_equal(actual_list):
    actual_count = len(actual_list)
    if expected_count != actual_count:
      raise BeamAssertException(
          'Expected %d not equal actual %d' % (expected_count, actual_count))
  return _count_equal


class VcfSourceTest(_TestCaseWithTempDirCleanUp):

  def _read_records(self, file_or_pattern):
    source = VcfSource(file_or_pattern)
    range_tracker = source.get_range_tracker(None, None)
    read_data = [record for record in source.read(range_tracker)]
    return read_data

  def _create_temp_vcf_file(self, lines, tmpdir=None):
    with self._create_temp_file(suffix='.vcf', tmpdir=tmpdir) as f:
      for line in lines:
        f.write(line)
    return f.name

  def _create_temp_file_and_read_records(self, lines):
    return self._read_records(self._create_temp_vcf_file(lines))

  def _assert_variants_equal(self, actual, expected):
    self.assertEqual(
        sorted(expected, cmp=_variant_comparator),
        sorted(actual, cmp=_variant_comparator))

  def _get_sample_variant_1(self):
    vcf_line = '20	1234	rs12345	C	A,T	50	PASS	AF=0.5;NS=1	GT:GQ	0/0:48\n'
    variant = Variant(
        reference_name='20', start=1233, end=1234, reference_bases='C',
        alternate_bases=['A', 'T'])
    return variant, vcf_line

  def _get_sample_variant_2(self):
    vcf_line = '19	123	rs12345	GTC	C	50	q10	AF=0.2;NS=2	GT:GQ	1|0:48\n'
    variant = Variant(
        reference_name='19', start=122, end=125, reference_bases='GTC',
        alternate_bases=['C'])
    return variant, vcf_line

  def _get_sample_variant_3(self):
    vcf_line = '19	12	.	C	<SYMBOLIC>	49	q10	AF=0.5;NS=2	GT:GQ	1|1:45\n'
    variant = Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['<SYMBOLIC>'])
    return variant, vcf_line

  def test_read_single_file(self):
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

  def test_read_file_pattern(self):
    read_data = self._read_records(
        os.path.join(testdata_util.get_full_dir(), 'valid-*.vcf'))
    self.assertEqual(9900, len(read_data))

    read_data_gz = self._read_records(
        os.path.join(testdata_util.get_full_dir(), 'valid-*.vcf.gz'))
    self.assertEqual(9900, len(read_data_gz))

  def test_single_file_no_records(self):
    self.assertEqual(
        [], self._create_temp_file_and_read_records(['']))
    self.assertEqual(
        [], self._create_temp_file_and_read_records(['\n', '\r\n', '\n']))
    self.assertEqual(
        [], self._create_temp_file_and_read_records(_SAMPLE_HEADER_LINES))

  def test_single_file_verify_details(self):
    variant_1, vcf_line_1 = self._get_sample_variant_1()
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [vcf_line_1])
    self.assertEqual(1, len(read_data))
    self.assertEqual(variant_1, read_data[0])

    variant_2, vcf_line_2 = self._get_sample_variant_2()
    variant_3, vcf_line_3 = self._get_sample_variant_3()
    read_data = self._create_temp_file_and_read_records(
        _SAMPLE_HEADER_LINES + [vcf_line_1, vcf_line_2, vcf_line_3])
    self.assertEqual(3, len(read_data))
    self._assert_variants_equal([variant_1, variant_2, variant_3], read_data)

  def test_file_pattern_verify_details(self):
    variant_1, vcf_line_1 = self._get_sample_variant_1()
    variant_2, vcf_line_2 = self._get_sample_variant_2()
    variant_3, vcf_line_3 = self._get_sample_variant_3()
    tmpdir = self._new_tempdir()
    self._create_temp_vcf_file(_SAMPLE_HEADER_LINES + [vcf_line_1],
                               tmpdir=tmpdir)
    self._create_temp_vcf_file(_SAMPLE_HEADER_LINES + [vcf_line_2, vcf_line_3],
                               tmpdir=tmpdir)
    read_data = self._read_records(os.path.join(tmpdir, '*.vcf'))
    self.assertEqual(3, len(read_data))
    self._assert_variants_equal([variant_1, variant_2, variant_3], read_data)

  def test_read_after_splitting(self):
    file_name = testdata_util.get_full_file_path('valid-4.1-large.vcf')
    source = VcfSource(file_name)
    splits = [split for split in source.split(desired_bundle_size=500)]
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
    invalid_file_contents = [
        # Malfromed record.
        [
            '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\n',
            '1    1  '
        ],
        # Missing "GT:GQ" format, but GQ is provided.
        [
            '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\n',
            '19	123	rs12345	T	C	50	q10	AF=0.2;NS=2	GT	1|0:48'
        ],
        # Malformed FILTER.
        [
            '##FILTER=<ID=PASS,Description="All filters passed">\n',
            '##FILTER=<ID=LowQual,Descri\n',
            '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\n',
            '19	123	rs12345	T	C	50	q10	AF=0.2;NS=2	GT:GQ	1|0:48',
        ],
        # POS should be an integer.
        [
            '##FILTER=<ID=PASS,Description="All filters passed">\n',
            '##FILTER=<ID=LowQual,Descri\n',
            '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SampleName\n',
            '19	abc	rs12345	T	C	50	q10	AF=0.2;NS=2	GT:GQ	1|0:48\n',
        ],
    ]
    for content in invalid_file_contents:
      try:
        self._read_records(self._create_temp_vcf_file(content))
        self.fail('Invalid VCF file must throw an exception')
      except ValueError:
        pass

    # Try with multiple files (any one of them will throw an exception).
    tmpdir = self._new_tempdir()
    for content in invalid_file_contents:
      self._create_temp_vcf_file(content, tmpdir=tmpdir)
    try:
      self._read_records(os.path.join(tmpdir, '*.vcf'))
      self.fail('Invalid VCF file must throw an exception.')
    except ValueError:
      pass

  def test_pipeline_read_single_file(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromVcf(
        testdata_util.get_full_file_path('valid-4.0.vcf'))
    assert_that(pcoll, _count_equals_to(5))
    pipeline.run()

  def test_pipeline_read_file_pattern(self):
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromVcf(
        os.path.join(testdata_util.get_full_dir(), 'valid-*.vcf'))
    assert_that(pcoll, _count_equals_to(9900))
    pipeline.run()



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
