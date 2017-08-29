"""Tests for vcfio module."""

import bz2
import gzip
import logging
import os
import shutil
import tempfile
import unittest

from apache_beam import coders
from apache_beam.io.filebasedsource_test import write_data
from apache_beam.io.filebasedsource_test import write_pattern
from apache_beam.io.filesystem import CompressionTypes
import apache_beam.io.source_test_utils as source_test_utils

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from beam_io.vcfio import _TextSource as TextSource
from beam_io.vcfio import ReadFromText


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

  def _create_temp_file(self, name='', suffix=''):
    if not name:
      name = tempfile.template
    file_name = tempfile.NamedTemporaryFile(
        delete=False, prefix=name,
        dir=self._new_tempdir(), suffix=suffix).name
    return file_name


class TextSourceTest(_TestCaseWithTempDirCleanUp):

  # Number of records that will be written by most tests.
  DEFAULT_NUM_RECORDS = 100

  def _run_read_test(self, file_or_pattern, expected_data,
                     buffer_size=DEFAULT_NUM_RECORDS,
                     compression=CompressionTypes.UNCOMPRESSED):
    # Since each record usually takes more than 1 byte, default buffer size is
    # smaller than the total size of the file. This is done to
    # increase test coverage for cases that hit the buffer boundary.
    source = TextSource(file_or_pattern, 0, compression,
                        True, coders.StrUtf8Coder(), buffer_size)
    range_tracker = source.get_range_tracker(None, None)
    read_data = [record for record in source.read(range_tracker)]
    self.assertItemsEqual(expected_data, read_data)

  def test_read_single_file(self):
    file_name, expected_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS)
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    self._run_read_test(file_name, expected_data)

  def test_read_file_pattern(self):
    pattern, expected_data = write_pattern(
        [TextSourceTest.DEFAULT_NUM_RECORDS * 5,
         TextSourceTest.DEFAULT_NUM_RECORDS * 3,
         TextSourceTest.DEFAULT_NUM_RECORDS * 12,
         TextSourceTest.DEFAULT_NUM_RECORDS * 8,
         TextSourceTest.DEFAULT_NUM_RECORDS * 8,
         TextSourceTest.DEFAULT_NUM_RECORDS * 4])
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS * 40
    self._run_read_test(pattern, expected_data)

  def test_read_from_text_file_pattern(self):
    pattern, expected_data = write_pattern([5, 3, 12, 8, 8, 4])
    assert len(expected_data) == 40
    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(pattern)
    assert_that(pcoll, equal_to(expected_data))
    pipeline.run()

  def test_read_auto_bzip2(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file(suffix='.bz2')
    with bz2.BZ2File(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(file_name)
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_auto_gzip(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file(suffix='.gz')

    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(file_name)
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def test_read_bzip2(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file()
    with bz2.BZ2File(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name,
        compression_type=CompressionTypes.BZIP2)
    assert_that(pcoll, equal_to(lines))
    pipeline.run()

  def _remove_lines(self, lines, sublist_lengths, num_to_remove):
    """Utility function to remove num_to_remove lines from each sublist.

    Args:
      lines: list of items.
      sublist_lengths: list of integers representing length of sublist
        corresponding to each source file.
      num_to_remove: number of lines to remove from each sublist.
    Returns:
      remaining lines.
    """
    curr = 0
    result = []
    for offset in sublist_lengths:
      end = curr + offset
      start = min(curr + num_to_remove, end)
      result += lines[start:end]
      curr += offset
    return result

  def _read_skip_header_lines(self, file_or_pattern, skip_header_lines):
    """Simple wrapper function for instantiating TextSource."""
    source = TextSource(
        file_or_pattern,
        0,
        CompressionTypes.UNCOMPRESSED,
        True,
        coders.StrUtf8Coder(),
        skip_header_lines=skip_header_lines)

    range_tracker = source.get_range_tracker(None, None)
    return [record for record in source.read(range_tracker)]

  def test_read_skip_header_single(self):
    file_name, expected_data = write_data(TextSourceTest.DEFAULT_NUM_RECORDS)
    assert len(expected_data) == TextSourceTest.DEFAULT_NUM_RECORDS
    skip_header_lines = 1
    expected_data = self._remove_lines(expected_data,
                                       [TextSourceTest.DEFAULT_NUM_RECORDS],
                                       skip_header_lines)
    read_data = self._read_skip_header_lines(file_name, skip_header_lines)
    self.assertEqual(len(expected_data), len(read_data))
    self.assertItemsEqual(expected_data, read_data)

  def test_read_skip_header_pattern(self):
    line_counts = [
        TextSourceTest.DEFAULT_NUM_RECORDS * 5,
        TextSourceTest.DEFAULT_NUM_RECORDS * 3,
        TextSourceTest.DEFAULT_NUM_RECORDS * 12,
        TextSourceTest.DEFAULT_NUM_RECORDS * 8,
        TextSourceTest.DEFAULT_NUM_RECORDS * 8,
        TextSourceTest.DEFAULT_NUM_RECORDS * 4
    ]
    skip_header_lines = 2
    pattern, data = write_pattern(line_counts)

    expected_data = self._remove_lines(data, line_counts, skip_header_lines)
    read_data = self._read_skip_header_lines(pattern, skip_header_lines)
    self.assertEqual(len(expected_data), len(read_data))
    self.assertItemsEqual(expected_data, read_data)

  def test_read_skip_header_pattern_insufficient_lines(self):
    line_counts = [
        5, 3,  # Fewer lines in file than we want to skip
        12, 8, 8, 4
    ]
    skip_header_lines = 4
    pattern, data = write_pattern(line_counts)

    data = self._remove_lines(data, line_counts, skip_header_lines)
    read_data = self._read_skip_header_lines(pattern, skip_header_lines)
    self.assertEqual(len(data), len(read_data))
    self.assertItemsEqual(data, read_data)

  def test_read_gzip_with_skip_lines(self):
    _, lines = write_data(15)
    file_name = self._create_temp_file()
    with gzip.GzipFile(file_name, 'wb') as f:
      f.write('\n'.join(lines))

    pipeline = TestPipeline()
    pcoll = pipeline | 'Read' >> ReadFromText(
        file_name, 0, CompressionTypes.GZIP,
        True, coders.StrUtf8Coder(), skip_header_lines=2)
    assert_that(pcoll, equal_to(lines[2:]))
    pipeline.run()

  def test_read_after_splitting_skip_header(self):
    file_name, expected_data = write_data(100)
    assert len(expected_data) == 100
    source = TextSource(file_name, 0, CompressionTypes.UNCOMPRESSED, True,
                        coders.StrUtf8Coder(), skip_header_lines=2)
    splits = [split for split in source.split(desired_bundle_size=33)]

    reference_source_info = (source, None, None)
    sources_info = ([
        (split.source, split.start_position, split.stop_position) for
        split in splits])
    self.assertGreater(len(sources_info), 1)
    reference_lines = source_test_utils.read_from_source(*reference_source_info)
    split_lines = []
    for source_info in sources_info:
      split_lines.extend(source_test_utils.read_from_source(*source_info))

    self.assertEqual(expected_data[2:], reference_lines)
    self.assertEqual(reference_lines, split_lines)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
