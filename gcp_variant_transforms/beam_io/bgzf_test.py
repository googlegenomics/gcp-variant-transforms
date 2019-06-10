# Copyright 2019 Google LLC.
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

import unittest

from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io import gcsio
from apache_beam.io.gcp import gcsio_test

from gcp_variant_transforms.testing import testdata_util
from gcp_variant_transforms.beam_io import bgzf
from gcp_variant_transforms.beam_io import bgzf_io


class BgzfBlockTest(unittest.TestCase):

  def setUp(self):
    with open(testdata_util.get_full_file_path('Y.vcf.bgz')) as file_to_read:
      data = file_to_read.readlines()
    self._data = ''.join(data)
    self.client = gcsio_test.FakeGcsClient()
    self.gcs = gcsio.GcsIO(self.client)
    self._file_name = 'gs://bucket/test'
    bucket, name = gcsio.parse_gcs_path(self._file_name)
    self.client.objects.add_file(gcsio_test.FakeFile(bucket, name, self._data,
                                                     1))

  def test_one_gzip_block(self):
    with self._open_bgzf_block(self._file_name,
                               bgzf_io.Block(9287, 18988)) as file_to_read:
      self._validate_first_line_is_complete(file_to_read.readline())
      lines = self._read_all_lines(file_to_read)
      self.assertEqual(len(lines), 43)
      self._validate_last_line_is_complete(lines[-1])

  def test_multiple_gzip_block(self):
    with self._open_bgzf_block(self._file_name,
                               bgzf_io.Block(9287, 37595)) as file_to_read:
      lines = self._read_all_lines(file_to_read)
      self.assertEqual(len(lines), 52)
      self._validate_last_line_is_complete(lines[-1])

  def test_block_size_larger_than_gcs_buffer(self):
    with self._open_bgzf_block(self._file_name,
                               bgzf_io.Block(9287, 949900),
                               read_buffer_size=64 * 1024) as file_to_read:
      file_to_read._read_size = 64 * 1024
      lines = self._read_all_lines(file_to_read)
      self.assertEqual(len(lines), 425)
      self._validate_last_line_is_complete(lines[-1])

  def _open_bgzf_block(self, file_name, block, read_buffer_size=16*1024*1024):
    compression_type = filesystems.CompressionTypes.GZIP
    mime_type = filesystem.CompressionTypes.mime_type(compression_type)
    raw_file = self.gcs.open(file_name, mime_type=mime_type,
                             read_buffer_size=read_buffer_size)
    return bgzf.BGZFBlock(raw_file, block)

  def _read_all_lines(self, file_to_read):
    lines = []
    while True:
      line = file_to_read.readline()
      if not line:
        break
      lines.append(line)
    return lines

  def _validate_first_line_is_complete(self, line):
    self.assertEqual(
        line,
        '##INFO=<ID=non_neuro_AC_popmax,Number=A,Type=Integer,'
        'Description="Allele count in the population with the maximum '
        'AF in the non_neuro subset">\n')

  def _validate_last_line_is_complete(self, line):
    self.assertEqual(line[len(line) - 1], '\n')
