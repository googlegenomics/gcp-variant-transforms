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

import zlib

from apache_beam.coders import coders
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io import textio
from apache_beam.io.gcp import gcsio


class BGZFSource(textio._TextSource):
  """A source for reading `BGZF` files."""

  def open_file(self, file_name):
    return open_bgzf(file_name)


def open_bgzf(file_name):
  compression_type = filesystems.CompressionTypes.GZIP
  mime_type = filesystems.CompressionTypes.mime_type(compression_type)
  raw_file = gcsio.GcsIO().open(file_name, 'rb', mime_type=mime_type)
  return BGZF(raw_file)


class BGZF(filesystem.CompressedFile):
  """File wrapper for easier handling of BGZF compressed files.

  It supports reading concatenated GZIP files.

  `CompressedFile` reads the file and loads `self._read_size` of data into the
  `buf`, decompress it, and repeats (read and decompress) until there is no more
  contents in the file.
  If it is concatenated GZIP files, the `CompressedFile` fails since the
  decompressor can only decompress one GZIP block, and other GZIP compressed
  data shall be left as `unused_data` in the decompressor. For instance, if
  there are 10 GZIP blocks in the buf, calling `decompress` on the buf only
  decompresses 1 GZIP block, and leaves 9 GZIP blocks in
  `decompressor.unused_data`.

  This class repeatedly fetches the `unused_data` from the decompressor until
  all GZIP blocks are decompressed.
  """

  def _fetch_to_internal_buffer(self, num_bytes):
    """Fetch up to num_bytes into the internal buffer."""
    if (not self._read_eof and self._read_position > 0 and
        (self._read_buffer.tell() - self._read_position) < num_bytes):
      # There aren't enough number of bytes to accommodate a read, so we
      # prepare for a possibly large read by clearing up all internal buffers
      # but without dropping any previous held data.
      self._read_buffer.seek(self._read_position)
      data = self._read_buffer.read()
      self._clear_read_buffer()
      self._read_buffer.write(data)

    while not self._read_eof and (self._read_buffer.tell() - self._read_position
                                 ) < num_bytes:
      self._decompress_decompressor_unused_data(num_bytes)
      if (self._read_buffer.tell() - self._read_position) >= num_bytes:
        return
      buf = self._file.read(self._read_size)
      if buf:
        decompressed = self._decompressor.decompress(buf)
        del buf
        self._read_buffer.write(decompressed)
        self._decompress_decompressor_unused_data(num_bytes)
      else:
        self._read_eof = True

  def _decompress_decompressor_unused_data(self, num_bytes):
    while (self._decompressor.unused_data != b'' and
           self._read_buffer.tell() - self._read_position < num_bytes):
      buf = self._decompressor.unused_data
      self._decompressor = zlib.decompressobj(self._gzip_mask)
      decompressed = self._decompressor.decompress(buf)
      del buf
      self._read_buffer.write(decompressed)


class BGZFBlockSource(textio._TextSource):

  def __init__(self,
               file_name,
               block,
               header_lines,
               compression_type,
               header_processor_fns,
               strip_trailing_newlines=True,
               min_bundle_size=0,
               coder=coders.StrUtf8Coder(),
               validate=True
              ):
    """A source for reading BGZF Block."""
    super(BGZFBlockSource, self).__init__(
        file_name,
        min_bundle_size,
        compression_type,
        strip_trailing_newlines,
        coder,
        validate=validate,
        header_processor_fns=header_processor_fns)
    self._block = block
    self._header_lines = header_lines

  def open_file(self, file_name):
    compression_type = filesystem.CompressionTypes.GZIP
    mime_type = filesystem.CompressionTypes.mime_type(compression_type)
    raw_file = gcsio.GcsIO().open(file_name, 'rb', mime_type=mime_type)
    return BGZFBlock(raw_file, self._block)

  def read_records(self, file_name, _):
    read_buffer = textio._TextSource.ReadBuffer(b'', 0)
    # Processes the header. It appends the header line `#CHROM...` (which
    # contains sample info that is unique for `file_name`) to `header_lines`.
    with open_bgzf(file_name) as file_to_read:
      self._process_header(file_to_read, read_buffer)
    with self.open_file(file_name) as file_to_read:
      while True:
        record = file_to_read.readline()
        if not record or not record.strip():
          break
        if record and not record.startswith('#'):
          yield self._coder.decode(record)


class BGZFBlock(filesystem.CompressedFile):
  """File wrapper to handle one BGZF Block."""

  # Each block in BGZF is no larger than `_MAX_GZIP_SIZE`.
  _MAX_GZIP_SIZE = 64 * 1024

  def __init__(self,
               fileobj,
               block,
               compression_type=filesystem.CompressionTypes.GZIP):
    super(BGZFBlock, self).__init__(fileobj,
                                    compression_type)
    self._block = block
    self._start_offset = self._block.start

  def _fetch_to_internal_buffer(self, num_bytes):
    """Fetches up to num_bytes into the internal buffer.

    It reads contents from `self._block`.
    - The string before first `\n` is discarded.
    - Decompress the next 64KB after self._block and reads the first line. It
      assumes one row can at most spans in two Blocks.
    """
    if self._read_eof or self._enough_data_in_buffer(num_bytes):
      return
    if self._is_first_time():
      self._read_first_gzip_block_into_buffer()
    else:
      self._reset_read_buffer_to_accommodate_more_data()
    self._fetch_data_to_read_buffer(num_bytes)
    if self._enough_data_in_buffer(num_bytes):
      return
    self._complete_last_line()
    self._read_eof = True

  def _enough_data_in_buffer(self, num_bytes):
    return self._read_buffer.tell() - self._read_position >= num_bytes

  def _is_first_time(self):
    return self._start_offset == self._block.start

  def _read_first_gzip_block_into_buffer(self):
    buf = self._read_data_from_block()
    decompressed = self._decompressor.decompress(buf)
    del buf
    lines = decompressed.split('\n')
    self._read_buffer.write('\n'.join(lines[1:]))

  def _reset_read_buffer_to_accommodate_more_data(self):
    # There aren't enough number of bytes to accommodate a read, so we
    # prepare for a possibly large read by clearing up all internal buffers
    # but without dropping any previous held data.
    self._read_buffer.seek(self._read_position)
    data = self._read_buffer.read()
    self._clear_read_buffer()
    self._read_buffer.write(data)

  def _fetch_data_to_read_buffer(self, num_bytes):
    # Decompress the data until there are at least `num_bytes` in the buffer.
    while (self._decompressor.unused_data != b'' and
           not self._enough_data_in_buffer(num_bytes)):
      buf = self._decompressor.unused_data
      if (len(buf) < self._MAX_GZIP_SIZE and
          self._start_offset < self._block.end):
        buf = ''.join([buf, self._read_data_from_block()])

      self._decompressor = zlib.decompressobj(self._gzip_mask)
      decompressed = self._decompressor.decompress(buf)
      del buf
      self._read_buffer.write(decompressed)

  def _complete_last_line(self):
    # Fetch the first line in the next `self._MAX_GZIP_SIZE` bytes.
    buf = self._file.raw._downloader.get_range(
        self._block.end, self._block.end + self._MAX_GZIP_SIZE)
    self._decompressor = zlib.decompressobj(self._gzip_mask)
    decompressed = self._decompressor.decompress(buf)
    del buf
    self._read_buffer.write(decompressed.split('\n')[0] + '\n')

  def _read_data_from_block(self):
    buf = self._file.raw._downloader.get_range(self._start_offset,
                                               self._block.end)
    self._start_offset += len(buf)
    return buf
