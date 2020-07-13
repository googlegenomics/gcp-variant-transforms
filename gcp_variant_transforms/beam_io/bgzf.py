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
  """File wrapper for handling of BGZF compressed files.

  It provides support for reading GZIP file and concatenated GZIP files ("block
  gzip" or BGZF files).

  `CompressedFile` periodically reads a fixed amount of data from a file, loads
  the contents into a buffer and decompresses until it reaches the end of the
  GZIP block. Therefore, when handling block-GZIP or concatenated GZIP files,
  only the first block would be processed while the rest of the data will be
  discarded and stored in `decompressor.unused_data` field. For instance, if
  there are 10 GZIP blocks in the buffer, it would only decompresses 1 GZIP
  block, and 9 GZIP blocks will be left in `decompressor.unused_data`.

  This class repeatedly fetches the `unused_data` from the decompressor until
  all GZIP blocks are decompressed.
  """
  def __init__(self,
               fileobj,
               compression_type=filesystem.CompressionTypes.GZIP):
    super(BGZF, self).__init__(fileobj, compression_type)
    self._first_fetch = True

  def _fetch_to_internal_buffer(self, num_bytes):
    """Fetches up to `num_bytes` into the internal buffer."""
    if self._read_eof or self._enough_data_in_buffer(num_bytes):
      return
    self._reset_read_buffer_to_accommodate_more_data()
    self._fetch_and_decompress_data_to_buffer(num_bytes)
    self._first_fetch = False

  def _enough_data_in_buffer(self, num_bytes):
    return self._read_buffer.tell() - self._read_position >= num_bytes

  def _reset_read_buffer_to_accommodate_more_data(self):
    if not self._first_fetch:
     # There aren't enough number of bytes to accommodate a read, so we prepare
     # for a possibly large read by clearing up all internal buffers but without
     # dropping any previous held data.
      self._read_buffer.seek(self._read_position)
      data = self._read_buffer.read()
      self._clear_read_buffer()
      self._read_buffer.write(data)

  def _fetch_and_decompress_data_to_buffer(self, num_bytes):
    while not self._read_eof and not self._enough_data_in_buffer(num_bytes):
      self._decompress_unused_data(num_bytes)
      if self._enough_data_in_buffer(num_bytes):
        return
      buf = self._read_data_from_source()
      if buf:
        decompressed = self._decompressor.decompress(buf)
        del buf
        self._read_buffer.write(decompressed)
      else:
        self._read_eof = True

  def _decompress_unused_data(self, num_bytes):
    while (self._decompressor.unused_data != b'' and
           not self._enough_data_in_buffer(num_bytes)):
      buf = self._decompressor.unused_data
      self._decompressor = zlib.decompressobj(self._gzip_mask)
      decompressed = self._decompressor.decompress(buf)
      del buf
      self._read_buffer.write(decompressed)

  def _read_data_from_source(self):
    return self._file.read(self._read_size)


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


class BGZFBlock(BGZF):
  """File wrapper to handle one BGZF Block.

  It reads contents from `self._block`.
    - The string before first `\n` is discarded.
    - Retrieves the next `self._read_size` (16 MB) after self._block and reads
      the first line. It assumes one row cannot be larger than 16 MB in
      compressed form.
  """
  def __init__(self,
               fileobj,
               block,
               compression_type=filesystem.CompressionTypes.GZIP):
    super(BGZFBlock, self).__init__(fileobj,
                                    compression_type)
    self._block = block
    self._start_offset = self._block.start

  def _fetch_and_decompress_data_to_buffer(self, num_bytes):
    if self._first_fetch:
      self._read_first_gzip_block_into_buffer()
    super(BGZFBlock, self)._fetch_and_decompress_data_to_buffer(num_bytes)
    if self._read_eof:
      self._complete_last_line()

  def _read_first_gzip_block_into_buffer(self):
    buf = self._read_data_from_source()
    decompressed = self._decompressor.decompress(buf)
    del buf
    # Discards all data before first `\n`.
    while b'\n' not in decompressed:
      if self._decompressor.unused_data != b'':
        buf = self._decompressor.unused_data
        self._decompressor = zlib.decompressobj(self._gzip_mask)
        decompressed = self._decompressor.decompress(buf)
        del buf
      else:
        raise ValueError('Read failed. The block {} does not contain any '
                         'valid record.'.format(self._block))

    lines = decompressed.split(b'\n')
    self._read_buffer.write(b'\n'.join(lines[1:]))

  def _read_data_from_source(self):
    if self._start_offset == self._block.end:
      return b''
    buf = self._file.raw._downloader.get_range(self._start_offset,
                                               self._block.end)
    self._start_offset += len(buf)
    return buf

  def _complete_last_line(self):
    # Fetches the first line in the next `self._read_size` bytes.
    buf = self._file.raw._downloader.get_range(
        self._block.end, self._block.end + self._read_size)
    self._decompressor = zlib.decompressobj(self._gzip_mask)
    decompressed = self._decompressor.decompress(buf)
    del buf
    if not decompressed:
      return
    # Writes all data to the buffer until the first `\n` is reached.
    while b'\n' not in decompressed:
      if self._decompressor.unused_data != b'':
        self._read_buffer.write(decompressed)
        buf = self._decompressor.unused_data
        self._decompressor = zlib.decompressobj(self._gzip_mask)
        decompressed = self._decompressor.decompress(buf)
        del buf
      else:
        raise ValueError('Read failed. The record is longer than {} '
                         'bytes.'.format(self._read_size))
    self._read_buffer.write(decompressed.split(b'\n')[0] + b'\n')
