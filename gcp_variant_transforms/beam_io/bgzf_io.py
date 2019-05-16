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

"""Splits the BGZF file."""

import struct
from collections import namedtuple
from typing import Iterable, List, Tuple  # pylint: disable=unused-import

from apache_beam.io import filesystem
from apache_beam.io import filesystems

# `Block` provides real file offset of the start and end of a gzip block. Given
# it, one can directly seek to the start of the gzip block and decompress it.
Block = namedtuple('Block', ['start', 'end'])

_INDEX_FILE_APPENDIX = '.tbi'

# `MAX_BLOCK_SIZE` defines the maximum amount of data included in one `Block`.
MAX_BLOCK_SIZE = 64 * 1024 * 1024


def split_bgzf(file_path):
  # type: (str) -> Iterable[Tuple[str, Tuple[Block, Block]]]
  """Splits BGZF to multiple blocks.

  It reads the `Block` (GZIP block offsets) from the corresponding index file.
  Given (`file_path`, `Block`), one is able to decompress the data within
  `Block`. It further:
  - Removes invalid Blocks.
  - Fills in the gap (by inserting a new Block) if there are gaps between two
    adjacent Blocks to make sure it has full coverage of the variant data.
  - Merges Blocks. It eliminates overlapping Blocks, and merges adjacent Blocks
    if their combined size is smaller than `MAX_BLOCK_SIZE`.

  Args:
    file_path: The BGZF path.

  Yields:
    (file_path, (Block_i, Block_(i+1))). Block_(i+1) can be used to complete the
    last row of Block_i if needed. It assumes one VCF record can at most spans
    in two Blocks.

  Raises:
    ValueError: If a block is larger than `MAX_BLOCK_SIZE`.
  """
  blocks = _get_block_offsets(_get_tbi_file(file_path))
  blocks = _remove_invalid_blocks(blocks)
  blocks = _fill_in_gap(blocks)
  blocks = _merge_blocks(blocks)

  for i in range(len(blocks)-1):
    if blocks[i].end - blocks[i].start > MAX_BLOCK_SIZE:
      raise ValueError("The Block size is too large!")
    yield (file_path, (blocks[i], blocks[i+1]))

  yield (file_path, (blocks[-1],))


def exists_tbi_file(file_path):
  return filesystems.FileSystems.exists(_get_tbi_file(file_path))


def _get_tbi_file(file_path):
  return file_path + _INDEX_FILE_APPENDIX


def _get_block_offsets(index_file):
  # type: (str) -> List[Block]
  """Returns block offsets by parsing the `index_file`.

  The file format can be found in http://samtools.github.io/hts-specs/tabix.pdf.
  """
  read_buffer = _read(index_file)
  offset = 4
  n_ref, offset = _get_next_int(read_buffer, offset)
  offset = 32
  l_mn, offset = _get_next_int(read_buffer, offset)
  offset += l_mn

  chunk_virtual_file_offsets = []
  for _ in range(n_ref):
    n_bin, offset = _get_next_int(read_buffer, offset)
    for _ in range(n_bin):
      _, offset = _get_next_unsigned_int(read_buffer, offset)
      n_chunk, offset = _get_next_int(read_buffer, offset)
      for _ in range(n_chunk):
        start, offset = _get_next_unsigned_long_int(read_buffer, offset)
        end, offset = _get_next_unsigned_long_int(read_buffer, offset)
        chunk_virtual_file_offsets.append((start, end))

    n_intv, offset = _get_next_int(read_buffer, offset)
    offset += 8 * n_intv

  blocks = []
  for chunk in chunk_virtual_file_offsets:
    blocks.append(
        Block(_get_block_offset(chunk[0]), _get_block_offset(chunk[1])))
  return blocks


def _read(index_file):
  # type: (str) -> str
  """Returns the contents of `index_file`."""
  with filesystems.FileSystems.open(
      index_file,
      compression_type=filesystem.CompressionTypes.GZIP) as file_to_read:
    lines = []
    while True:
      line = file_to_read.readline()
      if not line:
        break
      lines.append(line)
  return ''.join(lines)


def _remove_invalid_blocks(blocks):
  valid_blocks = []
  for block in blocks:
    if (block.start <= block.end and
        block.end - block.start <= MAX_BLOCK_SIZE):
      valid_blocks.append(block)
  return valid_blocks


def _fill_in_gap(blocks):
  blocks = sorted(blocks, key=lambda t: t.start)
  valid_blocks = []
  for i in range(len(blocks) - 1):
    valid_blocks.append(blocks[i])
    if blocks[i].end < blocks[i + 1].start:
      valid_blocks.append(Block(blocks[i].end, blocks[i + 1].start))
  valid_blocks.append(blocks[-1])
  return valid_blocks


def _merge_blocks(blocks, size_limit=MAX_BLOCK_SIZE):
  current_start = blocks[0].start
  current_end = blocks[0].end
  merged = []
  for i in range(1, len(blocks)):
    size = blocks[i].end - current_start
    if blocks[i].start <= current_end and size <= size_limit:
      if current_end < blocks[i].end:
        current_end = blocks[i].end
    else:
      merged.append(Block(current_start, current_end))
      current_start = max(blocks[i].start, current_end)
      current_end = blocks[i].end
  if (not merged
      or current_start != merged[-1].start
      or current_end != merged[-1].end):
    merged.append(Block(current_start, current_end))
  return merged


def _get_next_int(read_buffer, offset):
  return struct.unpack('<i', read_buffer[offset: offset + 4])[0], offset+4


def _get_next_unsigned_int(read_buffer, offset):
  return struct.unpack('<I', read_buffer[offset: offset + 4])[0], offset+4


def _get_next_unsigned_long_int(read_buffer, offset):
  return struct.unpack('<Q', read_buffer[offset: offset + 8])[0], offset+8


def _get_block_offset(virtual_file_offset):
  """Returns the offset to the start of the compressed block."""
  return virtual_file_offset >> 16
