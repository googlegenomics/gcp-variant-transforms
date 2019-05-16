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

import unittest

from gcp_variant_transforms.beam_io import bgzf_io
from gcp_variant_transforms.testing import testdata_util


class BgzfIOTest(unittest.TestCase):

  def test_read_tbi(self):
    blocks = bgzf_io.split_bgzf(testdata_util.get_full_file_path(
        'Y.vcf.bgz'))
    self.assertEqual(len(list(blocks)), 1)

  def test_get_block_offsets(self):
    blocks = bgzf_io._get_block_offsets(testdata_util.get_full_file_path(
        'Y.vcf.bgz.tbi'))
    self.assertEqual(len(blocks), 110)

  def test_remove_invalid_blocks(self):
    blocks = [bgzf_io.Block(start=5, end=bgzf_io.MAX_BLOCK_SIZE+5),
              bgzf_io.Block(start=5, end=bgzf_io.MAX_BLOCK_SIZE+6),
              bgzf_io.Block(start=20, end=19),
              bgzf_io.Block(start=3, end=9)]
    valid_blocks = bgzf_io._remove_invalid_blocks(blocks)
    self.assertEqual(valid_blocks,
                     [bgzf_io.Block(start=5, end=bgzf_io.MAX_BLOCK_SIZE+5),
                      bgzf_io.Block(start=3, end=9)])

  def test_fill_in_gap(self):
    blocks = [bgzf_io.Block(start=5, end=100),
              bgzf_io.Block(start=150, end=200),
              bgzf_io.Block(start=200, end=250),
              bgzf_io.Block(start=300, end=300)]
    valid_blocks = bgzf_io._fill_in_gap(blocks)
    self.assertEqual(valid_blocks, [bgzf_io.Block(start=5, end=100),
                                    bgzf_io.Block(start=100, end=150),
                                    bgzf_io.Block(start=150, end=200),
                                    bgzf_io.Block(start=200, end=250),
                                    bgzf_io.Block(start=250, end=300),
                                    bgzf_io.Block(start=300, end=300)])

  def test_merge_chunks(self):
    blocks = [bgzf_io.Block(start=0, end=100),
              bgzf_io.Block(start=90, end=99),
              bgzf_io.Block(start=100, end=150),
              bgzf_io.Block(start=120, end=201),
              bgzf_io.Block(start=201, end=300)]
    merged_blocks = bgzf_io._merge_blocks(blocks, size_limit=200)
    self.assertEqual(merged_blocks, [bgzf_io.Block(start=0, end=150),
                                     bgzf_io.Block(start=150, end=300)])
