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

"""Tests for `write_variants_to_shards` module."""

import os
import unittest

from apache_beam.io import filesystems
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.testing import temp_dir
from gcp_variant_transforms.testing.testdata_util import hash_name
from gcp_variant_transforms.transforms import write_variants_to_shards


class WriteVariantsToShardsTest(unittest.TestCase):

  def _get_variants(self):
    variant_1 = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1'], quality=2,
        filters=['PASS'],
        info={'A1': 'some data', 'A2': ['data1', 'data2']}
    )
    variant_2 = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1'], quality=20,
        filters=['q10'],
        info={'A1': 'some data2', 'A3': ['data3', 'data4']}

    )
    return [variant_1, variant_2]

  def test_write_to_shards(self):
    with temp_dir.TempDir() as tempdir:
      shards_writter = write_variants_to_shards._WriteVariantsToVCFShards(
          tempdir.get_path(), 3)
      variants = self._get_variants()
      variant_lines = [shards_writter._coder.encode(v).strip(b'\n')
                       for v in variants]
      shards_writter._write_variant_lines_to_vcf_shard(variant_lines)

      expected_content = [
          '\t'.join(['#CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER',
                     'INFO', 'FORMAT\n']).encode('utf-8'),
          '\t'.join(['19', '12', 'rs1', 'C', 'A,TT', '2', 'PASS',
                     'A1=some data;A2=data1,data2', '.\n']).encode('utf-8'),
          '\t'.join(['19', '12', 'rs1', 'C', 'A,TT', '20', 'q10',
                     'A1=some data2;A3=data3,data4', '.']).encode('utf-8')]

      file_paths = []
      for dirpath, _, filenames in os.walk(tempdir.get_path()):
        for f in filenames:
          file_paths.append(os.path.abspath(os.path.join(dirpath, f)))
      self.assertEqual(1, len(file_paths))
      with filesystems.FileSystems.open(file_paths[0]) as f:
        content = f.readlines()
        self.assertEqual(content, expected_content)

  def test_write_to_shards_pipeline(self):
    with temp_dir.TempDir() as tempdir:
      pipeline = TestPipeline()
      _ = (
          pipeline
          | Create(self._get_variants())
          | 'WriteToShards' >> write_variants_to_shards.WriteToShards(
              tempdir.get_path(),
              [hash_name('Sample 1'), hash_name('Sample 2')])
      )
      pipeline.run()
