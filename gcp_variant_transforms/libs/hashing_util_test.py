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

from gcp_variant_transforms.libs import hashing_util


class HashingUtilTest(unittest.TestCase):

  def test_generate_unsigned_hash_code(self):
    hash_code = (
        hashing_util._generate_unsigned_hash_code(['str1', 'str2', 'str3']))
    self.assertEqual(hash_code, 7972645828447426528)

    hash_code = (
        hashing_util._generate_unsigned_hash_code(['str1', 'str2', 'str3'],
                                                  1000))
    self.assertEqual(hash_code, 335)

    hash_code = (
        hashing_util._generate_unsigned_hash_code(['str1', 'str2'], 1000))
    self.assertEqual(hash_code, 678)

    hash_code = (
        hashing_util._generate_unsigned_hash_code(['str2', 'str1'], 1000))
    self.assertEqual(hash_code, 110)

  def test_generate_sample_id_with_file_path(self):
    hash_code = hashing_util.generate_sample_id('Sample1',
                                                'gs://bucket1/dir1/file1.vcf')
    self.assertEqual(hash_code, 7715696391291253656)
    hash_code = hashing_util.generate_sample_id('Sample2',
                                                'gs://bucket1/dir1/file1.vcf')
    self.assertEqual(hash_code, 5682150464643626236)
    hash_code = hashing_util.generate_sample_id('Sample1',
                                                'gs://bucket1/dir1/file2.vcf')
    self.assertEqual(hash_code, 668336000922978678)
    hash_code = hashing_util.generate_sample_id('Sample2',
                                                'gs://bucket1/dir1/file2.vcf')
    self.assertEqual(hash_code, 5498327443813165683)

  def test_generate_sample_id_without_file_path(self):
    hash_code = hashing_util.generate_sample_id('Sample1')
    self.assertEqual(hash_code, 6365297890523177914)

    hash_code = hashing_util.generate_sample_id('Sample2')
    self.assertEqual(hash_code, 8341768597576477893)

  def test_create_composite_sample_name(self):
    composite_names = {
        'gs___bucket1_dir1_file1_vcf_sample1':
            ('sample1', 'gs://bucket1/dir1/file1.vcf'),
        'gs___bucket1_dir1_file1_vcf_gz_sample1':
            ('sample1', 'gs://bucket1/dir1/file1.vcf.gz'),
        'gs___BUCKET1_DIR1_FILE1_vcf_sample1':
            ('sample1', 'gs://BUCKET1/DIR1/FILE1.vcf'),
        'gs___bucket_1_dir_1_file_1_vcf_sample1':
            ('sample1', 'gs://bucket-1/dir-1/file-1.vcf'),
        'gs___bucket1_dir1_file1_vcf_sample-@~!*&1':
            ('sample-@~!*&1', 'gs://bucket1/dir1/file1.vcf'),
    }
    for expected_name, inputs in list(composite_names.items()):
      self.assertEqual(expected_name,
                       hashing_util.create_composite_sample_name(inputs[0],
                                                                 inputs[1]))
