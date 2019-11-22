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
        hashing_util.generate_unsigned_hash_code(['str1', 'str2', 'str3']))
    self.assertEqual(hash_code, 7972645828447426528)

    hash_code = (
        hashing_util.generate_unsigned_hash_code(['str1', 'str2', 'str3'],
                                                 1000))
    self.assertEqual(hash_code, 335)

    hash_code = (
        hashing_util.generate_unsigned_hash_code(['str1', 'str2'], 1000))
    self.assertEqual(hash_code, 678)

    hash_code = (
        hashing_util.generate_unsigned_hash_code(['str2', 'str1'], 1000))
    self.assertEqual(hash_code, 110)

  def test_generate_sample_id_with_file_path(self):
    hash_code = hashing_util.generate_sample_id('Sample 1', 'file_1')
    self.assertEqual(hash_code, 5961690698012655974)

    hash_code = hashing_util.generate_sample_id('Sample 2', 'file_1')
    self.assertEqual(hash_code, 5854056809620188906)

    hash_code = hashing_util.generate_sample_id('Sample 1', 'file_2')
    self.assertEqual(hash_code, 5259968798637352651)

    hash_code = hashing_util.generate_sample_id('Sample 2', 'file_2')
    self.assertEqual(hash_code, 6253115674664185777)

  def test_generate_sample_id_without_file_path(self):
    hash_code = hashing_util.generate_sample_id('Sample 1')
    self.assertEqual(hash_code, 3787456804322732813)

    hash_code = hashing_util.generate_sample_id('Sample 2')
    self.assertEqual(hash_code, 6583536632710741776)
