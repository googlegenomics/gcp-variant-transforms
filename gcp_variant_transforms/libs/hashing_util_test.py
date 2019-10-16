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

  def test_generate_int64_hash_code(self):
    hash_code = hashing_util.generate_unsigned_hash_code('gs://bucket/blob '
                                                         'sample 1')
    self.assertEqual(hash_code, 5941535641672088077)
