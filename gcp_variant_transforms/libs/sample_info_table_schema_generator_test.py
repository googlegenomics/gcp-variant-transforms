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

"""Tests for `sample_info_table_schema_generator` module."""

import unittest

from gcp_variant_transforms.libs import sample_info_table_schema_generator


class GenerateSampleInfoTableSchemaTest(unittest.TestCase):

  def test_generate_sample_info_table_schema(self):
    schema = sample_info_table_schema_generator.generate_schema()
    expected_fields = [sample_info_table_schema_generator.SAMPLE_ID,
                       sample_info_table_schema_generator.SAMPLE_NAME,
                       sample_info_table_schema_generator.FILE_PATH]
    self.assertEqual(expected_fields, [field.name for field in schema.fields])
