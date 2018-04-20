# Copyright 2018 Google Inc.  All Rights Reserved.
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

"""Tests for vcf_to_bq_preprocess script."""

import unittest

from apache_beam.io import filesystems
from gcp_variant_transforms import vcf_to_bq_preprocess
from gcp_variant_transforms.testing import temp_dir


class PreprocessTest(unittest.TestCase):

  def test_preprocess_run_locally(self):
    with temp_dir.TempDir() as tempdir:
      argv = [
          '--input_pattern',
          'gs://gcp-variant-transforms-testfiles/small_tests/infer-undefined'
          '-header-fields.vcf',
          'directory',
          tempdir.get_path()
      ]
    abs_report_name, abs_resolved_headers_name = vcf_to_bq_preprocess.run(argv)

    assert filesystems.FileSystems.exists(abs_report_name)
    assert filesystems.FileSystems.exists(abs_resolved_headers_name)
