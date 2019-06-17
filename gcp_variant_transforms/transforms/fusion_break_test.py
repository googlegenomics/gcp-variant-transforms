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

import apache_beam as beam
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from gcp_variant_transforms.transforms import fusion_break


class FusionBreakTest(unittest.TestCase):

  def test_fusion_break_pipeline(self):
    pipeline = test_pipeline.TestPipeline()
    data = [1, 2, 3]
    output = pipeline | beam.Create(data) | fusion_break.FusionBreak()
    assert_that(output, equal_to(data))
    pipeline.run()
