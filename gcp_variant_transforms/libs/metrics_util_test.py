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

"""Unit tests for metrics_util module."""

from __future__ import absolute_import

import unittest

from gcp_variant_transforms.libs import metrics_util


_TEST_COUNTER = 'test_counter'


class CounterFactoryTest(unittest.TestCase):

  def test_create_counter(self):
    counter = metrics_util.CounterFactory().create_counter(_TEST_COUNTER)
    self.assertTrue(isinstance(counter, metrics_util.CounterInterface))
