# Copyright 2017 Google Inc.  All Rights Reserved.
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

"""Custom asserts for tests."""

from __future__ import absolute_import

from apache_beam.testing.util import BeamAssertException


def variants_equal_to_ignore_order(expected):
  """Returns a function for comparing Variant output from the pipeline."""
  def _equal(actual):
    sorted_expected = sorted(expected, key=repr)
    sorted_actual = sorted(actual, key=repr)
    if sorted_expected != sorted_actual:
      raise BeamAssertException(
          'Failed assert: %r == %r' % (sorted_expected, sorted_actual))
  return _equal


def count_equals_to(expected_count):
  """Returns a function for comparing count of items from pipeline output."""
  def _count_equal(actual_list):
    actual_count = len(actual_list)
    if expected_count != actual_count:
      raise BeamAssertException(
          'Expected %d not equal actual %d' % (expected_count, actual_count))
  return _count_equal
