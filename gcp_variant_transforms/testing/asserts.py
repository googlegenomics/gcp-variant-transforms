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


def items_equal(expected):
  """Returns a function for checking expected and actual have the same items."""
  def _items_equal(actual):
    sorted_expected = sorted(expected)
    sorted_actual = sorted(actual)
    if sorted_expected != sorted_actual:
      raise BeamAssertException(
          'Failed assert: %r != %r' % (sorted_expected, sorted_actual))
  return _items_equal


def variants_equal_to_ignore_order(expected):
  """Returns a function for comparing Variant output from the pipeline."""
  def _equal(actual):
    items_equal(expected)(actual)
  return _equal


def count_equals_to(expected_count):
  """Returns a function for comparing count of items from pipeline output."""
  def _count_equal(actual_list):
    actual_count = len(actual_list)
    if expected_count != actual_count:
      raise BeamAssertException(
          'Failed assert: %d == %d' % (expected_count, actual_count))
  return _count_equal


def has_calls(call_names):
  """Returns a function for checking presence of calls_names in variants."""
  def _has_calls(variants):
    assert_fn = items_equal(call_names)
    for variant in variants:
      variant_call_names = [call.name for call in variant.calls]
      assert_fn(variant_call_names)
  return _has_calls


def header_vars_equal(expected):
  def _vars_equal(actual):
    expected_vars = [vars(header) for header in expected]
    actual_vars = [vars(header) for header in actual]
    items_equal(expected_vars)(actual_vars)
  return _vars_equal
