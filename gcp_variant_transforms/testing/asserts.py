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

from typing import Callable, List  # pylint: disable=unused-import
from apache_beam.testing.util import BeamAssertException
from gcp_variant_transforms.beam_io import vcf_header_io  # pylint: disable=unused-import


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


def has_sample_ids(sample_ids):
  """Returns a function for checking presence of sample_ids in variants."""
  def _has_sample_ids(variants):
    assert_fn = items_equal(sample_ids)
    for variant in variants:
      variant_sample_ids = [call.sample_id for call in variant.calls]
      assert_fn(variant_sample_ids)
  return _has_sample_ids


def dict_values_equal(expected_dict):
  """Verifies that dictionary is the same as expected."""
  def _items_equal(actual_dict):
    actual = actual_dict[0]
    for k in expected_dict:
      if k not in actual or expected_dict[k] != actual[k]:
        raise BeamAssertException(
            'Failed assert: %s == %s' % (expected_dict, actual))
    if len(expected_dict) != len(actual):
      raise BeamAssertException(
          'Failed assert: %s == %s' % (expected_dict, actual))
  return _items_equal


def header_vars_equal(expected):
  def _vars_equal(actual):
    expected_vars = [vars(header) for header in expected]
    actual_vars = [vars(header) for header in actual]
    items_equal(expected_vars)(actual_vars)
  return _vars_equal


def header_fields_equal_ignore_order(expected):
  # type: (List[vcf_header_io.VcfHeader]) -> Callable
  """Returns a function for checking presence of headers ignoring order.

  Header fields have type `OrderedDict`, so this method essentially converts
  them to `dict`, so that we can verify values ignoring order.
  """
  def _fields_equal(actual):
    fields_to_check = ['alts', 'contigs', 'filters', 'formats', 'infos']
    for field in fields_to_check:
      expected_fields = [dict(getattr(header, field)) for header in expected]
      actual_fields = [dict(getattr(header, field)) for header in actual]
      items_equal(expected_fields)(actual_fields)
  return _fields_equal
