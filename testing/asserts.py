"""Custom asserts for tests."""

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
