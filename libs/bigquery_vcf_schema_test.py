"""Tests for bigquery_vcf_schema module."""

import unittest

from beam_io.vcfio import Variant
from libs.bigquery_vcf_schema import _ColumnKeyConstants as ColumnKeyConstants
from libs.bigquery_vcf_schema import get_row_from_variant


class GetRowFromVariantTest(unittest.TestCase):
  """Test cases for the ``get_row_from_variant`` library function."""

  def test_all_fields(self):
    variant = Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'])
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'C',
        ColumnKeyConstants.ALTERNATE_BASES: [
            {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A'},
            {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT'}]}
    self.assertEqual(expected_row, get_row_from_variant(variant))

  def test_no_alternate_bases(self):
    variant = Variant(
        reference_name='chr19', start=11, end=12, reference_bases='CT',
        alternate_bases=[])
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: 11,
        ColumnKeyConstants.END_POSITION: 12,
        ColumnKeyConstants.REFERENCE_BASES: 'CT',
        ColumnKeyConstants.ALTERNATE_BASES: []}
    self.assertEqual(expected_row, get_row_from_variant(variant))

  def test_some_fields_set(self):
    variant = Variant(
        reference_name='chr19', start=None, end=123, reference_bases=None,
        alternate_bases=[])
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: 'chr19',
        ColumnKeyConstants.START_POSITION: None,
        ColumnKeyConstants.END_POSITION: 123,
        ColumnKeyConstants.REFERENCE_BASES: None,
        ColumnKeyConstants.ALTERNATE_BASES: []}
    self.assertEqual(expected_row, get_row_from_variant(variant))

  def test_no_field_set(self):
    variant = Variant()
    expected_row = {
        ColumnKeyConstants.REFERENCE_NAME: None,
        ColumnKeyConstants.START_POSITION: None,
        ColumnKeyConstants.END_POSITION: None,
        ColumnKeyConstants.REFERENCE_BASES: None,
        ColumnKeyConstants.ALTERNATE_BASES: []}
    self.assertEqual(expected_row, get_row_from_variant(variant))
