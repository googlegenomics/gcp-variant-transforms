"""Tests for variant_to_bigquery module."""

import unittest

from apache_beam import ParDo
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create

from beam_io.vcfio import Variant
from libs.bigquery_vcf_schema import _ColumnKeyConstants as ColumnKeyConstants
from transforms.variant_to_bigquery import _ConvertToBigQueryTableRow as ConvertToBigQueryTableRow


class ConvertToBigQueryTableRowTest(unittest.TestCase):
  """Test cases for the ``ConvertToBigQueryTableRow`` DoFn."""

  def _get_sample_variant_1(self):
    variant = Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'])
    row = {ColumnKeyConstants.REFERENCE_NAME: 'chr19',
           ColumnKeyConstants.START_POSITION: 11,
           ColumnKeyConstants.END_POSITION: 12,
           ColumnKeyConstants.REFERENCE_BASES: 'C',
           ColumnKeyConstants.ALTERNATE_BASES: [
               {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A'},
               {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT'}]}
    return variant, row

  def _get_sample_variant_2(self):
    variant = Variant(
        reference_name='20', start=123, end=125, reference_bases='CT',
        alternate_bases=[])
    row = {ColumnKeyConstants.REFERENCE_NAME: '20',
           ColumnKeyConstants.START_POSITION: 123,
           ColumnKeyConstants.END_POSITION: 125,
           ColumnKeyConstants.REFERENCE_BASES: 'CT',
           ColumnKeyConstants.ALTERNATE_BASES: []}
    return variant, row

  def _get_sample_variant_3(self):
    variant = Variant(
        reference_name='20', start=None, end=None, reference_bases=None)
    row = {ColumnKeyConstants.REFERENCE_NAME: '20',
           ColumnKeyConstants.START_POSITION: None,
           ColumnKeyConstants.END_POSITION: None,
           ColumnKeyConstants.REFERENCE_BASES: None,
           ColumnKeyConstants.ALTERNATE_BASES: []}
    return variant, row

  def test_convert_variant_to_bigquery_row(self):
    variant_1, row_1 = self._get_sample_variant_1()
    variant_2, row_2 = self._get_sample_variant_2()
    variant_3, row_3 = self._get_sample_variant_3()
    pipeline = TestPipeline()
    bigquery_rows = (
        pipeline
        | Create([variant_1, variant_2, variant_3])
        | 'ConvertToRow' >> ParDo(ConvertToBigQueryTableRow()))
    assert_that(bigquery_rows, equal_to([row_1, row_2, row_3]))
    pipeline.run()
