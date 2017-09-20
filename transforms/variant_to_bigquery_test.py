"""Tests for variant_to_bigquery module."""

import unittest

from apache_beam import ParDo
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create

from beam_io.vcfio import Variant
from beam_io.vcfio import VariantCall
from beam_io.vcfio import VariantInfo
from libs.bigquery_vcf_schema import _ColumnKeyConstants as ColumnKeyConstants
from transforms.variant_to_bigquery import _ConvertToBigQueryTableRow as ConvertToBigQueryTableRow


class ConvertToBigQueryTableRowTest(unittest.TestCase):
  """Test cases for the ``ConvertToBigQueryTableRow`` DoFn."""

  def _get_sample_variant_1(self, split_alternate_allele_info_fields=True):
    variant = Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'AF': VariantInfo([0.1, 0.2], 'A'),
              'AF2': VariantInfo([0.2, 0.3], 'A'),
              'A1': VariantInfo('some data', '1'),
              'A2': VariantInfo(['data1', 'data2'], '2')},
        calls=[
            VariantCall(
                name='Sample1', genotype=[0, 1], phaseset='*',
                info={'GQ': 20, 'HQ': [10, 20]}),
            VariantCall(
                name='Sample2', genotype=[1, 0],
                info={'GQ': 10, 'FLAG1': True}),
        ]
    )
    row = {ColumnKeyConstants.REFERENCE_NAME: 'chr19',
           ColumnKeyConstants.START_POSITION: 11,
           ColumnKeyConstants.END_POSITION: 12,
           ColumnKeyConstants.REFERENCE_BASES: 'C',
           ColumnKeyConstants.NAMES: ['rs1', 'rs2'],
           ColumnKeyConstants.QUALITY: 2,
           ColumnKeyConstants.FILTER: ['PASS'],
           ColumnKeyConstants.CALLS: [
               {ColumnKeyConstants.CALLS_NAME: 'Sample1',
                ColumnKeyConstants.CALLS_GENOTYPE: [0, 1],
                ColumnKeyConstants.CALLS_PHASESET: '*',
                'GQ': 20, 'HQ': [10, 20]},
               {ColumnKeyConstants.CALLS_NAME: 'Sample2',
                ColumnKeyConstants.CALLS_GENOTYPE: [1, 0],
                ColumnKeyConstants.CALLS_PHASESET: None,
                'GQ': 10, 'FLAG1': True}],
           'A1': 'some data',
           'A2': ['data1', 'data2']}
    if split_alternate_allele_info_fields:
      row[ColumnKeyConstants.ALTERNATE_BASES] = [
          {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A', 'AF': 0.1, 'AF2': 0.2},
          {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT', 'AF': 0.2, 'AF2': 0.3}]
    else:
      row[ColumnKeyConstants.ALTERNATE_BASES] = [
          {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'A'},
          {ColumnKeyConstants.ALTERNATE_BASES_ALT: 'TT'}]
      row['AF'] = [0.1, 0.2]
      row['AF2'] = [0.2, 0.3]
    return variant, row

  def _get_sample_variant_2(self):
    variant = Variant(
        reference_name='20', start=123, end=125, reference_bases='CT',
        alternate_bases=[], filters=['q10', 's10'],
        info={'INTINFO': VariantInfo(1234, '1')})
    row = {ColumnKeyConstants.REFERENCE_NAME: '20',
           ColumnKeyConstants.START_POSITION: 123,
           ColumnKeyConstants.END_POSITION: 125,
           ColumnKeyConstants.REFERENCE_BASES: 'CT',
           ColumnKeyConstants.ALTERNATE_BASES: [],
           ColumnKeyConstants.NAMES: [],
           ColumnKeyConstants.QUALITY: None,
           ColumnKeyConstants.FILTER: ['q10', 's10'],
           ColumnKeyConstants.CALLS: [],
           'INTINFO': 1234}
    return variant, row

  def _get_sample_variant_3(self):
    variant = Variant(
        reference_name='20', start=None, end=None, reference_bases=None)
    row = {ColumnKeyConstants.REFERENCE_NAME: '20',
           ColumnKeyConstants.START_POSITION: None,
           ColumnKeyConstants.END_POSITION: None,
           ColumnKeyConstants.REFERENCE_BASES: None,
           ColumnKeyConstants.ALTERNATE_BASES: [],
           ColumnKeyConstants.NAMES: [],
           ColumnKeyConstants.QUALITY: None,
           ColumnKeyConstants.FILTER: [],
           ColumnKeyConstants.CALLS: []}
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

  def test_convert_variant_to_bigquery_row_no_split_alternate_headers(self):
    variant_1, row_1 = self._get_sample_variant_1(
        split_alternate_allele_info_fields=False)
    variant_2, row_2 = self._get_sample_variant_2()
    variant_3, row_3 = self._get_sample_variant_3()
    pipeline = TestPipeline()
    bigquery_rows = (
        pipeline
        | Create([variant_1, variant_2, variant_3])
        | 'ConvertToRow' >> ParDo(ConvertToBigQueryTableRow(
            split_alternate_allele_info_fields=False)))
    assert_that(bigquery_rows, equal_to([row_1, row_2, row_3]))
    pipeline.run()
