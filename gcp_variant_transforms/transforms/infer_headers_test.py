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

"""Tests for infer_headers module."""

from __future__ import absolute_import

from collections import OrderedDict
import unittest

from apache_beam import pvalue
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import Create
from vcf.parser import _Format as Format
from vcf.parser import _Info as Info
from vcf.parser import field_counts

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.testing import asserts
from gcp_variant_transforms.transforms import infer_headers


class InferHeaderFieldsTest(unittest.TestCase):
  """Test case for `InferHeaderFields` DoFn."""

  def _get_sample_header_fields(self):
    infos = OrderedDict([
        ('IS', Info('I1', 1, 'String', 'desc', 'src', 'v')),
        ('ISI', Info('ISI', 1, 'Int', 'desc', 'src', 'v')),
        ('ISF', Info('ISF', 1, 'Float', 'desc', 'src', 'v')),
        ('IF', Info('IF', 1, 'Float', 'desc', 'src', 'v')),
        ('IB', Info('I1', 1, 'Flag', 'desc', 'src', 'v')),
        ('IA', Info('IA', field_counts['A'], 'Integer', 'desc', 'src', 'v'))])
    formats = OrderedDict([
        ('FS', Format('FS', 1, 'String', 'desc')),
        ('FI', Format('FI', 2, 'Integer', 'desc')),
        ('FU', Format('FU', field_counts['.'], 'Float', 'desc')),
        ('GT', Format('GT', 2, 'Integer', 'Special GT key')),
        ('PS', Format('PS', 1, 'Integer', 'Special PS key'))])
    return vcf_header_io.VcfHeader(infos=infos, formats=formats)

  def _get_sample_variant_1(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IS': 'some data', 'ISI': '1', 'ISF': '1.0',
              'IF': 1.0, 'IB': True, 'IA': [1, 2]},
        calls=[vcfio.VariantCall(
            name='Sample1', genotype=[0, 1], phaseset='*',
            info={'FI': 20, 'FU': [10.0, 20.0]})]
    )
    return variant

  def _get_sample_variant_2(self):
    variant = vcfio.Variant(
        reference_name='20', start=123, end=125, reference_bases='CT',
        alternate_bases=[], filters=['q10', 's10'],
        info={'IS_2': 'some data'},
        calls=[vcfio.VariantCall(
            name='Sample1', genotype=[0, 1], phaseset='*', info={'FI_2': 20})]
    )
    return variant

  def _get_sample_variant_info_ia_cardinality_mismatch(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IS': 'some data',
              'ISI': '1',
              'ISF': '1.0',
              'IF': 1.0,
              'IB': True,
              'IA': [0.1]},
        calls=[vcfio.VariantCall(
            name='Sample1', genotype=[0, 1], phaseset='*',
            info={'FI': 20, 'FU': [10.0, 20.0]})]
    )
    return variant

  def _get_sample_variant_info_ia_float_in_list(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IS': 'some data',
              'ISI': '1',
              'ISF': '1.0',
              'IF': 1.0,
              'IB': True,
              'IA': [1, 0.2]},
        calls=[vcfio.VariantCall(
            name='Sample1', genotype=[0, 1], phaseset='*',
            info={'FI': 20, 'FU': [10.0, 20.0]})]
    )
    return variant

  def _get_sample_variant_format_fi_float_value(self):
    variant = vcfio.Variant(
        reference_name='chr19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'IS': 'some data',
              'ISI': '1',
              'ISF': '1.0',
              'IF': 1.0,
              'IB': True,
              'IA': [0.1, 0.2]},
        calls=[vcfio.VariantCall(
            name='Sample1', genotype=[0, 1], phaseset='*',
            info={'FI': 20.1, 'FU': [10.0, 20.0]})]
    )
    return variant

  def test_header_fields_inferred_one_variant(self):
    with TestPipeline() as p:
      variant = self._get_sample_variant_1()
      inferred_headers = (
          p
          | Create([variant])
          | 'InferHeaderFields' >>
          infer_headers.InferHeaderFields(defined_headers=None))

      expected_infos = {'IS': Info('IS', 1, 'String', '', '', ''),
                        'ISI': Info('ISI', 1, 'Integer', '', '', ''),
                        'ISF': Info('ISF', 1, 'Float', '', '', ''),
                        'IF': Info('IF', 1, 'Float', '', '', ''),
                        'IB': Info('IB', 0, 'Flag', '', '', ''),
                        'IA': Info('IA', None, 'Integer', '', '', '')}
      expected_formats = {'FI': Format('FI', 1, 'Integer', ''),
                          'FU': Format('FU', None, 'Float', '')}

      expected = vcf_header_io.VcfHeader(
          infos=expected_infos, formats=expected_formats)
      assert_that(inferred_headers, equal_to([expected]))
      p.run()

  def test_defined_fields_filtered_one_variant(self):
    # All FORMATs and INFOs are already defined in the header section of VCF
    # files.
    with TestPipeline() as p:
      vcf_headers = self._get_sample_header_fields()
      vcf_headers_side_input = p | 'vcf_headers' >> Create([vcf_headers])
      variant = self._get_sample_variant_1()
      inferred_headers = (
          p
          | Create([variant])
          | 'InferHeaderFields' >>
          infer_headers.InferHeaderFields(
              pvalue.AsSingleton(vcf_headers_side_input)))
      expected = vcf_header_io.VcfHeader()
      assert_that(inferred_headers, equal_to([expected]))
      p.run()

  def test_header_fields_inferred_from_two_variants(self):
    with TestPipeline() as p:
      variant_1 = self._get_sample_variant_1()
      variant_2 = self._get_sample_variant_2()
      inferred_headers = (
          p
          | Create([variant_1, variant_2])
          | 'InferHeaderFields' >>
          infer_headers.InferHeaderFields(defined_headers=None))

      expected_infos = {'IS': Info('IS', 1, 'String', '', '', ''),
                        'ISI': Info('ISI', 1, 'Integer', '', '', ''),
                        'ISF': Info('ISF', 1, 'Float', '', '', ''),
                        'IF': Info('IF', 1, 'Float', '', '', ''),
                        'IB': Info('IB', 0, 'Flag', '', '', ''),
                        'IA': Info('IA', None, 'Integer', '', '', ''),
                        'IS_2': Info('IS_2', 1, 'String', '', '', '')}
      expected_formats = {'FI': Format('FI', 1, 'Integer', ''),
                          'FU': Format('FU', None, 'Float', ''),
                          'FI_2': Format('FI_2', 1, 'Integer', '')}

      expected = vcf_header_io.VcfHeader(
          infos=expected_infos, formats=expected_formats)
      assert_that(inferred_headers,
                  asserts.header_fields_equal_ignore_order([expected]))
      p.run()

  def test_defined_fields_filtered_two_variants(self):
    # Only INFO and FORMAT in the first variants are already defined in the
    # header section of the VCF files.
    with TestPipeline() as p:
      vcf_headers = self._get_sample_header_fields()
      vcf_headers_side_input = p | 'vcf_header' >> Create([vcf_headers])
      variant_1 = self._get_sample_variant_1()
      variant_2 = self._get_sample_variant_2()
      inferred_headers = (
          p
          | Create([variant_1, variant_2])
          | 'InferHeaderFields' >>
          infer_headers.InferHeaderFields(
              pvalue.AsSingleton(vcf_headers_side_input)))

      expected_infos = {'IS_2': Info('IS_2', 1, 'String', '', '', '')}
      expected_formats = {'FI_2': Format('FI_2', 1, 'Integer', '')}
      expected = vcf_header_io.VcfHeader(
          infos=expected_infos, formats=expected_formats)
      assert_that(inferred_headers,
                  asserts.header_fields_equal_ignore_order([expected]))
      p.run()

  def test_infer_mismatched_info_field_no_mismatches(self):
    variant = self._get_sample_variant_1()
    infos = {'IS': Info('IS', 1, 'String', '', '', ''),
             'ISI': Info('ISI', 1, 'Integer', '', '', ''),
             'ISF': Info('ISF', 1, 'Float', '', '', ''),
             'IF': Info('IF', 1, 'Float', '', '', ''),
             'IB': Info('IB', 0, 'Flag', '', '', ''),
             'IA': Info('IA', 'A', 'Float', '', '', '')}
    infer_header_fields = infer_headers._InferHeaderFields()
    corrected_info = infer_header_fields._infer_mismatched_info_field(
        'IA', variant.info.get('IA'),
        vcf_header_io.VcfHeader(infos=infos).infos.get('IA'),
        len(variant.alternate_bases))
    self.assertEqual(None, corrected_info)

  def test_infer_mismatched_info_field_correct_num(self):
    variant = self._get_sample_variant_info_ia_cardinality_mismatch()
    infos = {'IS': Info('IS', 1, 'String', '', '', ''),
             'ISI': Info('ISI', 1, 'Integer', '', '', ''),
             'ISF': Info('ISF', 1, 'Float', '', '', ''),
             'IF': Info('IF', 1, 'Float', '', '', ''),
             'IB': Info('IB', 0, 'Flag', '', '', ''),
             'IA': Info('IA', -1, 'Float', '', '', '')}
    infer_header_fields = infer_headers._InferHeaderFields()
    corrected_info = infer_header_fields._infer_mismatched_info_field(
        'IA', variant.info.get('IA'),
        vcf_header_io.VcfHeader(infos=infos).infos.get('IA'),
        len(variant.alternate_bases))
    expected = Info('IA', None, 'Float', '', '', '')
    self.assertEqual(expected, corrected_info)

  def test_infer_mismatched_info_field_correct_type(self):
    variant = self._get_sample_variant_info_ia_cardinality_mismatch()
    infos = {'IS': Info('IS', 1, 'String', '', '', ''),
             'ISI': Info('ISI', 1, 'Integer', '', '', ''),
             'ISF': Info('ISF', 1, 'Float', '', '', ''),
             'IF': Info('IF', 1, 'Float', '', '', ''),
             'IB': Info('IB', 0, 'Flag', '', '', ''),
             'IA': Info('IA', None, 'Integer', '', '', '')}
    infer_header_fields = infer_headers._InferHeaderFields()
    corrected_info = infer_header_fields._infer_mismatched_info_field(
        'IA', variant.info.get('IA'),
        vcf_header_io.VcfHeader(infos=infos).infos.get('IA'),
        len(variant.alternate_bases)
    )
    expected = Info('IA', None, 'Float', '', '', '')
    self.assertEqual(expected, corrected_info)

  def test_infer_mismatched_info_field_correct_type_list(self):
    variant = self._get_sample_variant_info_ia_float_in_list()
    infos = {'IS': Info('IS', 1, 'String', '', '', ''),
             'ISI': Info('ISI', 1, 'Integer', '', '', ''),
             'ISF': Info('ISF', 1, 'Float', '', '', ''),
             'IF': Info('IF', 1, 'Float', '', '', ''),
             'IB': Info('IB', 0, 'Flag', '', '', ''),
             'IA': Info('IA', None, 'Integer', '', '', '')}
    infer_header_fields = infer_headers._InferHeaderFields()
    corrected_info = infer_header_fields._infer_mismatched_info_field(
        'IA', variant.info.get('IA'),
        vcf_header_io.VcfHeader(infos=infos).infos.get('IA'),
        len(variant.alternate_bases)
    )
    expected = Info('IA', None, 'Float', '', '', '')
    self.assertEqual(expected, corrected_info)

  def test_infer_info_fields_no_conflicts(self):
    variant = self._get_sample_variant_1()
    infos = {'IS': Info('IS', 1, 'String', '', '', ''),
             'ISI': Info('ISI', 1, 'Integer', '', '', ''),
             'ISF': Info('ISF', 1, 'Float', '', '', ''),
             'IF': Info('IF', 1, 'Float', '', '', ''),
             'IB': Info('IB', 0, 'Flag', '', '', ''),
             'IA': Info('IA', -1, 'Float', '', '', '')}
    infer_header_fields = infer_headers._InferHeaderFields()
    inferred_infos = infer_header_fields._infer_info_fields(
        variant, vcf_header_io.VcfHeader(infos=infos))
    self.assertEqual({}, inferred_infos)

  def test_infer_info_fields_combined_conflicts(self):
    variant = self._get_sample_variant_info_ia_cardinality_mismatch()
    infos = {'IS': Info('IS', 1, 'String', '', '', ''),
             'ISI': Info('ISI', 1, 'Integer', '', '', ''),
             'ISF': Info('ISF', 1, 'Float', '', '', ''),
             'IB': Info('IB', 0, 'Flag', '', '', ''),
             'IA': Info('IA', -1, 'Integer', '', '', '')}
    infer_header_fields = infer_headers._InferHeaderFields()
    inferred_infos = infer_header_fields._infer_info_fields(
        variant, vcf_header_io.VcfHeader(infos=infos))
    expected_infos = {'IF': Info('IF', 1, 'Float', '', '', ''),
                      'IA': Info('IA', None, 'Float', '', '', '')}
    self.assertEqual(expected_infos, inferred_infos)

  def test_infer_mismatched_format_field(self):
    variant = self._get_sample_variant_format_fi_float_value()
    formats = OrderedDict([
        ('FS', Format('FS', 1, 'String', 'desc')),
        ('FI', Format('FI', 2, 'Integer', 'desc')),
        ('FU', Format('FU', field_counts['.'], 'Float', 'desc')),
        ('GT', Format('GT', 2, 'Integer', 'Special GT key')),
        ('PS', Format('PS', 1, 'Integer', 'Special PS key'))])
    infer_header_fields = infer_headers._InferHeaderFields()
    corrected_format = infer_header_fields._infer_mismatched_format_field(
        'FI', variant.calls[0].info.get('FI'),
        vcf_header_io.VcfHeader(formats=formats).formats.get('FI'))
    expected_formats = Format('FI', 2, 'Float', 'desc')
    self.assertEqual(expected_formats, corrected_format)

  def test_infer_format_fields_no_conflicts(self):
    variant = self._get_sample_variant_1()
    formats = OrderedDict([
        ('FS', Format('FS', 1, 'String', 'desc')),
        ('FI', Format('FI', 2, 'Integer', 'desc')),
        ('FU', Format('FU', field_counts['.'], 'Float', 'desc')),
        ('GT', Format('GT', 2, 'Integer', 'Special GT key')),
        ('PS', Format('PS', 1, 'Integer', 'Special PS key'))])
    infer_header_fields = infer_headers._InferHeaderFields()
    header = infer_header_fields._infer_format_fields(
        variant, vcf_header_io.VcfHeader(formats=formats))
    self.assertEqual({}, header)

  def test_infer_format_fields_combined_conflicts(self):
    variant = self._get_sample_variant_format_fi_float_value()
    formats = OrderedDict([
        ('FS', Format('FS', 1, 'String', 'desc')),
        ('FI', Format('FI', 2, 'Integer', 'desc')),
        ('GT', Format('GT', 2, 'Integer', 'Special GT key')),
        ('PS', Format('PS', 1, 'Integer', 'Special PS key'))])
    infer_header_fields = infer_headers._InferHeaderFields()
    inferred_formats = infer_header_fields._infer_format_fields(
        variant, vcf_header_io.VcfHeader(formats=formats))
    expected_formats = {'FI': Format('FI', 2, 'Float', 'desc'),
                        'FU': Format('FU', field_counts['.'], 'Float', '')}
    self.assertEqual(expected_formats, inferred_formats)

  def test_pipeline(self):
    infos = {'IS': Info('IS', 1, 'String', '', '', ''),
             'ISI': Info('ISI', 1, 'Integer', '', '', ''),
             'ISF': Info('ISF', 1, 'Float', '', '', ''),
             'IB': Info('IB', 0, 'Flag', '', '', ''),
             'IA': Info('IA', -1, 'Integer', '', '', '')}
    formats = OrderedDict([
        ('FS', Format('FS', 1, 'String', 'desc')),
        ('FI', Format('FI', 2, 'Integer', 'desc')),
        ('GT', Format('GT', 2, 'Integer', 'Special GT key')),
        ('PS', Format('PS', 1, 'Integer', 'Special PS key'))])

    with TestPipeline() as p:
      variant_1 = self._get_sample_variant_info_ia_cardinality_mismatch()
      variant_2 = self._get_sample_variant_format_fi_float_value()
      inferred_headers = (
          p
          | Create([variant_1, variant_2])
          | 'InferHeaderFields' >>
          infer_headers.InferHeaderFields(
              defined_headers=vcf_header_io.VcfHeader(infos=infos,
                                                      formats=formats),
              allow_incompatible_records=True))

      expected_infos = {'IA': Info('IA', None, 'Float', '', '', ''),
                        'IF': Info('IF', 1, 'Float', '', '', '')}
      expected_formats = {'FI': Format('FI', 2, 'Float', 'desc'),
                          'FU': Format('FU', None, 'Float', '')}
      expected = vcf_header_io.VcfHeader(infos=expected_infos,
                                         formats=expected_formats)
      assert_that(inferred_headers,
                  asserts.header_fields_equal_ignore_order([expected]))
      p.run()
