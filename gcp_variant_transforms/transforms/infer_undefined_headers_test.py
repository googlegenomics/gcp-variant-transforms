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

"""Tests for infer_variant_header module."""

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
from gcp_variant_transforms.transforms import infer_undefined_headers

class InferUndefinedHeaderFieldsTest(unittest.TestCase):
  """ Test case for ``InferUndefinedHeaderFields`` DoFn."""

  def _get_sample_header_fields(self):
    infos = OrderedDict([
        ('IS', Info('I1', 1, 'String', 'desc', 'src', 'v')),
        ('ISI', Info('ISI', 1, 'Int', 'desc', 'src', 'v')),
        ('ISF', Info('ISF', 1, 'Float', 'desc', 'src', 'v')),
        ('IF', Info('I1', 1, 'Flag', 'desc', 'src', 'v')),
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
        info={'IS': vcfio.VariantInfo('some data', '1'),
              'ISI': vcfio.VariantInfo('1', '1'),
              'ISF': vcfio.VariantInfo('1.0', '1'),
              'IF': vcfio.VariantInfo(True, '0'),
              'IA': vcfio.VariantInfo([0.1, 0.2], '2')},
        calls=[vcfio.VariantCall(
            name='Sample1', genotype=[0, 1], phaseset='*',
            info={'FI': 20, 'FU': [10.0, 20.0]})]
    )
    return variant

  def _get_sample_variant_2(self):
    variant = vcfio.Variant(
        reference_name='20', start=123, end=125, reference_bases='CT',
        alternate_bases=[], filters=['q10', 's10'],
        info={'IS_2': vcfio.VariantInfo('some data', '1')},
        calls=[vcfio.VariantCall(
            name='Sample1', genotype=[0, 1], phaseset='*', info={'FI_2': 20})]
    )
    return variant

  def test_header_fields_inferred_one_variant(self):
    with TestPipeline() as p:
      variant = self._get_sample_variant_1()
      inferred_headers = (
          p
          | Create([variant])
          | 'InferUndefinedHeaderFields' >>
          infer_undefined_headers.InferUndefinedHeaderFields(
              defined_headers=None))

      expected_infos = {'IS': Info('IS', 1, 'String', '', '', ''),
                        'ISI': Info('ISI', 1, 'Integer', '', '', ''),
                        'ISF': Info('ISF', 1, 'Float', '', '', ''),
                        'IF': Info('IF', 0, 'Flag', '', '', ''),
                        'IA': Info('IA', None, 'Float', '', '', '')}
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
          | 'InferUndefinedHeaderFields' >>
          infer_undefined_headers.InferUndefinedHeaderFields(
              pvalue.AsSingleton(vcf_headers_side_input)))

      assert_that(inferred_headers, equal_to([]))
      p.run()

  def test_header_fields_inferred_from_two_variants(self):
    with TestPipeline() as p:
      variant_1 = self._get_sample_variant_1()
      variant_2 = self._get_sample_variant_2()
      inferred_headers = (
          p
          | Create([variant_1, variant_2])
          | 'InferUndefinedHeaderFields' >>
          infer_undefined_headers.InferUndefinedHeaderFields(
              defined_headers=None))

      expected_infos = {'IS': Info('IS', 1, 'String', '', '', ''),
                        'ISI': Info('ISI', 1, 'Integer', '', '', ''),
                        'ISF': Info('ISF', 1, 'Float', '', '', ''),
                        'IF': Info('IF', 0, 'Flag', '', '', ''),
                        'IA': Info('IA', None, 'Float', '', '', ''),
                        'IS_2': Info('IS_2', 1, 'String', '', '', '')}
      expected_formats = {'FI': Format('FI', 1, 'Integer', ''),
                          'FU': Format('FU', None, 'Float', ''),
                          'FI_2': Format('FI_2', 1, 'Integer', '')}

      expected = vcf_header_io.VcfHeader(
          infos=expected_infos, formats=expected_formats)
      assert_that(inferred_headers, equal_to([expected]))
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
          | 'InferUndefinedHeaderFields' >>
          infer_undefined_headers.InferUndefinedHeaderFields(
              pvalue.AsSingleton(vcf_headers_side_input)))

      expected_infos = {'IS_2': Info('IS_2', 1, 'String', '', '', '')}
      expected_formats = {'FI_2': Format('FI_2', 1, 'Integer', '')}
      expected = vcf_header_io.VcfHeader(
          infos=expected_infos, formats=expected_formats)
      assert_that(inferred_headers, equal_to([expected]))
      p.run()
