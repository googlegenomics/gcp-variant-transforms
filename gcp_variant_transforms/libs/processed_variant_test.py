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

from __future__ import absolute_import

import unittest

from vcf import parser

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_header_parser

class ProcessedVariantFactoryTest(unittest.TestCase):

  def _get_sample_variant(self):
    return vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'A1': vcfio.VariantInfo('some data', '1'),
              'A2': vcfio.VariantInfo(['data1', 'data2'], 'A')},
        calls=[
            vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                              info={'GQ': 20, 'HQ': [10, 20]}),
            vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                              info={'GQ': 10, 'FLAG1': True})])

  def test_create_processed_variant_no_change(self):
    variant = self._get_sample_variant()
    header_fields = vcf_header_parser.HeaderFields({}, {})
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=False)
    proc_var = factory.create_processed_variant(variant)
    # In this mode, the only difference between the original `variant` and
    # `proc_var` should be that INFO fields are copied to `_non_alt_info` map
    # and `_alternate_datas` are filled with alternate bases information only.
    proc_var_synthetic = processed_variant.ProcessedVariant(variant)
    proc_var_synthetic._non_alt_info = {'A1': 'some data',
                                        'A2': ['data1', 'data2']}
    proc_var_synthetic._alternate_datas = [
        processed_variant.AlternateBaseData(a) for a in ['A', 'TT']]
    self.assertEqual([proc_var_synthetic], [proc_var])

  def test_create_processed_variant_move_alt_info(self):
    variant = self._get_sample_variant()
    header_fields = vcf_header_parser.HeaderFields({}, {})
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=True)
    proc_var = factory.create_processed_variant(variant)
    alt1 = processed_variant.AlternateBaseData('A')
    alt1._info = {'A2': 'data1'}
    alt2 = processed_variant.AlternateBaseData('TT')
    alt2._info = {'A2': 'data2'}
    self.assertEqual(proc_var.alternate_data_list, [alt1, alt2])
    self.assertFalse(proc_var.non_alt_info.has_key('A2'))

  def test_create_processed_variant_move_alt_info_and_annotation(self):
    variant = self._get_sample_variant()
    variant.info['CSQ'] = vcfio.VariantInfo(
        data=['A|C1|I1|S1|G1', 'TT|C2|I2|S2|G2', 'A|C3|I3|S3|G3'],
        field_count='.')
    csq_info = parser._Info(
        id=None,
        num='.',
        type=None,
        desc='some desc Allele|Consequence|IMPACT|SYMBOL|Gene',
        source=None,
        version=None)
    header_fields = vcf_header_parser.HeaderFields(
        infos={'CSQ': csq_info},
        formats={})
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=True,
        annotation_fields=['CSQ'])
    proc_var = factory.create_processed_variant(variant)
    alt1 = processed_variant.AlternateBaseData('A')
    alt1._info = {
        'A2': 'data1',
        'CSQ': [
            {'Consequence': 'C1', 'IMPACT': 'I1', 'SYMBOL': 'S1', 'Gene': 'G1'},
            {'Consequence': 'C3', 'IMPACT': 'I3', 'SYMBOL': 'S3', 'Gene': 'G3'}]
    }
    alt2 = processed_variant.AlternateBaseData('TT')
    alt2._info = {
        'A2': 'data2',
        'CSQ': [
            {'Consequence': 'C2', 'IMPACT': 'I2', 'SYMBOL': 'S2', 'Gene': 'G2'}]
    }
    self.assertEqual(proc_var.alternate_data_list, [alt1, alt2])
    self.assertFalse(proc_var.non_alt_info.has_key('A2'))
    self.assertFalse(proc_var.non_alt_info.has_key('CSQ'))

# TODO(bashir2): Add tests for create_alt_record_for_schema.
