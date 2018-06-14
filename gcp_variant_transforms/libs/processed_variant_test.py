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
from typing import Dict  # pylint: disable=unused-import

import unittest

from vcf import parser

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import metrics_util
from gcp_variant_transforms.libs import processed_variant
# This is intentionally breaking the style guide because without this the lines
# referencing the counter names are too long and hard to read.
from gcp_variant_transforms.libs.processed_variant import _CounterEnum as CEnum


class _CounterSpy(metrics_util.CounterInterface):

  def __init__(self):
    self._count = 0

  def inc(self, n=1):
    self._count += n

  def get_value(self):
    return self._count


class _CounterSpyFactory(metrics_util.CounterFactoryInterface):

  def __init__(self):
    self.counter_map = {}  # type: Dict[str, _CounterSpy]

  def create_counter(self, counter_name):
    assert counter_name not in self.counter_map
    counter = _CounterSpy()
    self.counter_map[counter_name] = counter
    return counter


class ProcessedVariantFactoryTest(unittest.TestCase):

  def _get_sample_variant(self):
    return vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['A', 'TT'], names=['rs1', 'rs2'], quality=2,
        filters=['PASS'],
        info={'A1': 'some data', 'A2': ['data1', 'data2']},
        calls=[
            vcfio.VariantCall(name='Sample1', genotype=[0, 1],
                              info={'GQ': 20, 'HQ': [10, 20]}),
            vcfio.VariantCall(name='Sample2', genotype=[1, 0],
                              info={'GQ': 10, 'FLAG1': True})])

  def _make_header(self, key_num_dict):
    infos_dict = {}
    for k, v in key_num_dict.iteritems():
      if v == '.':
        infos_dict[k] = parser._Info(
            None, None, None,
            'some desc Allele|Consequence|IMPACT|SYMBOL|Gene', None, None)
      elif v == 'A':
        infos_dict[k] = parser._Info(None, -1, None, '', None, None)
      elif v == 'G':
        infos_dict[k] = parser._Info(None, -2, None, '', None, None)
      elif v == 'R':
        infos_dict[k] = parser._Info(None, -3, None, '', None, None)
      elif int(v) <= 4 and int(v) >= 1:
        infos_dict[k] = parser._Info(None, int(v), None, '', None, None)
      else:
        self.fail("given NUM value is not valid: " + v)
    return vcf_header_io.VcfHeader(infos=infos_dict)

  def test_create_processed_variant_no_change(self):
    variant = self._get_sample_variant()
    header_fields = self._make_header({'A1': '1', 'A2': 'A'})
    counter_factory = _CounterSpyFactory()
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=False,
        counter_factory=counter_factory)
    proc_var = factory.create_processed_variant(variant)
    # In this mode, the only difference between the original `variant` and
    # `proc_var` should be that INFO fields are copied to `_non_alt_info` map
    # and `_alternate_datas` are filled with alternate bases information only.
    proc_var_synthetic = processed_variant.ProcessedVariant(variant)
    proc_var_synthetic._non_alt_info = {'A1': 'some data'}
    alt1 = processed_variant.AlternateBaseData('A')
    alt1._info['A2'] = 'data1'
    alt2 = processed_variant.AlternateBaseData('TT')
    alt2._info['A2'] = 'data2'
    proc_var_synthetic._alternate_datas = [alt1, alt2]

    self.assertEqual([proc_var_synthetic], [proc_var])
    self.assertEqual(counter_factory.counter_map[
        CEnum.VARIANT.value].get_value(), 1)
    self.assertEqual(counter_factory.counter_map[
        CEnum.ANNOTATION_ALT_MATCH.value].get_value(), 0)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MISMATCH.value].get_value(), 0)

  def test_create_processed_variant_move_alt_info(self):
    variant = self._get_sample_variant()
    header_fields = self._make_header({'A1': '1', 'A2': 'A'})
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

  def _get_sample_variant_and_header_with_csq(self):
    variant = self._get_sample_variant()
    variant.info['CSQ'] = ['A|C1|I1|S1|G1', 'TT|C2|I2|S2|G2', 'A|C3|I3|S3|G3']
    header_fields = self._make_header({'CSQ': '.', 'A1': '1', 'A2': 'A'})
    return variant, header_fields

  def test_create_processed_variant_move_alt_info_and_annotation(self):
    variant, header_fields = self._get_sample_variant_and_header_with_csq()
    counter_factory = _CounterSpyFactory()
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=True,
        annotation_fields=['CSQ'],
        counter_factory=counter_factory)
    proc_var = factory.create_processed_variant(variant)
    alt1 = processed_variant.AlternateBaseData('A')
    alt1._info = {
        'A2': 'data1',
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'A', 'Consequence': 'C1',
             'IMPACT': 'I1', 'SYMBOL': 'S1', 'Gene': 'G1'},
            {processed_variant._ANNOTATION_ALT: 'A', 'Consequence': 'C3',
             'IMPACT': 'I3', 'SYMBOL': 'S3', 'Gene': 'G3'}]
    }
    alt2 = processed_variant.AlternateBaseData('TT')
    alt2._info = {
        'A2': 'data2',
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'TT', 'Consequence': 'C2',
             'IMPACT': 'I2', 'SYMBOL': 'S2', 'Gene': 'G2'}]
    }
    self.assertEqual(proc_var.alternate_data_list, [alt1, alt2])
    self.assertFalse(proc_var.non_alt_info.has_key('A2'))
    self.assertFalse(proc_var.non_alt_info.has_key('CSQ'))
    self.assertEqual(counter_factory.counter_map[
        CEnum.VARIANT.value].get_value(), 1)
    self.assertEqual(counter_factory.counter_map[
        CEnum.ANNOTATION_ALT_MATCH.value].get_value(), 2)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MISMATCH.value].get_value(), 0)

  def test_create_processed_variant_mismatched_annotation_alt(self):
    # This is like `test_create_processed_variant_move_alt_info_and_annotation`
    # with the difference that it has an extra alt annotation which does not
    # match any alts.
    variant, header_fields = self._get_sample_variant_and_header_with_csq()
    variant.info['CSQ'] = ['A|C1|I1|S1|G1', 'TT|C2|I2|S2|G2',
                           'A|C3|I3|S3|G3', 'ATAT|C3|I3|S3|G3']
    counter_factory = _CounterSpyFactory()
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=True,
        annotation_fields=['CSQ'],
        counter_factory=counter_factory)
    proc_var = factory.create_processed_variant(variant)
    alt1 = processed_variant.AlternateBaseData('A')
    alt1._info = {
        'A2': 'data1',
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'A', 'Consequence': 'C1',
             'IMPACT': 'I1', 'SYMBOL': 'S1', 'Gene': 'G1'},
            {processed_variant._ANNOTATION_ALT: 'A', 'Consequence': 'C3',
             'IMPACT': 'I3', 'SYMBOL': 'S3', 'Gene': 'G3'}]
    }
    alt2 = processed_variant.AlternateBaseData('TT')
    alt2._info = {
        'A2': 'data2',
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'TT', 'Consequence': 'C2',
             'IMPACT': 'I2', 'SYMBOL': 'S2', 'Gene': 'G2'}]
    }
    self.assertEqual(proc_var.alternate_data_list, [alt1, alt2])
    self.assertFalse(proc_var.non_alt_info.has_key('A2'))
    self.assertFalse(proc_var.non_alt_info.has_key('CSQ'))
    self.assertEqual(counter_factory.counter_map[
        CEnum.VARIANT.value].get_value(), 1)
    self.assertEqual(counter_factory.counter_map[
        CEnum.ANNOTATION_ALT_MATCH.value].get_value(), 2)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MISMATCH.value].get_value(), 1)

  def test_create_processed_variant_symbolic_and_breakend_annotation_alt(self):
    # The returned variant is ignored as we create a custom one next.
    _, header_fields = self._get_sample_variant_and_header_with_csq()
    variant = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['<SYMBOLIC>', '[13:123457[.', 'C[10:10357[.'],
        names=['rs1'], quality=2,
        filters=['PASS'],
        info={'CSQ': [
            'SYMBOLIC|C1|I1|S1|G1', '[13|C2|I2|S2|G2', 'C[10|C3|I3|S3|G3',
            'C[1|C3|I3|S3|G3']}) # The last one does not match any alts.
    counter_factory = _CounterSpyFactory()
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=True,
        annotation_fields=['CSQ'],
        counter_factory=counter_factory)
    proc_var = factory.create_processed_variant(variant)
    alt1 = processed_variant.AlternateBaseData('<SYMBOLIC>')
    alt1._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'SYMBOLIC', 'Consequence': 'C1',
             'IMPACT': 'I1', 'SYMBOL': 'S1', 'Gene': 'G1'}]
    }
    alt2 = processed_variant.AlternateBaseData('[13:123457[.')
    alt2._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: '[13', 'Consequence': 'C2',
             'IMPACT': 'I2', 'SYMBOL': 'S2', 'Gene': 'G2'}]
    }
    alt3 = processed_variant.AlternateBaseData('C[10:10357[.')
    alt3._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'C[10', 'Consequence': 'C3',
             'IMPACT': 'I3', 'SYMBOL': 'S3', 'Gene': 'G3'}]
    }
    self.assertEqual(proc_var.alternate_data_list, [alt1, alt2, alt3])
    self.assertFalse(proc_var.non_alt_info.has_key('CSQ'))
    self.assertEqual(counter_factory.counter_map[
        CEnum.VARIANT.value].get_value(), 1)
    self.assertEqual(counter_factory.counter_map[
        CEnum.ANNOTATION_ALT_MATCH.value].get_value(), 3)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MISMATCH.value].get_value(), 1)

  def test_create_processed_variant_annotation_alt_prefix(self):
    # The returned variant is ignored as we create a custom one next.
    _, header_fields = self._get_sample_variant_and_header_with_csq()
    variant = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['CT', 'CC', 'CCC'],
        names=['rs1'], quality=2,
        filters=['PASS'],
        info={'CSQ': ['T|C1|I1|S1|G1', 'C|C2|I2|S2|G2', 'CC|C3|I3|S3|G3']})
    counter_factory = _CounterSpyFactory()
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=True,
        annotation_fields=['CSQ'],
        counter_factory=counter_factory)
    proc_var = factory.create_processed_variant(variant)
    alt1 = processed_variant.AlternateBaseData('CT')
    alt1._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'T', 'Consequence': 'C1',
             'IMPACT': 'I1', 'SYMBOL': 'S1', 'Gene': 'G1'}]
    }
    alt2 = processed_variant.AlternateBaseData('CC')
    alt2._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'C', 'Consequence': 'C2',
             'IMPACT': 'I2', 'SYMBOL': 'S2', 'Gene': 'G2'}]
    }
    alt3 = processed_variant.AlternateBaseData('CCC')
    alt3._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'CC', 'Consequence': 'C3',
             'IMPACT': 'I3', 'SYMBOL': 'S3', 'Gene': 'G3'}]
    }
    self.assertEqual(proc_var.alternate_data_list, [alt1, alt2, alt3])
    self.assertFalse(proc_var.non_alt_info.has_key('CSQ'))
    self.assertEqual(counter_factory.counter_map[
        CEnum.VARIANT.value].get_value(), 1)
    self.assertEqual(counter_factory.counter_map[
        CEnum.ANNOTATION_ALT_MATCH.value].get_value(), 3)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MISMATCH.value].get_value(), 0)

  def test_create_processed_variant_annotation_alt_long_prefix(self):
    # The returned variant is ignored as we create a custom one next.
    _, header_fields = self._get_sample_variant_and_header_with_csq()
    variant = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='CC',
        alternate_bases=['CCT', 'CCC', 'CCCC'],
        names=['rs1'], quality=2,
        filters=['PASS'],
        info={'CSQ': ['CT|C1|I1|S1|G1', 'CC|C2|I2|S2|G2', 'CCC|C3|I3|S3|G3']})
    counter_factory = _CounterSpyFactory()
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=True,
        annotation_fields=['CSQ'],
        counter_factory=counter_factory)
    proc_var = factory.create_processed_variant(variant)
    alt1 = processed_variant.AlternateBaseData('CCT')
    alt1._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'CT', 'Consequence': 'C1',
             'IMPACT': 'I1', 'SYMBOL': 'S1', 'Gene': 'G1'}]
    }
    alt2 = processed_variant.AlternateBaseData('CCC')
    alt2._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'CC', 'Consequence': 'C2',
             'IMPACT': 'I2', 'SYMBOL': 'S2', 'Gene': 'G2'}]
    }
    alt3 = processed_variant.AlternateBaseData('CCCC')
    alt3._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'CCC', 'Consequence': 'C3',
             'IMPACT': 'I3', 'SYMBOL': 'S3', 'Gene': 'G3'}]
    }
    self.assertEqual(proc_var.alternate_data_list, [alt1, alt2, alt3])
    self.assertFalse(proc_var.non_alt_info.has_key('CSQ'))
    self.assertEqual(counter_factory.counter_map[
        CEnum.VARIANT.value].get_value(), 1)
    self.assertEqual(counter_factory.counter_map[
        CEnum.ANNOTATION_ALT_MATCH.value].get_value(), 3)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MISMATCH.value].get_value(), 0)

  def test_create_processed_variant_annotation_alt_prefix_but_ref(self):
    # The returned variant is ignored as we create a custom one next.
    _, header_fields = self._get_sample_variant_and_header_with_csq()
    variant = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        alternate_bases=['AA', 'AAA'],
        names=['rs1'], quality=2,
        filters=['PASS'],
        info={'CSQ': ['AA|C1|I1|S1|G1', 'AAA|C2|I2|S2|G2']})
    counter_factory = _CounterSpyFactory()
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=True,
        annotation_fields=['CSQ'],
        counter_factory=counter_factory)
    proc_var = factory.create_processed_variant(variant)
    alt1 = processed_variant.AlternateBaseData('AA')
    alt1._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'AA', 'Consequence': 'C1',
             'IMPACT': 'I1', 'SYMBOL': 'S1', 'Gene': 'G1'}]
    }
    alt2 = processed_variant.AlternateBaseData('AAA')
    alt2._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'AAA', 'Consequence': 'C2',
             'IMPACT': 'I2', 'SYMBOL': 'S2', 'Gene': 'G2'}]
    }
    self.assertEqual(proc_var.alternate_data_list, [alt1, alt2])
    self.assertFalse(proc_var.non_alt_info.has_key('CSQ'))
    self.assertEqual(counter_factory.counter_map[
        CEnum.VARIANT.value].get_value(), 1)
    self.assertEqual(counter_factory.counter_map[
        CEnum.ANNOTATION_ALT_MATCH.value].get_value(), 2)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MISMATCH.value].get_value(), 0)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MINIMAL_MATCH.value].get_value(), 0)

  def test_create_processed_variant_annotation_alt_minimal(self):
    # The returned variant is ignored as we create a custom one next.
    _, header_fields = self._get_sample_variant_and_header_with_csq()
    variant = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='CC',
        # The following represent a SNV, an insertion, and a deletion, resp.
        alternate_bases=['CT', 'CCT', 'C'],
        names=['rs1'], quality=2,
        filters=['PASS'],
        # Note that in the minimal mode, 'T' is an ambiguous annotation ALT
        # because it can map to either the 'CT' SNV or the 'CCT' insertion.
        # It is not ambiguous in the non-minimal mode (it only maps to `CT`).
        info={'CSQ': ['T|C1|I1|S1|G1', '-|C2|I2|S2|G2']})
    counter_factory = _CounterSpyFactory()
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=True,
        annotation_fields=['CSQ'],
        minimal_match=True,
        counter_factory=counter_factory)
    proc_var = factory.create_processed_variant(variant)
    alt1 = processed_variant.AlternateBaseData('CT')
    alt1._info = {}
    alt2 = processed_variant.AlternateBaseData('CCT')
    alt2._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'T',
             processed_variant._ANNOTATION_ALT_AMBIGUOUS: True,
             'Consequence': 'C1', 'IMPACT': 'I1', 'SYMBOL': 'S1', 'Gene': 'G1'}]
    }
    alt3 = processed_variant.AlternateBaseData('C')
    alt3._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: '-',
             processed_variant._ANNOTATION_ALT_AMBIGUOUS: False,
             'Consequence': 'C2', 'IMPACT': 'I2', 'SYMBOL': 'S2', 'Gene': 'G2'}]
    }
    self.assertEqual(proc_var.alternate_data_list, [alt1, alt2, alt3])
    self.assertFalse(proc_var.non_alt_info.has_key('CSQ'))
    self.assertEqual(counter_factory.counter_map[
        CEnum.VARIANT.value].get_value(), 1)
    self.assertEqual(counter_factory.counter_map[
        CEnum.ANNOTATION_ALT_MATCH.value].get_value(), 0)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MISMATCH.value].get_value(), 0)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MINIMAL_MATCH.value].get_value(), 2)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MINIMAL_AMBIGUOUS.value].get_value(), 1)

  def test_create_processed_variant_annotation_alt_allele_num(self):
    csq_info = parser._Info(
        id=None, num='.', type=None,
        desc='some desc Allele|Consequence|IMPACT|ALLELE_NUM',
        source=None, version=None)
    header_fields = vcf_header_io.VcfHeader(infos={'CSQ': csq_info})
    variant = vcfio.Variant(
        reference_name='19', start=11, end=12, reference_bases='C',
        # The following represent a SNV and an insertion, resp.
        alternate_bases=['T', 'CT'],
        names=['rs1'], quality=2,
        filters=['PASS'],
        # Note that in the minimal mode of VEP, 'T' is an ambiguous annotation
        # ALT because it can map to either the 'T' SNV or the 'CT' insertion.
        # But because there is ALLELE_NUM there should be no ambiguity.
        # The last four annotations have incorrect ALLELE_NUMs.
        info={'CSQ': ['T|C1|I1|1', 'T|C2|I2|2', 'T|C3|I3|0', 'T|C4|I4|3',
                      'T|C5|I5|TEST', 'T|C6|I6|']})
    counter_factory = _CounterSpyFactory()
    factory = processed_variant.ProcessedVariantFactory(
        header_fields,
        split_alternate_allele_info_fields=True,
        annotation_fields=['CSQ'],
        use_allele_num=True,
        minimal_match=True,  # This should be ignored by the factory method.
        counter_factory=counter_factory)
    proc_var = factory.create_processed_variant(variant)
    alt1 = processed_variant.AlternateBaseData('T')
    alt1._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'T',
             'Consequence': 'C1', 'IMPACT': 'I1', 'ALLELE_NUM': '1'}]
    }
    alt2 = processed_variant.AlternateBaseData('CT')
    alt2._info = {
        'CSQ': [
            {processed_variant._ANNOTATION_ALT: 'T',
             'Consequence': 'C2', 'IMPACT': 'I2', 'ALLELE_NUM': '2'}]
    }
    self.assertEqual(proc_var.alternate_data_list, [alt1, alt2])
    self.assertFalse(proc_var.non_alt_info.has_key('CSQ'))
    self.assertEqual(counter_factory.counter_map[
        CEnum.VARIANT.value].get_value(), 1)
    self.assertEqual(counter_factory.counter_map[
        CEnum.ANNOTATION_ALT_MATCH.value].get_value(), 2)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MISMATCH.value].get_value(), 0)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MINIMAL_MATCH.value].get_value(), 0)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ANNOTATION_ALT_MINIMAL_AMBIGUOUS.value].get_value(), 0)
    self.assertEqual(
        counter_factory.counter_map[
            CEnum.ALLELE_NUM_INCORRECT.value].get_value(), 4)


# TODO(bashir2): Add tests for create_alt_record_for_schema.
