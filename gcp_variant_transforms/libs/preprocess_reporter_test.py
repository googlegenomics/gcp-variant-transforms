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

"""Test cases for preprocess_reporter module."""

from collections import OrderedDict
from typing import List  # pylint: disable=unused-import
import unittest

from apache_beam.io.filesystems import FileSystems
from vcf.parser import _Format as Format
from vcf.parser import _Info as Info

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeader
from gcp_variant_transforms.libs import preprocess_reporter
from gcp_variant_transforms.testing import temp_dir
from gcp_variant_transforms.transforms import merge_header_definitions
from gcp_variant_transforms.transforms.merge_header_definitions import Definition


class PreprocessReporterTest(unittest.TestCase):
  _REPORT_NAME = 'report.tsv'

  def _generate_report_and_assert_contents_equal(
      self,
      expected_content,  # type: List[str]
      header_definitions,  # type: merge_header_definitions.VcfHeaderDefinitions
      resolved_headers=None,  # type: VcfHeader
      inferred_headers=None,  # type: VcfHeader
      malformed_records=None  # type: List[vcfio.MalformedVcfRecord]
      ):
    # type: (...) -> None
    with temp_dir.TempDir() as tempdir:
      file_path = FileSystems.join(tempdir.get_path(),
                                   PreprocessReporterTest._REPORT_NAME)
      preprocess_reporter.generate_report(header_definitions,
                                          file_path,
                                          resolved_headers,
                                          inferred_headers,
                                          malformed_records)
      with FileSystems.open(file_path) as f:
        reader = f.readlines()
        self.assertEqual(reader, expected_content)

  def test_report_no_conflicts(self):
    header_definitions = merge_header_definitions.VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Float'): ['file1']}}
    header_definitions._formats = {'NS': {Definition(1, 'Float'): ['file2']}}

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Integer', 'Number samples', None, None))])
    formats = OrderedDict([('NS', Format('NS', 1, 'Float', 'Number samples'))])
    resolved_headers = VcfHeader(infos=infos, formats=formats)

    expected = ['No Header Conflicts Found.\n', '\n']
    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions,
                                                    resolved_headers)

  def test_report_conflicts(self):
    header_definitions = merge_header_definitions.VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Integer'): ['file1'],
                                        Definition(1, 'Float'): ['file2']}}

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Float', 'Number samples', None, None))])
    resolved_headers = VcfHeader(infos=infos)

    expected = [
        preprocess_reporter._InconsistencyType.HEADER_CONFLICTS + '\n',
        preprocess_reporter._HeaderLine.CONFLICTS_HEADER + '\n',
        (preprocess_reporter._DELIMITER).join([
            'NS', 'INFO', 'num=1 type=Float', 'file2', 'num=1 type=Float\n']),
        (preprocess_reporter._DELIMITER).join([
            ' ', ' ', 'num=1 type=Integer', 'file1', ' \n']),
        '\n'
    ]
    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions,
                                                    resolved_headers)

  def test_report_multiple_files(self):
    header_definitions = merge_header_definitions.VcfHeaderDefinitions()
    header_definitions._infos = {
        'NS': {Definition(1, 'Float'): ['file1', 'file2'],
               Definition(1, 'Integer'): ['file3']}
    }

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Float', 'Number samples', None, None))])
    resolved_headers = VcfHeader(infos=infos)

    expected = [
        preprocess_reporter._InconsistencyType.HEADER_CONFLICTS + '\n',
        preprocess_reporter._HeaderLine.CONFLICTS_HEADER + '\n',
        (preprocess_reporter._DELIMITER).join([
            'NS', 'INFO', 'num=1 type=Float', 'file1', 'num=1 type=Float\n']),
        (preprocess_reporter._DELIMITER).join([
            ' ', ' ', ' ', 'file2', ' \n']),
        (preprocess_reporter._DELIMITER).join([
            ' ', ' ', 'num=1 type=Integer', 'file3', ' \n']),
        '\n'
    ]
    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions,
                                                    resolved_headers)

  def test_report_multiple_fields(self):
    header_definitions = merge_header_definitions.VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Float'): ['file1'],
                                        Definition(1, 'Integer'): ['file2']}}
    header_definitions._formats = {'DP': {Definition(2, 'Float'): ['file3'],
                                          Definition(2, 'Integer'): ['file4']}}

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Float', 'Number samples', None, None))])
    formats = OrderedDict([
        ('DP', Format('DP', 2, 'Float', 'Total Depth'))])
    resolved_headers = VcfHeader(infos=infos, formats=formats)

    expected = [
        preprocess_reporter._InconsistencyType.HEADER_CONFLICTS + '\n',
        preprocess_reporter._HeaderLine.CONFLICTS_HEADER + '\n',
        (preprocess_reporter._DELIMITER).join([
            'NS', 'INFO', 'num=1 type=Float', 'file1', 'num=1 type=Float\n']),
        (preprocess_reporter._DELIMITER).join([
            ' ', ' ', 'num=1 type=Integer', 'file2', ' \n']),
        (preprocess_reporter._DELIMITER).join([
            'DP', 'FORMAT', 'num=2 type=Float', 'file3', 'num=2 type=Float\n']),
        (preprocess_reporter._DELIMITER).join([
            ' ', ' ', 'num=2 type=Integer', 'file4', ' \n']),
        '\n'

    ]
    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions,
                                                    resolved_headers)

  def test_report_no_resolved_headers(self):
    header_definitions = merge_header_definitions.VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Float'): ['file1'],
                                        Definition(1, 'Integer'): ['file2']}}

    expected = [
        preprocess_reporter._InconsistencyType.HEADER_CONFLICTS + '\n',
        preprocess_reporter._HeaderLine.CONFLICTS_HEADER + '\n',
        (preprocess_reporter._DELIMITER).join([
            'NS', 'INFO', 'num=1 type=Float', 'file1', 'Not resolved.\n']),
        (preprocess_reporter._DELIMITER).join([
            ' ', ' ', 'num=1 type=Integer', 'file2', ' \n']),
        '\n'
    ]

    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions)

  def test_report_inferred_headers_only(self):
    header_definitions = merge_header_definitions.VcfHeaderDefinitions()
    formats = OrderedDict([('DP', Format('DP', 2, 'Float', 'Total Depth'))])

    inferred_headers = VcfHeader(formats=formats)
    expected = [
        'No Header Conflicts Found.\n',
        '\n',
        preprocess_reporter._InconsistencyType.INFERRED_HEADERS + '\n',
        preprocess_reporter._HeaderLine.INFERRED_FIELD_HEADER + '\n',
        (preprocess_reporter._DELIMITER).join([
            'DP', 'FORMAT', 'num=2 type=Float\n']),
        '\n'
    ]
    self._generate_report_and_assert_contents_equal(
        expected, header_definitions, inferred_headers=inferred_headers)

  def test_report_conflicted_and_inferred_headers(self):
    header_definitions = merge_header_definitions.VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Float'): ['file1'],
                                        Definition(1, 'Integer'): ['file2']}}

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Float', 'Number samples', None, None))])
    formats = OrderedDict([
        ('DP', Format('DP', 2, 'Float', 'Total Depth'))])
    resolved_headers = VcfHeader(infos=infos, formats=formats)
    inferred_headers = VcfHeader(formats=formats)
    expected = [
        preprocess_reporter._InconsistencyType.HEADER_CONFLICTS + '\n',
        preprocess_reporter._HeaderLine.CONFLICTS_HEADER + '\n',
        (preprocess_reporter._DELIMITER).join([
            'NS', 'INFO', 'num=1 type=Float', 'file1', 'num=1 type=Float\n']),
        (preprocess_reporter._DELIMITER).join([
            ' ', ' ', 'num=1 type=Integer', 'file2', ' \n']),
        '\n',
        preprocess_reporter._InconsistencyType.INFERRED_HEADERS + '\n',
        preprocess_reporter._HeaderLine.INFERRED_FIELD_HEADER + '\n',
        (preprocess_reporter._DELIMITER).join([
            'DP', 'FORMAT', 'num=2 type=Float\n']),
        '\n'
    ]
    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions,
                                                    resolved_headers,
                                                    inferred_headers)

  def test_report_malformed_records(self):
    header_definitions = merge_header_definitions.VcfHeaderDefinitions()
    inferred_headers = VcfHeader()
    records = [vcfio.MalformedVcfRecord('file1',
                                        'rs6054257  G  A  29  PASS',
                                        'Invalid literal')]
    expected = [
        'No Header Conflicts Found.\n',
        '\n',
        'No Inferred Headers Found.\n',
        '\n',
        preprocess_reporter._InconsistencyType.MALFORMED_RECORDS + '\n',
        preprocess_reporter._HeaderLine.MALFORMED_RECORDS_HEADER + '\n',
        (preprocess_reporter._DELIMITER).join([
            'file1', 'rs6054257  G  A  29  PASS', 'Invalid literal\n']),
        '\n'
    ]
    self._generate_report_and_assert_contents_equal(
        expected, header_definitions, inferred_headers=inferred_headers,
        malformed_records=records)

  def test_report_no_inconsistencies(self):
    header_definitions = merge_header_definitions.VcfHeaderDefinitions()
    inferred_headers = VcfHeader()
    expected = [
        'No Header Conflicts Found.\n',
        '\n',
        'No Inferred Headers Found.\n',
        '\n',
        'No Malformed Records Found.\n',
        '\n'
    ]
    self._generate_report_and_assert_contents_equal(
        expected, header_definitions, inferred_headers=inferred_headers,
        malformed_records=[])
