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

"""Test cases for conflicts_reporter module."""

from collections import OrderedDict
from typing import List  # pylint: disable=unused-import
import unittest

from apache_beam.io.filesystems import FileSystems
from vcf.parser import _Format as Format
from vcf.parser import _Info as Info

from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeader
from gcp_variant_transforms.libs import conflicts_reporter
from gcp_variant_transforms.testing import temp_dir
from gcp_variant_transforms.transforms.merge_header_definitions import VcfHeaderDefinitions
from gcp_variant_transforms.transforms.merge_header_definitions import Definition


class ConflictsReporterTest(unittest.TestCase):

  # TODO(yifangchen): Move the default report name to be the default value of
  # the path argument.
  _REPORT_NAME = 'header_conflicts_report.csv'

  def _generate_report_and_assert_contents_equal(self,
                                                 expected_content,
                                                 header_definitions,
                                                 resolved_headers=None,
                                                 inferred_headers=None):
    # type: (List[str], VcfHeaderDefinitions, VcfHeader, VcfHeader) -> None
    with temp_dir.TempDir() as tempdir:
      file_path = FileSystems.join(tempdir.get_path(), self._REPORT_NAME)
      conflicts_reporter.generate_conflicts_report(header_definitions,
                                                   file_path,
                                                   resolved_headers,
                                                   inferred_headers)
      with FileSystems.open(file_path) as f:
        reader = f.readlines()
        self.assertItemsEqual(reader, expected_content)

  def test_report_no_conflicts(self):
    header_definitions = VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Float'): ['file1']}}
    header_definitions._formats = {'NS': {Definition(1, 'Float'): ['file2']}}

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Integer', 'Number samples', None, None))])
    formats = OrderedDict([('NS', Format('NS', 1, 'Float', 'Number samples'))])
    resolved_headers = VcfHeader(infos=infos, formats=formats)

    expected = [conflicts_reporter._NO_CONFLICTS_MESSAGE]
    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions,
                                                    resolved_headers)

  def test_report_conflicts(self):
    header_definitions = VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Integer'): ['file1'],
                                        Definition(1, 'Float'): ['file2']}}

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Float', 'Number samples', None, None))])
    resolved_headers = VcfHeader(infos=infos)

    expected = [
        conflicts_reporter._HEADER_LINE,
        ('NS;'
         'num=1 type=Float in [\'file2\'], num=1 type=Integer in [\'file1\'];'
         'num=1 type=Float')
    ]
    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions,
                                                    resolved_headers)

  def test_report_multiple_files(self):
    header_definitions = VcfHeaderDefinitions()
    header_definitions._infos = {
        'NS': {Definition(1, 'Float'): ['file1', 'file2'],
               Definition(1, 'Integer'): ['file3']}
    }

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Float', 'Number samples', None, None))])
    resolved_headers = VcfHeader(infos=infos)

    expected = [
        conflicts_reporter._HEADER_LINE,
        ('NS;'
         'num=1 type=Float in [\'file1\', \'file2\'], '
         'num=1 type=Integer in [\'file3\'];'
         'num=1 type=Float')
    ]
    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions,
                                                    resolved_headers)

  def test_report_multiple_fields(self):
    header_definitions = VcfHeaderDefinitions()
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
        conflicts_reporter._HEADER_LINE,
        ('DP;'
         'num=2 type=Float in [\'file3\'], num=2 type=Integer in [\'file4\'];'
         'num=2 type=Float\n'),
        ('NS;'
         'num=1 type=Float in [\'file1\'], num=1 type=Integer in [\'file2\'];'
         'num=1 type=Float'),
    ]
    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions,
                                                    resolved_headers)

  def test_report_no_resolved_headers(self):
    header_definitions = VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Float'): ['file1'],
                                        Definition(1, 'Integer'): ['file2']}}

    expected = [
        conflicts_reporter._HEADER_LINE,
        ('NS;'
         'num=1 type=Float in [\'file1\'], num=1 type=Integer in [\'file2\'];'
         'Not resolved.')
    ]

    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions)

  def test_report_inferred_headers_only(self):
    header_definitions = VcfHeaderDefinitions()
    formats = OrderedDict([('DP', Format('DP', 2, 'Float', 'Total Depth'))])

    inferred_headers = VcfHeader(formats=formats)
    expected = [
        conflicts_reporter._HEADER_LINE,
        ('DP;Undefined header.;num=2 type=Float')
    ]
    self._generate_report_and_assert_contents_equal(
        expected, header_definitions, inferred_headers=inferred_headers)

  def test_report_conflicted_and_inferred_headers(self):
    header_definitions = VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Float'): ['file1'],
                                        Definition(1, 'Integer'): ['file2']}}

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Float', 'Number samples', None, None))])
    formats = OrderedDict([
        ('DP', Format('DP', 2, 'Float', 'Total Depth'))])
    resolved_headers = VcfHeader(infos=infos, formats=formats)
    inferred_headers = VcfHeader(formats=formats)
    expected = [
        conflicts_reporter._HEADER_LINE,
        ('NS;'
         'num=1 type=Float in [\'file1\'], num=1 type=Integer in [\'file2\'];'
         'num=1 type=Float\n'),
        ('DP;'
         'Undefined header.;'
         'num=2 type=Float')
    ]
    self._generate_report_and_assert_contents_equal(expected,
                                                    header_definitions,
                                                    resolved_headers,
                                                    inferred_headers)
