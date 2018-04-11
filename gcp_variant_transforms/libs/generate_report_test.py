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

"""Test cases for generate_report module."""

from collections import OrderedDict
import csv
import os
import unittest

from vcf.parser import _Format as Format
from vcf.parser import _Info as Info

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs.generate_report import ConflictsReporter
from gcp_variant_transforms.transforms.merge_header_definitions import VcfHeaderDefinitions
from gcp_variant_transforms.transforms.merge_header_definitions import Definition


class GenerateReportTest(unittest.TestCase):

  def test_report_no_conflicts(self):
    header_definitions = VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Float'): ['file1']}}
    header_definitions._formats = {'NS': {Definition(1, 'Float'): ['file2']}}

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Integer', 'Number samples', None, None))])
    formats = OrderedDict([('NS', Format('NS', 1, 'Float', 'Number samples'))])
    resolved_header = vcf_header_io.VcfHeader(infos=infos, formats=formats)

    ConflictsReporter().report_conflicts(header_definitions, resolved_header)

    report = ConflictsReporter._REPORT_NAME
    with open(report) as f:
      s = []
      reader = csv.reader(f)
      for row in reader:
        s.append(row)
      self.assertEqual(s, [ConflictsReporter._NO_CONFLICTS_MESSAGE])
    os.remove(report)

  def test_report_conflicts(self):
    header_definitions = VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Integer'): ['file1'],
                                        Definition(1, 'Float'): ['file2']}}

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Float', 'Number samples', None, None))])
    resolved_header = vcf_header_io.VcfHeader(infos=infos)

    ConflictsReporter('test').report_conflicts(header_definitions,
                                               resolved_header)

    expected = [
        ConflictsReporter._HEADER_LINE,
        ['NS',
         'num=1 type=Float in [\'file2\'] num=1 type=Integer in [\'file1\']',
         'num=1 type=Float']
    ]
    report = os.path.join('test', ConflictsReporter._REPORT_NAME)
    with open(report) as f:
      s = []
      reader = csv.reader(f)
      for row in reader:
        s.append(row)
      self.assertEqual(s, expected)
    os.remove(report)

  def test_report_multiple_files(self):
    header_definitions = VcfHeaderDefinitions()
    header_definitions._infos = {
        'NS': {Definition(1, 'Float'): ['file1', 'file2'],
               Definition(1, 'Integer'): ['file3']}
    }

    infos = OrderedDict([
        ('NS', Info('NS', 1, 'Float', 'Number samples', None, None))])
    resolved_header = vcf_header_io.VcfHeader(infos=infos)

    ConflictsReporter().report_conflicts(header_definitions, resolved_header)

    expected = [
        ConflictsReporter._HEADER_LINE,
        ['NS',
         'num=1 type=Float in [\'file1\', \'file2\'] '
         'num=1 type=Integer in [\'file3\']',
         'num=1 type=Float']
    ]
    report = ConflictsReporter._REPORT_NAME
    with open(report) as f:
      s = []
      reader = csv.reader(f)
      for row in reader:
        s.append(row)
      self.assertEqual(s, expected)
    os.remove(report)

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
    resolved_header = vcf_header_io.VcfHeader(infos=infos, formats=formats)

    ConflictsReporter().report_conflicts(header_definitions, resolved_header)

    expected = [
        ConflictsReporter._HEADER_LINE,
        ['NS',
         'num=1 type=Float in [\'file1\'] num=1 type=Integer in [\'file2\']',
         'num=1 type=Float'],
        ['DP',
         'num=2 type=Float in [\'file3\'] num=2 type=Integer in [\'file4\']',
         'num=2 type=Float'],
    ]
    report = ConflictsReporter._REPORT_NAME
    with open(report) as f:
      s = []
      reader = csv.reader(f)
      for row in reader:
        s.append(row)
      self.assertItemsEqual(s, expected)
    os.remove(report)

  def test_report_no_representative_header(self):
    header_definitions = VcfHeaderDefinitions()
    header_definitions._infos = {'NS': {Definition(1, 'Float'): ['file1'],
                                        Definition(1, 'Integer'): ['file2']}}

    ConflictsReporter().report_conflicts(header_definitions)

    expected = [
        ConflictsReporter._HEADER_LINE,
        ['NS',
         'num=1 type=Float in [\'file1\'] '
         'num=1 type=Integer in [\'file2\']',
         ConflictsReporter._NO_SOLUTION_MESSAGE]
    ]
    report = ConflictsReporter._REPORT_NAME
    with open(report) as f:
      s = []
      reader = csv.reader(f)
      for row in reader:
        s.append(row)
      self.assertEqual(s, expected)
    os.remove(report)
