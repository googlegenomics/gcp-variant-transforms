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

import csv
import os
import unittest
import vcf

import apache_beam as beam
from apache_beam.pvalue import AsSingleton
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import Create

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs.generate_report import ConflictsReporter
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.transforms import merge_header_definitions


class GenerateReportTest(unittest.TestCase):

  def _get_vcf_header_from_reader(self, reader, file_name):
    return vcf_header_io.VcfHeader(infos=reader.infos,
                                   filters=reader.filters,
                                   alts=reader.alts,
                                   formats=reader.formats,
                                   contigs=reader.contigs,
                                   file_name=file_name)

  def test_report_no_conflicts(self):
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##FORMAT=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]
    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    vcf_reader_2 = vcf.Reader(fsock=iter(lines_2))
    headers_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    headers_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')

    pipeline = TestPipeline()
    pcoll = (pipeline | Create([headers_1, headers_2]))
    merged_definitions = (pcoll | merge_header_definitions.MergeDefinitions())
    merged_headers = (pcoll | merge_headers.MergeHeaders())
    _ = (merged_definitions | beam.ParDo(ConflictsReporter().report_conflicts,
                                         AsSingleton(merged_headers)))
    pipeline.run()

    report = ConflictsReporter._REPORT_NAME
    with open(report) as f:
      s = []
      reader = csv.reader(f)
      for row in reader:
        s.append(row)
      self.assertEqual(s, [ConflictsReporter._NO_CONFLICTS_MESSAGE])
    os.remove(report)

  def test_report_conflicts(self):
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]
    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    vcf_reader_2 = vcf.Reader(fsock=iter(lines_2))
    headers_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    headers_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')

    pipeline = TestPipeline()
    pcoll = (pipeline | Create([headers_1, headers_2]))
    merged_definitions = (pcoll | merge_header_definitions.MergeDefinitions())
    merged_headers = (pcoll | merge_headers.MergeHeaders())
    _ = (merged_definitions |
         beam.ParDo(ConflictsReporter('test').report_conflicts,
                    AsSingleton(merged_headers)))
    pipeline.run()

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
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]
    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    vcf_reader_2 = vcf.Reader(fsock=iter(lines_2))
    headers_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    headers_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')
    headers_3 = self._get_vcf_header_from_reader(vcf_reader_2, 'file3')

    pipeline = TestPipeline()
    pcoll = (pipeline | Create([headers_1, headers_2, headers_3]))
    merged_definitions = (pcoll | merge_header_definitions.MergeDefinitions())
    merged_headers = (pcoll | merge_headers.MergeHeaders())
    _ = (merged_definitions | beam.ParDo(ConflictsReporter().report_conflicts,
                                         AsSingleton(merged_headers)))
    pipeline.run()

    expected = [
        ConflictsReporter._HEADER_LINE,
        ['NS',
         'num=1 type=Float in [\'file2\', \'file3\'] '
         'num=1 type=Integer in [\'file1\']',
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
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]
    lines_3 = [
        '##FORMAT=<ID=DP,Number=2,Type=Float,Description="Total Depth">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_4 = [
        '##FORMAT=<ID=DP,Number=2,Type=Integer,Description="Total Depth">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]
    lines = [lines_1, lines_2, lines_3, lines_4]
    file_names = ['file1', 'file2', 'file3', 'file4']
    headers = []
    for line, file_name in zip(lines, file_names):
      vcf_reader = vcf.Reader(fsock=iter(line))
      headers.append(self._get_vcf_header_from_reader(vcf_reader, file_name))

    pipeline = TestPipeline()
    pcoll = (pipeline | Create(headers))
    merged_definitions = (pcoll | merge_header_definitions.MergeDefinitions())
    merged_headers = (pcoll | merge_headers.MergeHeaders())
    _ = (merged_definitions | beam.ParDo(ConflictsReporter().report_conflicts,
                                         AsSingleton(merged_headers)))
    pipeline.run()

    expected = [
        ConflictsReporter._HEADER_LINE,
        ['NS',
         'num=1 type=Float in [\'file2\'] num=1 type=Integer in [\'file1\']',
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
    lines_1 = [
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample1 Sample2\n'
    ]
    lines_2 = [
        '##INFO=<ID=NS,Number=1,Type=Float,Description="Number samples">\n',
        '#CHROM  POS ID  REF ALT QUAL  FILTER  INFO  FORMAT  Sample3\n'
    ]
    vcf_reader_1 = vcf.Reader(fsock=iter(lines_1))
    vcf_reader_2 = vcf.Reader(fsock=iter(lines_2))

    headers_1 = self._get_vcf_header_from_reader(vcf_reader_1, 'file1')
    headers_2 = self._get_vcf_header_from_reader(vcf_reader_2, 'file2')

    pipeline = TestPipeline()
    pcoll = (pipeline | Create([headers_1, headers_2]))
    merged_definitions = (pcoll | merge_header_definitions.MergeDefinitions())
    _ = (merged_definitions | beam.ParDo(ConflictsReporter().report_conflicts))
    pipeline.run()

    expected = [
        ConflictsReporter._HEADER_LINE,
        ['NS',
         'num=1 type=Float in [\'file2\'] '
         'num=1 type=Integer in [\'file1\']',
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
