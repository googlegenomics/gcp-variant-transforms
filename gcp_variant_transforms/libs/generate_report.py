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

"""Generates conflicts report."""

import csv
import os
from typing import Dict, List, Union  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeader  # pylint: disable=unused-import
from gcp_variant_transforms.libs.vcf_field_conflict_resolver import VcfParserConstants
from gcp_variant_transforms.transforms.merge_header_definitions import Definition  # pylint: disable=unused-import
from gcp_variant_transforms.transforms.merge_header_definitions import VcfHeaderDefinitions  # pylint: disable=unused-import


class ConflictsReporter(object):
  """Generates the header conflicts report."""

  _HEADER_LINE = ['ID', 'Conflicts', 'Proposed Solution']
  _REPORT_NAME = 'header_conflicts_report.csv'
  _NO_CONFLICTS_MESSAGE = ['No conflicts found.']
  _NO_SOLUTION_MESSAGE = 'Not resolved'

  def __init__(self, directory=''):
    # type: (str) -> None
    self.directory = directory

  def report_conflicts(self, header_definitions, representative_header=None):
    # type: (VcfHeaderDefinitions, VcfHeader) -> None
    """Generates a report.

    Combines the conflicts extracted from ``header_definitions`` and the
    solution from `representative_header`` to generate a conflicts report in
    ``self.directory``.
    """
    representative_header = representative_header or VcfHeader()
    format_conflicts = self._extract_conflicts(header_definitions.formats)
    info_conflicts = self._extract_conflicts(header_definitions.infos)
    format_headers = representative_header.formats
    info_headers = representative_header.infos
    contents = list()
    contents.extend(self._generate_contents(format_conflicts, format_headers))
    contents.extend(self._generate_contents(info_conflicts, info_headers))
    self._write_to_report(contents)

  def _extract_conflicts(
      self,
      definitions  # type: Dict[str, Dict[Definition, List[str]]]
      ):
    # type: (...) -> Dict[str, Dict[Definition, List[str]]]
    """Extracts the fields that have conflicted definitions."""
    return dict([(k, v) for k, v in definitions.items() if len(v) > 1])

  def _generate_contents(
      self,
      conflicts,  # type: Dict[str, Dict[Definition, List[str]]]
      headers  # type: Dict[str, Dict[str, Union[str, int]]
      ):
    # type: (...) -> List[str]
    contents = list()
    for field_id, definitions_to_files_map in conflicts.iteritems():
      row = [
          field_id,
          self._generate_conflicts(definitions_to_files_map),
          self._generate_solution(headers, field_id)
      ]
      contents.append(row)
    return contents

  def _generate_conflicts(self, definition_to_files_map):
    # type: (Dict[Definition, List[str]]) -> str
    conflict_definitions = []
    for definition, file_names in definition_to_files_map.iteritems():
      definition = self._format_definition(definition.num, definition.type)
      conflict_definitions.append(definition + ' in ' + str(file_names))
    return ' '.join(conflict_definitions)

  def _generate_solution(self, header, filed_id):
    # type: (Dict[str, Dict[str, Union[str, int]]], str) -> str
    if filed_id not in header:
      return self._NO_SOLUTION_MESSAGE
    return self._format_definition(header[filed_id][VcfParserConstants.NUM],
                                   header[filed_id][VcfParserConstants.TYPE])

  def _format_definition(self, num_value, type_value):
    # type: (Union[str, int], str) -> str
    formatted_definition = [
        VcfParserConstants.NUM + '=' + str(num_value),
        VcfParserConstants.TYPE + '=' + str(type_value)
    ]
    return ' '.join(formatted_definition)

  def _write_to_report(self, contents):
    # type: (List[str], str) -> None
    if self.directory and not os.path.exists(self.directory):
      os.makedirs(self.directory)
    file_path = os.path.join(self.directory, self._REPORT_NAME)
    with open(file_path, 'w') as file_to_write:
      writer = csv.writer(file_to_write)
      if not contents:
        writer.writerow(self._NO_CONFLICTS_MESSAGE)
        return
      writer.writerow(self._HEADER_LINE)
      for row in contents:
        writer.writerow(row)
