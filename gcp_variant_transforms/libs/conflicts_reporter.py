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

"""Generates conflicts report.

The report is aimed to help the user to easily import the malformed/incompatible
VCF files. It contains the conflicted header definitions, the corresponding file
paths, and the suggested resolutions.
TODO(yifangchen): Eventually, it also contains the inferred header definitions
and the malformed records.
"""

from typing import Dict, List, Union  # pylint: disable=unused-import

from apache_beam.io.filesystems import FileSystems

from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeader  # pylint: disable=unused-import
from gcp_variant_transforms.beam_io.vcf_header_io import VcfParserHeaderKeyConstants
from gcp_variant_transforms.transforms.merge_header_definitions import Definition  # pylint: disable=unused-import
from gcp_variant_transforms.transforms.merge_header_definitions import VcfHeaderDefinitions  # pylint: disable=unused-import


_HEADER_LINE = 'ID;Conflicts;Proposed Resolution\n'
_NO_CONFLICTS_MESSAGE = 'No conflicts found.'
_NO_SOLUTION_MESSAGE = 'Not resolved'


def generate_conflicts_report(file_path,
                              header_definitions,
                              resolved_headers=None):
  # type: (str, VcfHeaderDefinitions, VcfHeader) -> None
  """Generates a report.

  Combines the conflicts extracted from ``header_definitions`` and their
  resolutions from ``resolved_headers`` to generate a conflicts report.
  ``file_path`` specifies the location where the conflicts report is saved.
  """
  resolved_headers = resolved_headers or VcfHeader()
  content_lines = []
  content_lines.extend(_generate_contents(
      _extract_conflicts(header_definitions.formats), resolved_headers.formats))
  content_lines.extend(_generate_contents(
      _extract_conflicts(header_definitions.infos), resolved_headers.infos))
  _write_to_report(content_lines, file_path)


def _extract_conflicts(
    definitions  # type: Dict[str, Dict[Definition, List[str]]]
):
  # type: (...) -> Dict[str, Dict[Definition, List[str]]]
  """Extracts the fields that have conflicted definitions.

  Returns:
    A dictionary that maps field id with conflicted definitions to a dictionary
    which maps ``Definition`` to a list of file names.
  """
  # len(v) > 1 means there are conflicted definitions for this field.
  return dict([(k, v) for k, v in definitions.items() if len(v) > 1])


def _generate_contents(
    conflicts,  # type: Dict[str, Dict[Definition, List[str]]]
    resolved_headers  # type: Dict[str, Dict[str, Union[str, int]]
):
  # type: (...) -> List[str]
  """Generates the report contents.

  The conflicted definitions, the file names and the resolutions are included
  in the contents.
  Output example:
  (NS; num=1 type=Float in ['file1','file2'] num=1 type=Integer in ['file3'];
  num=1 type=Float)
  """
  contents = []
  for field_id, definitions_to_files_map in conflicts.iteritems():
    row = [
        field_id,
        _extract_definitions_and_file_names(definitions_to_files_map),
        _extract_resolution(resolved_headers, field_id)
    ]
    contents.append(';'.join(row))
  return contents


def _extract_definitions_and_file_names(definition_to_files_map):
  # type: (Dict[Definition, List[str]]) -> str
  """Extracts the definitions and related file names.

  Output example:
  num=1 type=Float in ['file1','file2'] num=1 type=Integer in ['file3']
  """
  conflict_definitions = []
  for definition, file_names in definition_to_files_map.iteritems():
    definition = _format_definition(definition.num, definition.type)
    conflict_definitions.append(definition + ' in ' + str(file_names))
  return ' '.join(conflict_definitions)


def _extract_resolution(header, filed_id):
  # type: (Dict[str, Dict[str, Union[str, int]]], str) -> str
  """Extracts the resolution.

  Output example:
  num=1 type=Float
  """
  if filed_id not in header:  # It happens when no resolved_headers is provided.
    return _NO_SOLUTION_MESSAGE
  return _format_definition(header[filed_id][VcfParserHeaderKeyConstants.NUM],
                            header[filed_id][VcfParserHeaderKeyConstants.TYPE])


def _format_definition(num_value, type_value):
  # type: (Union[str, int], str) -> str
  formatted_definition = [
      '='.join([VcfParserHeaderKeyConstants.NUM, str(num_value)]),
      '='.join([VcfParserHeaderKeyConstants.TYPE, str(type_value)])
  ]
  return ' '.join(formatted_definition)


def _write_to_report(contents, file_path):
  # type: (List[str], str) -> None
  with FileSystems.create(file_path) as file_to_write:
    if not contents:
      file_to_write.write(_NO_CONFLICTS_MESSAGE)
    else:
      file_to_write.write(_HEADER_LINE)
      file_to_write.write('\n'.join(contents))
