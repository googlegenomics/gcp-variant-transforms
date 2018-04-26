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

"""Generates preprocess report.

The report is aimed to help the user to easily import the malformed/incompatible
VCF files. It contains two parts. The first part reports the header fields that
have conflicting definitions across multiple VCF files, by providing the
conflicting definitions, the corresponding file paths, and the suggested
resolutions. The second part contains the undefined header fields and the
inferred definitions.
TODO(allieychen): Eventually, it also contains the malformed records and the
resource estimation.
"""

from typing import Dict, List, Union  # pylint: disable=unused-import

from apache_beam.io.filesystems import FileSystems
from apache_beam import pvalue

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeader  # pylint: disable=unused-import
from gcp_variant_transforms.beam_io.vcf_header_io import VcfParserHeaderKeyConstants
from gcp_variant_transforms.transforms.merge_header_definitions import Definition  # pylint: disable=unused-import
from gcp_variant_transforms.transforms.merge_header_definitions import VcfHeaderDefinitions  # pylint: disable=unused-import


_HEADER_LINE = 'ID\tCategory\tConflicts\tFile Paths\tProposed Resolution\n'
_NO_CONFLICTS_MESSAGE = 'No conflicts found.'
_NO_SOLUTION_MESSAGE = 'Not resolved.'
_UNDEFINED_HEADER_MESSAGE = 'Undefined header.'
_PADDING_CHARACTER = ' '
_DELIMITER = '\t'


def generate_report(header_definitions,
                    file_path,
                    resolved_headers=None,
                    inferred_headers=None):
  # type: (VcfHeaderDefinitions, str, VcfHeader, VcfHeader) -> None
  """Generates a report.

  Args:
    header_definitions: The container which contains all header definitions and
      the corresponding file names.
    file_path: The location where the conflicts report is saved.
    resolved_headers: The ``VcfHeader`` that provides the resolutions for the
      fields that have conflicting definitions.
    inferred_headers: The ``VcfHeader`` that contains the inferred header
      definitions of the undefined header fields.
  """
  resolved_headers = resolved_headers or VcfHeader()
  inferred_headers = inferred_headers or VcfHeader()
  content_lines = []
  content_lines.extend(_generate_conflicting_headers_lines(
      _extract_conflicts(header_definitions.infos), resolved_headers.infos,
      vcf_header_io.HeaderTypeConstants.INFO))
  content_lines.extend(_generate_conflicting_headers_lines(
      _extract_conflicts(header_definitions.formats), resolved_headers.formats,
      vcf_header_io.HeaderTypeConstants.FORMAT))
<<<<<<< HEAD:gcp_variant_transforms/libs/preprocess_reporter.py
  content_lines.extend(_generate_inferred_headers_lines(
      inferred_headers.infos, vcf_header_io.HeaderTypeConstants.INFO))
  content_lines.extend(_generate_inferred_headers_lines(
      inferred_headers.formats, vcf_header_io.HeaderTypeConstants.FORMAT))
=======
  if not isinstance(inferred_headers, pvalue.EmptySideInput):
    content_lines.extend(_generate_inferred_headers_lines(
        inferred_headers.infos, vcf_header_io.HeaderTypeConstants.INFO))
    content_lines.extend(_generate_inferred_headers_lines(
        inferred_headers.formats, vcf_header_io.HeaderTypeConstants.FORMAT))
>>>>>>> Format the report:gcp_variant_transforms/libs/preprocess_reporter.py
  _write_to_report(content_lines, file_path)


def _extract_conflicts(
    definitions  # type: Dict[str, Dict[Definition, List[str]]]
    ):
  # type: (...) -> Dict[str, Dict[Definition, List[str]]]
  """Extracts the fields that have conflicting definitions.

  Returns:
    A dictionary that maps field id with conflicting definitions to a dictionary
    which maps ``Definition`` to a list of file names.
  """
  # len(v) > 1 means there are conflicting definitions for this field.
  return dict([(k, v) for k, v in definitions.items() if len(v) > 1])


def _generate_conflicting_headers_lines(
    conflicts,  # type: Dict[str, Dict[Definition, List[str]]]
    resolved_headers,  # type: Dict[str, Dict[str, Union[str, int]]
    category  # type: str
    ):
  # type: (...) -> List[str]
  """Returns the conflicting headers lines for the report.

<<<<<<< HEAD:gcp_variant_transforms/libs/preprocess_reporter.py
  Each conflicting header record is structured into columns(TAB separated
  values): the ID, the category('FOMRAT' or 'INFO'), conflicting definitions,
  file names and the resolution. To make the contents more readable (especially
  the file names can be extremely long and there can be at most 5 of them), the
  conflicting definitions and the file names are split in the continuous rows
  such that each row/cell only contains one definition/file name. While
  splitting, the empty cells are filled by ``_PADDING_CHARACTER`` as a
  placeholder so it can be easily viewed in both text editor and spreadsheets.
=======
  To better align contents in the report so that it can be viewed easily, for
  every unique ID, generate several rows with the following steps.
  - Step 1: The first row starts with The ID, the category('FOMRAT' or 'INFO'),
    one conflicting definition, one file name and the resolution, separated by
    the ``_DELIMITER ``.
  - Step 2: If there there are more files having the same definition as the
    previous step, list each file path in one separate row by filling all other
    columns using ``_PADDING_CHARACTER``, separated by the ``_DELIMITER ``.
  - Step 3: If there are more conflicting definitions, generate a new row with
    one conflicting definition, one file path, and fill the ID, the category and
    the resolution with `_PADDING_CHARACTER``, separated by the ``_DELIMITER ``.
  - Repeat step 2 and step 3 until there are no more conflicts.
>>>>>>> Format the report:gcp_variant_transforms/libs/preprocess_reporter.py
  Output example:
  DP\tFORMAT\tnum=1 type=Float\tfile1\tnum=1 type=Float
   \t \t \tfile2\t
   \t \tnum=1 type=Integer\tfile3\t
  """
  content_lines = []
  for field_id, definitions_to_files_map in conflicts.iteritems():
    first_item = True
    for definition, file_names in definitions_to_files_map.iteritems():
      if first_item:
        row = [field_id,
               category,
               _format_definition(definition.num, definition.type),
               file_names[0],
               _extract_resolution(resolved_headers, field_id)]
        first_item = False
      else:
        row = [_PADDING_CHARACTER,
               _PADDING_CHARACTER,
               _format_definition(definition.num, definition.type),
               file_names[0],
               _PADDING_CHARACTER]
      content_lines.append(_DELIMITER.join(row))
      for file_name in file_names[1:]:
        row = [_PADDING_CHARACTER,
               _PADDING_CHARACTER,
               _PADDING_CHARACTER,
               file_name,
               _PADDING_CHARACTER]
        content_lines.append(_DELIMITER.join(row))
  return content_lines


def _generate_inferred_headers_lines(inferred_headers, category):
  # type: (Dict[str, Dict[str, Union[str, int]]], str) -> List[str]
  """Returns the inferred headers lines for the report.

  The field ID, category (FORMAT or INFO), and the inferred header definitions
  are included in the contents.
  Output example:
  NS\tFORMAT\tUndefined header.\t \tnum=1 type=Float
  """
  content_lines = []
  for field_id in inferred_headers.keys():
    row = [field_id,
           category,
           _UNDEFINED_HEADER_MESSAGE,
           _PADDING_CHARACTER,
           _extract_resolution(inferred_headers, field_id)]
    content_lines.append(_DELIMITER.join(row))
  return content_lines


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
  """Generates the report in ``file_path``.

  Output example:
  ID\tCategory\tConflicts\tFile Paths\tProposed Resolution
  DP\tFORMAT\tnum=1 type=Float\tfile1\tnum=1 type=Float
   \t \t \tfile2\t
   \t \tnum=1 type=Integer\tfile3\t
  NS\tFORMAT\tUndefined header\t \tnum=1 type=Float
  """
  with FileSystems.create(file_path) as file_to_write:
    if not contents:
      file_to_write.write(_NO_CONFLICTS_MESSAGE)
    else:
      file_to_write.write(_HEADER_LINE)
      file_to_write.write('\n'.join(contents))
