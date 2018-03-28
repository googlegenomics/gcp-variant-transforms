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

"""Beam combiner function for merging VCF file header conflicts/definitions."""

from collections import OrderedDict   #pylint: disable=unused-import

import apache_beam as beam
from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeader
from gcp_variant_transforms.libs.vcf_field_conflict_resolver import VcfParserConstants


class _ConflictDetails(object):
  """Container for conflict details for one field id."""
  _MAX_FILE_NAME_NUM = 5

  def __init__(self, definition, file_name):
    # type: (OrderedDict[str, Union[str, int]], str) -> None
    """Initialized a _ConflictDetails object.

    Creates a dictionary that maps field definition to a list of file names.
    Upon initialization, add ``definition`` and ``file_name`` to the dictionary.
    """
    self.conflicts = {self._format_definition(definition): list([file_name])}

  def _add_details(self, other):
    # type: (_ConflictDetails) -> None
    for conflict, file_names in other.conflicts.iteritems():
      if conflict not in self.conflicts.keys():
        self.conflicts[conflict] = file_names
        continue

      for file_name in file_names:
        if len(self.conflicts[conflict]) == self._MAX_FILE_NAME_NUM:
          break
        self.conflicts[conflict].append(file_name)

  def _format_definition(self, definition):
    # type: (OrderedDict[str, Union[str, int]]) -> str
    """Formats the conflict string given one field definition.

    Only num and type is considered for the conflicts, and the definition forms
    a string in the format of `num={value} type={value}`.
    """
    conflict_keys = [VcfParserConstants.NUM, VcfParserConstants.TYPE]
    formatted_conflict = []
    for key in conflict_keys:
      formatted_conflict.append(key + '=' + str(definition[key]))
    return ' '.join(formatted_conflict)


class VcfHeaderConflicts(object):
  """Container for header conflicts."""

  def __init__(self, vcf_header=VcfHeader()):
    # type: (VcfHeader) -> None
    """Initializes a VcfHeaderConflicts object.

    Creates two dictionaries that map info and format field id/key to
    _ConflictDetails respectively.
    """

    file_name = vcf_header.file_name
    self.info_conflicts = {}
    self.format_conflicts = {}
    for key, val in vcf_header.infos.iteritems():
      self.info_conflicts[key] = _ConflictDetails(val, file_name)
    for key, val in vcf_header.formats.iteritems():
      self.format_conflicts[key] = _ConflictDetails(val, file_name)


class _ConflictsMerger(object):
  """Class for merging two :class:`VcfHeaderConflicts`s."""

  def merge(self, first, second):
    # type: (VcfHeaderConflicts, VcfHeaderConflicts) -> None
    """Updates ``first``'s conflicts with values from ``second``."""
    if (not isinstance(first, VcfHeaderConflicts) or
        not isinstance(first, VcfHeaderConflicts)):
      raise NotImplementedError
    self._merge_conflicts(first.info_conflicts, second.info_conflicts)
    self._merge_conflicts(first.format_conflicts, second.format_conflicts)

  def _merge_conflicts(self, first, second):
    # type: (Dict[str, _ConflictDetails], Dict[str, _ConflictDetails]) -> None
    """Updates ``first`` by merging values from ``first`` and ``second``."""

    for key, conflicts_details in second.iteritems():
      if key not in first:
        first[key] = conflicts_details
        continue
      first[key]._add_details(conflicts_details)


class _MergeConflictsFn(beam.CombineFn):
  """Combiner function for merging conflicts."""

  def __init__(self, conflicts_merger):
    super(_MergeConflictsFn, self).__init__()
    self._conflicts_merger = conflicts_merger

  def create_accumulator(self):
    return VcfHeaderConflicts()

  def add_input(self, source, to_merge):
    return self.merge_accumulators([source,
                                    VcfHeaderConflicts(vcf_header=to_merge)])

  def merge_accumulators(self, accumulators):
    merged_conflicts = self.create_accumulator()
    for to_merge in accumulators:
      self._conflicts_merger.merge(merged_conflicts, to_merge)
    return merged_conflicts

  def extract_output(self, merged_conflicts):
    return merged_conflicts
