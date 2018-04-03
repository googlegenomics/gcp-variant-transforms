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

"""Beam combiner function for merging VCF file header definitions."""

from collections import OrderedDict  # pylint: disable=unused-import
from collections import namedtuple
from typing import Dict, Union  # pylint: disable=unused-import

import apache_beam as beam
from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeader   # pylint: disable=unused-import
from gcp_variant_transforms.libs.vcf_field_conflict_resolver import VcfParserConstants

HeaderFields = namedtuple('HeaderFields', [VcfParserConstants.NUM,
                                           VcfParserConstants.TYPE])


class DefinitionDetails(object):
  """Container for conflict details for one field ID."""

  # For the same field definition, save at most `_MAX_NUM_FILE_NAMES` names.
  _MAX_NUM_FILE_NAMES = 5

  def __init__(self, definition, file_name):
    # type: (OrderedDict[str, Union[str, int]], str) -> None
    """Initializes a _DefinitionDetails object.

    Creates a dictionary that maps field definition to a list of file names.
    Upon initialization, add ``definition`` and ``file_name`` to the dictionary.
    """
    self._definitions_to_files = {
        HeaderFields(definition[VcfParserConstants.NUM],
                     definition[VcfParserConstants.TYPE]): [file_name]}

  def __eq__(self, other):
    return self._definitions_to_files == other.definitions_to_files

  def _add_details(self, other):
    # type: (DefinitionDetails) -> None
    for definition, file_names in other._definitions_to_files.iteritems():
      if definition not in self._definitions_to_files.keys():
        self._definitions_to_files[definition] = file_names
      else:
        self._definitions_to_files[definition].extend(file_names)
        self._definitions_to_files[definition] = \
          self._definitions_to_files[definition][:self._MAX_NUM_FILE_NAMES]

  @property
  def definitions_to_files(self):
    return self._definitions_to_files


class VcfHeaderDefinitions(object):
  """Container for header definitions."""

  def __init__(self, vcf_header=None):
    # type: (VcfHeader) -> None
    """Initializes a VcfHeaderDefinitions object.

    Creates two dictionaries that map info and format field id/key to
    _ConflictDefinitions respectively.
    """
    self._info = {}
    self._format = {}
    if not vcf_header:
      return
    file_name = vcf_header.file_name
    for key, val in vcf_header.infos.iteritems():
      self._info[key] = DefinitionDetails(val, file_name)
    for key, val in vcf_header.formats.iteritems():
      self._format[key] = DefinitionDetails(val, file_name)

  def __eq__(self, other):
    return self._info == other._info and self._format == other._format

  @property
  def info(self):
    return self._info

  @property
  def format(self):
    return self._format


class _DefinitionsMerger(object):
  """Class for merging two :class:`VcfHeaderDefinitions`s."""

  def merge(self, first, second):
    # type: (VcfHeaderDefinitions, VcfHeaderDefinitions) -> None
    """Updates ``first``'s definitions with values from ``second``."""
    if (not isinstance(first, VcfHeaderDefinitions) or
        not isinstance(first, VcfHeaderDefinitions)):
      raise NotImplementedError
    self._merge_definitions(first.info, second.info)
    self._merge_definitions(first.format, second.format)

  def _merge_definitions(self, first, second):
    # type: (Dict[str, DefinitionDetails], Dict[str, DefinitionDetails]) -> None
    """Updates ``first`` by merging values from ``first`` and ``second``."""
    for key, definition_details in second.iteritems():
      if key in first:
        first[key]._add_details(definition_details)
      else:
        first[key] = definition_details


class _MergeDefinitionsFn(beam.CombineFn):
  """Combiner function for merging definitions."""

  def __init__(self, definitions_merger):
    # type: (_DefinitionsMerger) -> None
    super(_MergeDefinitionsFn, self).__init__()
    self._definitions_merger = definitions_merger

  def create_accumulator(self):
    return VcfHeaderDefinitions()

  def add_input(self, source, to_merge):
    # type: (VcfHeaderDefinitions, VcfHeader) -> VcfHeaderDefinitions
    return self.merge_accumulators([source,
                                    VcfHeaderDefinitions(vcf_header=to_merge)])

  def merge_accumulators(self, accumulators):
    # type: (List[VcfHeaderDefinitions]) -> VcfHeaderDefinitions
    merged_definitions = self.create_accumulator()
    for to_merge in accumulators:
      self._definitions_merger.merge(merged_definitions, to_merge)
    return merged_definitions

  def extract_output(self, merged_definitions):
    # type: (VcfHeaderDefinitions) -> VcfHeaderDefinitions
    return merged_definitions


class MergeDefinitions(beam.PTransform):
  """A PTransform to merge header definitions."""

  def __init__(self):
    """Initializes :class:`MergeConflicts` object."""
    super(MergeDefinitions, self).__init__()
    self._definitions_merger = _DefinitionsMerger()

  def expand(self, pcoll):
    return pcoll | beam.CombineGlobally(
        _MergeDefinitionsFn(self._definitions_merger)).without_defaults()
