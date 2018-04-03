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

from collections import namedtuple
from typing import Dict, List  # pylint: disable=unused-import

import apache_beam as beam
from gcp_variant_transforms.beam_io.vcf_header_io import VcfHeader  # pylint: disable=unused-import
from gcp_variant_transforms.libs.vcf_field_conflict_resolver import VcfParserConstants

Definition = namedtuple('Definition', [VcfParserConstants.NUM,
                                       VcfParserConstants.TYPE])


class VcfHeaderDefinitions(object):
  """Container for header definitions."""

  def __init__(self, vcf_header=None):
    # type: (VcfHeader) -> None
    """Initializes a VcfHeaderDefinitions object.

    Creates two dictionaries (for infos and formats respectively) that map field
    id to a dictionary which maps Definition to a list of file names.
    """
    self._infos = {}
    self._formats = {}
    if not vcf_header:
      return
    for key, val in vcf_header.infos.iteritems():
      self._infos[key] = {
          Definition(val[VcfParserConstants.NUM], val[VcfParserConstants.TYPE]):
          [vcf_header.file_name]}
    for key, val in vcf_header.formats.iteritems():
      self._formats[key] = {
          Definition(val[VcfParserConstants.NUM], val[VcfParserConstants.TYPE]):
          [vcf_header.file_name]}

  def __eq__(self, other):
    return self._infos == other._infos and self._formats == other._formats

  @property
  def infos(self):
    return self._infos

  @property
  def formats(self):
    return self._formats


class _DefinitionsMerger(object):
  """Class for merging two :class:`VcfHeaderDefinitions`s."""

  # For the same field definition, save at most `_MAX_NUM_FILE_NAMES` names.
  _MAX_NUM_FILE_NAMES = 5

  def merge(self, first, second):
    # type: (VcfHeaderDefinitions, VcfHeaderDefinitions) -> None
    """Updates ``first``'s definitions with values from ``second``."""
    if (not isinstance(first, VcfHeaderDefinitions) or
        not isinstance(first, VcfHeaderDefinitions)):
      raise NotImplementedError
    self._merge_definitions(first.infos, second.infos)
    self._merge_definitions(first.formats, second.formats)

  def _merge_definitions(
      self,
      first,  # type: Dict[str, Dict[Definition, List[str]]]
      second  # type: Dict[str, Dict[Definition, List[str]]]
      ):
    # type: (...) -> None
    """Updates ``first`` by merging values from ``first`` and ``second``."""
    for key, definitions_to_files in second.iteritems():
      if key in first:
        self._merge_definitions_to_files_dict(first[key], definitions_to_files)
      else:
        first[key] = definitions_to_files

  def _merge_definitions_to_files_dict(
      self,
      first_definitions_to_files,  # type: Dict[Definition, List[str]]
      second_definitions_to_files  # type: Dict[Definition, List[str]]
      ):
    """Updates ``first_definitions_to_files`` by merging definitions and
    corresponding file names from ``first_definitions_to_files`` and
    ``second_definitions_to_files``."""
    # type: (...) -> None
    for definition, file_names in second_definitions_to_files.iteritems():
      if definition not in first_definitions_to_files.keys():
        first_definitions_to_files[definition] = file_names
      else:
        first_definitions_to_files[definition].extend(file_names)
        first_definitions_to_files[definition] = \
          first_definitions_to_files[definition][:self._MAX_NUM_FILE_NAMES]


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
  """A PTransform to merge header definitions.

  Reads a PCollection of VcfHeader and produces a PCollection of
  VcfHeaderDefinitions.
  """

  def __init__(self):
    """Initializes :class:`MergeDefinitions` object."""
    super(MergeDefinitions, self).__init__()
    self._definitions_merger = _DefinitionsMerger()

  def expand(self, pcoll):
    return pcoll | beam.CombineGlobally(
        _MergeDefinitionsFn(self._definitions_merger)).without_defaults()
