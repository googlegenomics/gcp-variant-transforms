# Copyright 2019 Google Inc.  All Rights Reserved.
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

"""VCF Header Definitions Merger class."""

import collections
from collections import namedtuple
from typing import Dict, List  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcf_header_io

# `Definition` cherry-picks the attributes from vcf header definitions that
# are critical for checking field compatibilities across VCF files.
Definition = namedtuple('Definition',
                        [vcf_header_io.VcfParserHeaderKeyConstants.NUM,
                         vcf_header_io.VcfParserHeaderKeyConstants.TYPE])


class VcfHeaderDefinitions(object):
  """Container for header definitions."""

  def __init__(self, vcf_header=None):
    # type: (vcf_header_io.VcfHeader) -> None
    """Initializes a `VcfHeaderDefinitions` object.

    Creates two dictionaries (for infos and formats respectively) that map field
    id to a dictionary which maps `Definition` to a list of file names.
    """
    self._infos = collections.defaultdict(dict)
    self._formats = collections.defaultdict(dict)
    if not vcf_header:
      return
    for key, val in list(vcf_header.infos.items()):
      definition = Definition(
          val[vcf_header_io.VcfParserHeaderKeyConstants.NUM],
          val[vcf_header_io.VcfParserHeaderKeyConstants.TYPE])
      self._infos[key][definition] = [vcf_header.file_path]
    for key, val in list(vcf_header.formats.items()):
      definition = Definition(
          val[vcf_header_io.VcfParserHeaderKeyConstants.NUM],
          val[vcf_header_io.VcfParserHeaderKeyConstants.TYPE])
      self._formats[key][definition] = [vcf_header.file_path]

  def __eq__(self, other):
    return self._infos == other._infos and self._formats == other._formats

  @property
  def infos(self):
    return self._infos

  @property
  def formats(self):
    return self._formats


class DefinitionsMerger(object):
  """Class for merging two `VcfHeaderDefinitions`s."""

  # For the same field definition, save at most `_MAX_NUM_FILE_NAMES` names.
  _MAX_NUM_FILE_NAMES = 5

  def merge(self, first, second):
    # type: (VcfHeaderDefinitions, VcfHeaderDefinitions) -> None
    """Updates `first`'s definitions with values from `second`."""
    if (not isinstance(first, VcfHeaderDefinitions) or
        not isinstance(second, VcfHeaderDefinitions)):
      raise NotImplementedError
    self._merge_definitions(first.infos, second.infos)
    self._merge_definitions(first.formats, second.formats)

  def _merge_definitions(
      self,
      first,  # type: Dict[str, Dict[Definition, List[str]]]
      second  # type: Dict[str, Dict[Definition, List[str]]]
      ):
    # type: (...) -> None
    """Updates `first` by merging values from `first` and `second`."""
    for key, definitions_to_files_map in list(second.items()):
      for definition, file_names in list(definitions_to_files_map.items()):
        first[key].setdefault(definition, [])
        first[key][definition].extend(str(s) for s in file_names)
        first[key][definition] = (
            first[key][definition][:self._MAX_NUM_FILE_NAMES])
