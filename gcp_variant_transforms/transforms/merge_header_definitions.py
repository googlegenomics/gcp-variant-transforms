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

from typing import Dict, List  # pylint: disable=unused-import

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcf_header_io # pylint: disable=unused-import
from gcp_variant_transforms.libs import vcf_header_definitions_merger

# An alias for the header key constants to make referencing easier.
_VcfHeaderDefinitions = vcf_header_definitions_merger.VcfHeaderDefinitions

class _MergeDefinitionsFn(beam.CombineFn):
  """Combiner function for merging definitions."""

  def __init__(self, definitions_merger):
    # type: (vcf_header_definitions_merger.DefinitionsMerger) -> None
    super().__init__()
    self._definitions_merger = definitions_merger

  def create_accumulator(self):
    return vcf_header_definitions_merger.VcfHeaderDefinitions()

  def add_input(self,
                source,  # type: _VcfHeaderDefinitions
                to_merge  # type: vcf_header_io.VcfHeader
               ):
    # type: (...) -> _VcfHeaderDefinitions
    return self.merge_accumulators(
        [source,
         vcf_header_definitions_merger.VcfHeaderDefinitions(
             vcf_header=to_merge)])

  def merge_accumulators(self, accumulators):
    # type: (List[_VcfHeaderDefinitions]) -> _VcfHeaderDefinitions
    merged_definitions = self.create_accumulator()
    for to_merge in accumulators:
      self._definitions_merger.merge(merged_definitions, to_merge)
    return merged_definitions

  def extract_output(self, merged_definitions):
    # type: (_VcfHeaderDefinitions) -> _VcfHeaderDefinitions
    return merged_definitions


class MergeDefinitions(beam.PTransform):
  """A PTransform to merge header definitions.

  Reads a PCollection of `VcfHeader` and produces a PCollection of
  `VcfHeaderDefinitions`.
  """

  def __init__(self):
    """Initializes `MergeDefinitions` object."""
    super().__init__()
    self._definitions_merger = vcf_header_definitions_merger.DefinitionsMerger()

  def expand(self, pcoll):
    return pcoll | beam.CombineGlobally(
        _MergeDefinitionsFn(self._definitions_merger)).without_defaults()
