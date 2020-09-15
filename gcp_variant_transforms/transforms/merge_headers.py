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

"""beam combiner function for merging VCF file headers."""

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import header_merger
from gcp_variant_transforms.libs import vcf_field_conflict_resolver

# An alias for the header key constants to make referencing easier.
_HeaderKeyConstants = vcf_header_io.VcfParserHeaderKeyConstants
_VcfHeaderTypeConstants = vcf_header_io.VcfHeaderFieldTypeConstants

class _MergeHeadersFn(beam.CombineFn):
  """Combiner function for merging VCF file headers."""

  def __init__(self, merger):
    # type: (HeaderMerger) -> None
    super().__init__()
    self._header_merger = merger

  def create_accumulator(self):
    # type: () -> vcf_header_io.VcfHeader
    return vcf_header_io.VcfHeader()

  def add_input(self,
                source,  # type: vcf_header_io.VcfHeader
                to_merge  # type: vcf_header_io.VcfHeader
               ):
    # type: (...) -> vcf_header_io.VcfHeader
    return self.merge_accumulators([source, to_merge])

  def merge_accumulators(self, accumulators):
    # type: (List[vcf_header_io.VcfHeader]) -> vcf_header_io.VcfHeader
    merged_headers = self.create_accumulator()
    for to_merge in accumulators:
      self._header_merger.merge(merged_headers, to_merge)
    return merged_headers

  def extract_output(self, merged_headers):
    # type: (vcf_header_io.VcfHeader) -> vcf_header_io.VcfHeader
    for info in merged_headers.infos.values():
      if info[_HeaderKeyConstants.TYPE] == '.':
        info[_HeaderKeyConstants.TYPE] = _VcfHeaderTypeConstants.STRING
    return merged_headers

class MergeHeaders(beam.PTransform):
  """A PTransform to merge VCF file headers."""

  def __init__(self,
               split_alternate_allele_info_fields=True,
               allow_incompatible_records=False):
    # type: (bool, bool) -> None
    """Initializes :class:`MergeHeaders` object.

    Args:
      split_alternate_allele_info_fields: Whether INFO fields with
        `Number=A` are store under the alternate_bases record. This is relevant
        as it changes the header compatibility rules as it changes the schema.
      allow_incompatible_records: If true, header definition with type mismatch
        (e.g., string vs float) are always resolved.
    """
    super().__init__()
    # Resolver makes extra efforts to resolve conflict in header definitions
    # when flag allow_incompatible_records is set. For example, it resolves
    # type conflict of string and float into string.
    self._header_merger = header_merger.HeaderMerger(
        vcf_field_conflict_resolver.FieldConflictResolver(
            split_alternate_allele_info_fields,
            resolve_always=allow_incompatible_records))

  def expand(self, pcoll):
    return pcoll | 'MergeHeaders' >> beam.CombineGlobally(
        _MergeHeadersFn(self._header_merger)).without_defaults()
