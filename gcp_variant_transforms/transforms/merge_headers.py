# Copyright 2017 Google Inc.  All Rights Reserved.
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
from collections import OrderedDict
from typing import Dict, Any  #pylint: disable=unused-import

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import vcf_field_conflict_resolver

# TODO(nmousavi): Consider moving this into a separate file.


class _HeaderMerger(object):
  """Class for merging two :class:`VcfHeader`s."""

  def __init__(self, resolver, save_conflicts=False):
    # type: (vcf_field_cnflict_resolver.FieldConflictResolver, bool) -> None
    """Initialize :class:`VcfHeader` object.

    Args:
      resolver: Auxiliary class for resolving possible header value mismatches.
      save_conflicts: save all conflicts when merging headers.
    """
    self._resolver = resolver
    self._save_conflicts = save_conflicts

  def merge(self, first, second):
    """Updates ``first``'s headers with values from ``second``.

    If a specific key does not already exist in a specific one of ``first``'s
    headers, that key and the associated value will be added. If the key does
    already exist in the specific header of ``first``, then the value of that
    key will be updated with the value from ``second``.

    Args:
      first (:class:`VcfHeader`): The VcfHeader object.
      second (:class:`VcfHeader`): The VcfHeader object that's headers will be
        merged into the headers of first.
    """
    if (not isinstance(first, vcf_header_io.VcfHeader) or
        not isinstance(first, vcf_header_io.VcfHeader)):
      raise NotImplementedError
    self._merge_header_fields(first.infos, second.infos, first.conflicts,
                              second.conflicts)
    self._merge_header_fields(first.filters, second.filters, first.conflicts,
                              second.conflicts)
    self._merge_header_fields(first.alts, second.alts, first.conflicts,
                              second.conflicts)
    self._merge_header_fields(first.formats, second.formats, first.conflicts,
                              second.conflicts)
    self._merge_header_fields(first.contigs, second.contigs, first.conflicts,
                              second.conflicts)

  def _merge_header_fields(self, first_headers, second_headers,
                           first_conflicts, second_conflicts):
    # type: (Dict[str, OrderedDict[str, Union[str, int]]],
    #        Dict[str, OrderedDict[str, Union[str, int]]],
    #        Dict[str, Set[str]],
    #        Dict[str, Set[str]]) -> None
    """Modifies ``first`` to add any keys from ``second`` not in ``first``.

    Args:
      first_headers (dict): first header fields.
      second_headers (dict): second header fields.
      first_conflicts (dict): first header related conflicts.
      second_conflicts (dict): second header related conflicts.
    Raises:
      ValueError: If the header fields are incompatible (e.g. same key with
        different types or numbers).
    """
    for second_key, second_value in second_headers.iteritems():
      if second_key not in first_headers:
        first_headers[second_key] = second_value
        self._merge_conflicts(second_key, second_value,
                              first_conflicts, second_conflicts)
        continue
      first_value = first_headers[second_key]
      if first_value.keys() != second_value.keys():
        raise ValueError('Incompatible header fields: {}, {}'.format(
            first_value, second_value))
      merged_value = OrderedDict()
      for first_field_key, first_field_value in first_value.iteritems():
        second_field_value = second_value[first_field_key]
        self._merge_conflicts(second_key, second_value,
                              first_conflicts, second_conflicts)
        try:
          resolution_field_value = self._resolver.resolve_attribute_conflict(
              first_field_key,
              first_field_value,
              second_field_value)
          merged_value.update({first_field_key: resolution_field_value})
        except ValueError as e:
          raise ValueError('Incompatible number or types in header fields:'
                           '{}, {} \n. Error: {}'.format(
                               first_value, second_value, str(e)))

      first_headers[second_key] = merged_value

  def _merge_conflicts(self, key, value, first_conflicts, second_conflicts):
    # type: (str,
    #        OrderedDict[str, Union[str, int]],
    #        Dict[str, Set[str]],
    #        Dict[str, Set[str]])-> None
    """Updates ``first_conflicts``'s value of ``key`` by merging values from
    ``first_conflicts`` and ``second_conflicts``.

    If ``key`` does not already exist in ``first_conflicts``, then ``key`` is
    not defined in the first headers.
    If ``key`` does not already exist in ``second_conflicts``, it means that
    ``value`` is the only definition for this key. Simply add ``value`` into the
    ``second_conflicts``.
    """
    if not self._save_conflicts:
      return
    if key not in first_conflicts:
      first_conflicts.update({key: set()})
    if key not in second_conflicts:
      second_conflicts.update({key: {self._format_conflict(value)}})
    first_conflicts[key].update(second_conflicts[key])

  def _format_conflict(self, field_definition):
    # type: (OrderedDict[str, Union[str, int]]) -> str
    """Formats the conflict string given one field definition.

    Only num and type is considered for the conflicts, and the definition forms
    a string in the format of `num={value} type={value}`.
    """
    conflict_keys = ['num', 'type']
    formatted_conflict = []
    for key in conflict_keys:
      formatted_conflict.append(key + '=' + str(field_definition[key]))
    return ' '.join(formatted_conflict)


class _MergeHeadersFn(beam.CombineFn):
  """Combiner function for merging VCF file headers."""

  def __init__(self, header_merger):
    super(_MergeHeadersFn, self).__init__()
    self._header_merger = header_merger

  def create_accumulator(self):
    return vcf_header_io.VcfHeader()

  def add_input(self, source, to_merge):
    return self.merge_accumulators([source, to_merge])

  def merge_accumulators(self, accumulators):
    merged_headers = self.create_accumulator()
    for to_merge in accumulators:
      self._header_merger.merge(merged_headers, to_merge)
    return merged_headers

  def extract_output(self, merged_headers):
    return merged_headers


class MergeHeaders(beam.PTransform):
  """A PTransform to merge VCF file headers."""

  def __init__(self, split_alternate_allele_info_fields=True):
    # type: (bool) -> None
    """Initializes :class:`MergeHeaders` object.

    Args:
      split_alternate_allele_info_fields: Whether INFO fields with
        `Number=A` are store under the alternate_bases record. This is relevant
        as it changes the header compatibility rules as it changes the schema.
    """
    super(MergeHeaders, self).__init__()
    self._header_merger = _HeaderMerger(
        vcf_field_conflict_resolver.FieldConflictResolver(
            split_alternate_allele_info_fields))

  def expand(self, pcoll):
    return pcoll | 'MergeHeaders' >> beam.CombineGlobally(_MergeHeadersFn(
        self._header_merger)).without_defaults()
