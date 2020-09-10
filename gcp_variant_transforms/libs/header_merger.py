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

"""Class for merging 2 VcfHeader objects."""

from collections import OrderedDict
from typing import Dict, Any  #pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import vcf_field_conflict_resolver #pylint: disable=unused-import

class HeaderMerger():
  """Class for merging two :class:`VcfHeader`s."""

  def __init__(self, resolver):
    # type: (vcf_field_conflict_resolver.FieldConflictResolver) -> None
    """Initialize :class:`HeaderMerger` object.

    Args:
      resolver: Auxiliary class for resolving possible header value mismatches.
    """
    self._resolver = resolver

  def merge(self, first, second):
    # type: (vcf_header_io.VcfHeader, vcf_header_io.VcfHeader) -> None
    """Updates `first`'s headers with values from `second`.

    If a specific key does not already exist in a specific one of `first`'s
    headers, that key and the associated value will be added. If the key does
    already exist in the specific header of `first`, then the value of that
    key will be updated with the value from `second`.

    Args:
      first: The VcfHeader object.
      second: The VcfHeader object that's headers will be merged into the
        headers of first.
    """
    if (not isinstance(first, vcf_header_io.VcfHeader) or
        not isinstance(second, vcf_header_io.VcfHeader)):
      raise NotImplementedError
    self._merge_header_fields(first.infos, second.infos)
    self._merge_header_fields(first.filters, second.filters)
    self._merge_header_fields(first.alts, second.alts)
    self._merge_header_fields(first.formats, second.formats)
    self._merge_header_fields(first.contigs, second.contigs)

  def _merge_header_fields(
      self,
      first,  # type: Dict[str, OrderedDict[str, Union[str, int]]]
      second  # type: Dict[str, OrderedDict[str, Union[str, int]]]
      ):
    # type: (...) -> None
    """Modifies `first` to add any keys from `second` not in `first`.

    Args:
      first: first header fields.
      second: second header fields.
    Raises:
      ValueError: If the header fields are incompatible (e.g. same key with
        different types or numbers).
    """
    for second_key, second_value in list(second.items()):
      if second_key not in first:
        first[second_key] = second_value
        continue
      first_value = first[second_key]
      if list(first_value.keys()) != list(second_value.keys()):
        raise ValueError('Incompatible header fields: {}, {}'.format(
            first_value, second_value))
      merged_value = OrderedDict()
      for first_field_key, first_field_value in list(first_value.items()):
        second_field_value = second_value[first_field_key]
        try:
          resolution_field_value = self._resolver.resolve_attribute_conflict(
              first_field_key,
              first_field_value,
              second_field_value)
          merged_value.update({first_field_key: resolution_field_value})
        except ValueError as e:
          raise ValueError('Incompatible number or types in header fields:'
                           '{}, {} \n. Error: {}'.format(
                               first_value, second_value, str(e))) from e

      first[second_key] = merged_value
