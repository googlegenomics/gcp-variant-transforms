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

"""A PTransform to combine call names from all variants."""

from typing import List, Tuple  # pylint: disable=unused-import

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcf_parser  # pylint: disable=unused-import


class CallNamesCombiner(beam.PTransform):
  """A PTransform to combine call names from all variants."""

  def __init__(self, preserve_call_names_order=False):
    # type: (bool) -> None
    """Initializes a `CallNamesCombiner` object.

    Args:
      preserve_call_names_order: If true, the order of the call names will be
        the same as in the BigQuery table. Otherwise, the call names are sorted
        in increasing order.
    Raises:
      ValueError: If the call names are not equal and in the same order when
        `preserve_call_names_order` is True.
    """
    self._preserve_call_names_order = preserve_call_names_order

  def _get_call_names(self, variant):
    # type: (vcf_parser.Variant) -> Tuple[str]
    """Returns the names of all calls for the variant."""
    call_names = [call.name for call in variant.calls]
    if len(call_names) != len(set(call_names)):
      raise ValueError('There are duplicate call names in the variant {}'.
                       format(variant))
    return tuple(call_names)

  def _extract_unique_call_names(self, call_names):
    # type: (List[Tuple[str]]) -> List[str]
    """Extracts unique call names from all variants.

    Returns:
      The unique call name tuple in `call_names`.
    Raises:
      ValueError: If the call names are not equal and in the same order when
        `preserve_call_names_order` is True.
    """
    if len(call_names) == 1:
      return list(call_names[0])
    raise ValueError('The call names are not equal and in the same order '
                     'across the variants being exported. Please rerun the '
                     'pipeline with `--preserve_call_names_order false`.')

  def expand(self, pcoll):
    if self._preserve_call_names_order:
      return (pcoll
              | 'GetCallNames' >> beam.Map(self._get_call_names)
              | 'RemoveDuplicates' >> beam.RemoveDuplicates()
              | 'Combine' >> beam.combiners.ToList()
              | 'ExtractUniqueCallNames'
              >> beam.ParDo(self._extract_unique_call_names)
              | beam.combiners.ToList())
    else:
      return (pcoll
              | 'GetCallNames' >> beam.FlatMap(self._get_call_names)
              | 'RemoveDuplicates' >> beam.RemoveDuplicates()
              | 'Combine' >> beam.combiners.ToList()
              | 'SortCallNames' >> beam.ParDo(sorted)
              | beam.combiners.ToList())
