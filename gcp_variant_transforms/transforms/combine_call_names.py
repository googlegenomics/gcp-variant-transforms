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

  def _get_call_names(self, variant):
    # type: (vcf_parser.Variant) -> Tuple[str]
    """Returns the names of all calls for the variant."""
    call_names = [call.name for call in variant.calls]
    if len(call_names) != len(set(call_names)):
      raise ValueError('There are duplicate call names in the variant {}'.
                       format(variant))
    return tuple(call_names)

  def _combine_unique_call_names(self, call_names):
    # type: (List[Tuple[str]]) -> List[str]
    """Combines unique call names from all variants.

    If there is only one unique call name tuple in `call_names`, it means that
    the call names from all variants are the same. For this case, return this
    call name tuple directly. Otherwise, return the call names in sorted order.
    """
    if len(call_names) == 1:
      return list(call_names[0])
    return (call_names
            | 'FlattenCallNames' >> beam.Flatten()
            | 'RemoveDuplicates' >> beam.RemoveDuplicates()
            | 'Combine' >> beam.combiners.ToList()
            | 'SortCallNames' >> beam.ParDo(sorted))

  def expand(self, pcoll):
    return (pcoll
            | 'GetCallNames' >> beam.Map(self._get_call_names)
            | 'RemoveDuplicates' >> beam.RemoveDuplicates()
            | 'Combine' >> beam.combiners.ToList()
            | 'CombineUniqueCallNames'
            >> beam.ParDo(self._combine_unique_call_names)
            | beam.combiners.ToList())
