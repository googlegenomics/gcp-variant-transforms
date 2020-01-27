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


class CallSampleIdsCombiner(beam.PTransform):
  """A PTransform to combine call sample ids from all variants."""

  def __init__(self, preserve_call_sample_ids_order=False):
    # type: (bool) -> None
    """Initializes a `CallSampleIdsCombiner` object.

    Args:
      preserve_call_sample_ids_order: If true, the order of the call sample ids
        will be the same as in the BigQuery table. Otherwise, the call sample
        ids are sorted in increasing order.
    Raises:
      ValueError: If the call sample ids are not equal and in the same order
        when `preserve_call_sample_ids_order` is True.
    """
    self._preserve_call_sample_ids_order = preserve_call_sample_ids_order

  def _get_call_sample_ids(self, variant):
    # type: (vcf_parser.Variant) -> Tuple[str]
    """Returns the sample ids of all calls for the variant."""
    call_sample_ids = [call.sample_id for call in variant.calls]
    if len(call_sample_ids) != len(set(call_sample_ids)):
      raise ValueError('There are duplicate call sample ids in the variant {}'.
                       format(variant))
    return tuple(call_sample_ids)

  def _extract_unique_call_sample_ids(self, call_sample_ids):
    # type: (List[Tuple[str]]) -> List[str]
    """Extracts unique call sample ids from all variants.

    Returns:
      The unique call sample ids tuple in `call_sample_ids`.
    Raises:
      ValueError: If the call sample ids are not equal and in the same order
        when `preserve_call_sample_ids_order` is True.
    """
    if len(call_sample_ids) == 1:
      return list(call_sample_ids[0])
    raise ValueError('The call sample ids are not equal and in the same order '
                     'across the variants being exported. Please rerun the '
                     'pipeline with `--preserve_call_sample_ids_order false`.')

  def expand(self, pcoll):
    if self._preserve_call_sample_ids_order:
      return (pcoll
              | 'GetCallSampleIds' >> beam.Map(self._get_call_sample_ids)
              | 'RemoveDuplicates' >> beam.RemoveDuplicates()
              | 'Combine' >> beam.combiners.ToList()
              | 'ExtractUniqueCallSampleIds'
              >> beam.ParDo(self._extract_unique_call_sample_ids)
              | beam.combiners.ToList())
    else:
      return (pcoll
              | 'GetCallSampleIds' >> beam.FlatMap(self._get_call_sample_ids)
              | 'RemoveDuplicates' >> beam.RemoveDuplicates()
              | 'Combine' >> beam.combiners.ToList()
              | 'SortCallSampleIds' >> beam.ParDo(sorted)
              | beam.combiners.ToList())
