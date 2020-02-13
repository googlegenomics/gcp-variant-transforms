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

"""A PTransform to combine sample ids from all variants."""

from typing import List, Tuple  # pylint: disable=unused-import

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcf_parser  # pylint: disable=unused-import


class SampleIdsCombiner(beam.PTransform):
  """A PTransform to combine sample ids from all variants."""

  def __init__(self, preserve_sample_order=False):
    # type: (bool) -> None
    """Initializes a `SampleIdsCombiner` object.

    Args:
      preserve_sample_order: If true, the order of the sample ids will be the
        same as in the BigQuery table. Otherwise, the sample ids are sorted in
        increasing order.
    Raises:
      ValueError: If the sample ids are not equal and in the same order
        when `preserve_sample_order` is True.
    """
    self._preserve_sample_order = preserve_sample_order

  def _get_sample_ids(self, variant):
    # type: (vcf_parser.Variant) -> Tuple[str]
    """Returns the sample ids of all calls for the variant."""
    sample_ids = [call.sample_id for call in variant.calls]
    if len(sample_ids) != len(set(sample_ids)):
      raise ValueError('There are duplicate sample ids in the variant {}'.
                       format(variant))
    return tuple(sample_ids)

  def _extract_unique_sample_ids(self, sample_ids):
    # type: (List[Tuple[str]]) -> List[str]
    """Extracts unique sample ids from all variants.

    Returns:
      The unique sample ids tuple in `sample_ids`.
    Raises:
      ValueError: If the sample ids are not equal and in the same order
        when `preserve_sample_order` is True.
    """
    if len(sample_ids) == 1:
      return list(sample_ids[0])
    raise ValueError('The sample ids are not equal and in the same order '
                     'across the variants being exported. Please rerun the '
                     'pipeline with `--preserve_sample_order false`.')

  def expand(self, pcoll):
    if self._preserve_sample_order:
      return (pcoll
              | 'GetSampleIds' >> beam.Map(self._get_sample_ids)
              | 'RemoveDuplicates' >> beam.RemoveDuplicates()
              | 'Combine' >> beam.combiners.ToList()
              | 'ExtractUniqueSampleIds'
              >> beam.ParDo(self._extract_unique_sample_ids)
              | beam.combiners.ToList())
    else:
      return (pcoll
              | 'GetSampleIds' >> beam.FlatMap(self._get_sample_ids)
              | 'RemoveDuplicates' >> beam.RemoveDuplicates()
              | 'Combine' >> beam.combiners.ToList()
              | 'SortSampleIds' >> beam.ParDo(sorted)
              | beam.combiners.ToList())
