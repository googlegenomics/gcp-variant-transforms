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

"""A PTransform to extend each Variant's calls with data for all samples."""

from __future__ import absolute_import

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io import vcf_parser  # pylint: disable=unused-import

__all__ = ['DensifyVariants']


class DensifyVariants(beam.PTransform):
  """Densifys each Variant's calls to contain data for `all_call_sample_ids`."""

  def __init__(self, all_call_sample_ids):
    # type: (List[str]) -> None
    """Initializes a `DensifyVariants` object.

    Args:
      all_call_sample_ids: A list of sample names that used to select/extend
      each variant calls.
    """
    self._all_call_sample_ids = all_call_sample_ids

  def _densify_variants(self, variant, all_call_sample_ids):
    # type: (vcf_parser.Variant, List[str]) -> vcf_parser.Variant
    """Cherry-picks calls for the variant.

    The calls are in the same order as the `all_call_sample_ids`.
    Args:
      variant: The variant that will be modified to contain calls for
        `all_call_sample_ids`.
      all_call_sample_ids: A list of sample names that used to cherry-pick each
        variant'calls. If one call is missing, an empty `VariantCall` is added.

    Returns:
      `variant` modified to contain calls for `all_call_sample_ids`.
    """
    existing_call_sample_id = {call.sample_id: call for call in variant.calls}

    new_calls = []
    for call_sample_id in all_call_sample_ids:
      if call_sample_id in existing_call_sample_id.keys():
        new_calls.append(existing_call_sample_id.get(call_sample_id))
      else:
        new_calls.append(
            vcfio.VariantCall(sample_id=call_sample_id,
                              genotype=vcfio.MISSING_GENOTYPE_VALUE))
    variant.calls = new_calls

    return variant

  def expand(self, pcoll):
    # Extend each variant's list of calls to contain `all_call_sample_ids`.
    return (pcoll
            | 'DensifyVariants' >> beam.Map(
                self._densify_variants,
                all_call_sample_ids=self._all_call_sample_ids))
