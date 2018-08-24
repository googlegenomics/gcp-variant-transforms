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
  """Extends each Variant's calls to contain data for all samples."""

  def __init__(self, all_call_names):
    # type: (List[str]) -> None
    """Initializes a `DensifyVariants` object.

    Args:
      all_call_names: A list of sample names that used to extend each
        variant'calls.
    """
    self._all_call_names = all_call_names

  def _densify_variants(self, variant, all_call_names):
    # type: (vcf_parser.Variant, List[str]) -> vcf_parser.Variant
    """Adds all missing calls to the variant.

    The calls are in the same order as the `all_call_names`.
    Args:
      variant: The variant that will be modified to contain calls for
        `all_call_names`.
      all_call_names: A list of sample names that used to extend each
        variant'calls.

    Returns:
      `variant` modified to contain calls for `all_call_names`.
    """
    existing_call_name = {call.name: call for call in variant.calls}

    new_calls = []
    for call_name in all_call_names:
      if call_name in existing_call_name.keys():
        new_calls.append(existing_call_name.get(call_name))
      else:
        new_calls.append(
            vcfio.VariantCall(name=call_name,
                              genotype=vcfio.MISSING_GENOTYPE_VALUE))
    variant.calls = new_calls

    return variant

  def expand(self, pcoll):
    # Extend each variant's list of calls to contain all samples.
    return (pcoll
            | 'DensifyVariants' >> beam.Map(
                self._densify_variants, all_call_names=self._all_call_names))
