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

__all__ = ['DensifyVariants']


class DensifyVariants(beam.PTransform):
  """Extends each Variant's calls to contain data for all samples."""

  def _get_call_names(self, variant):
    """Returns the names of all calls for the variant."""
    return [call.name for call in variant.calls]

  def _densify_variants(self, variant, all_call_names):
    """Adds all missing calls to the variant.

    Args:
      variant: The variant that will be modified to contain calls for
        all samples.
      all_call_names: A list of all sample names across all records in the
        collection.

    Returns:
      `variant` modified to contain calls for all samples.
    """
    variant_call_names = [call.name for call in variant.calls]
    missing_call_names = set(all_call_names) - set(variant_call_names)

    for call_name in missing_call_names:
      variant.calls.append(
          vcfio.VariantCall(name=call_name,
                            genotype=vcfio.MISSING_GENOTYPE_VALUE))

    return variant

  def expand(self, pcoll):
    # Get a list of all call names across variants.
    call_names = (pcoll
                  | 'GetCallNames' >> beam.FlatMap(self._get_call_names)
                  | 'RemoveDuplicates' >> beam.RemoveDuplicates()
                  | 'Combine' >> beam.combiners.ToList())

    # Extend each variant's list of calls to contain all samples.
    return (pcoll
            | 'DensifyVariants' >> beam.Map(
                self._densify_variants,
                all_call_names=beam.pvalue.AsSingleton(call_names)))


