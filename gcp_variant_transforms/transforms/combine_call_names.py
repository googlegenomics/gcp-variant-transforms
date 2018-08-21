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

from typing import List  # pylint: disable=unused-import

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcf_parser  # pylint: disable=unused-import


class CallNamesCombiner(beam.PTransform):
  """A PTransform to combine call names from all variants."""

  def _get_call_names(self, variant):
    # type: (vcf_parser.Variant) -> List[str]
    """Returns the names of all calls for the variant."""
    return [call.name for call in variant.calls]

  def expand(self, pcoll):
    return (pcoll
            | 'GetCallNames' >> beam.FlatMap(self._get_call_names)
            | 'RemoveDuplicates' >> beam.RemoveDuplicates()
            | 'Combine' >> beam.combiners.ToList())
