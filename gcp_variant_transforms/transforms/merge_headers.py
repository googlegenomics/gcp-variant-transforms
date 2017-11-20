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

import apache_beam as beam
from gcp_variant_transforms.beam_io import vcf_header_io

__all__ = ['MergeHeadersFn']


class MergeHeadersFn(beam.CombineFn):
  """Combiner function for merging VCF file headers."""

  def create_accumulator(self):
    return vcf_header_io.VcfHeader()

  def add_input(self, source, to_merge):
    return self.merge_accumulators([source, to_merge])

  def merge_accumulators(self, accumulators):
    merged_headers = self.create_accumulator()
    for to_merge in accumulators:
      merged_headers.update(to_merge)
    return merged_headers

  def extract_output(self, merged_headers):
    return merged_headers


class MergeHeaders(beam.PTransform):
  """A PTransform to merge VCF file headers."""

  def expand(self, pcoll):
    return pcoll | 'MergeHeaders' >> beam.CombineGlobally(MergeHeadersFn())
