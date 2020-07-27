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

"""A PTransform for merging variants based on a strategy."""



import apache_beam as beam

from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy

__all__ = ['MergeVariants']


class MergeVariants(beam.PTransform):
  """Merges variants according to the specified strategy."""

  def __init__(self, variant_merger):
    # type: (variant_merge_strategy.VariantMergeStrategy) -> None
    """Initializes the transform.

    Args:
      variant_merger: The strategy to use for merging variants. Must not be
        null.
    """
    assert isinstance(variant_merger,
                      variant_merge_strategy.VariantMergeStrategy)
    self._variant_merger = variant_merger

  def _map_by_variant_keys(self, variant):
    for key in self._variant_merger.get_merge_keys(variant):
      yield (key, variant)

  def _merge_variants_by_key(self, xxx_todo_changeme):
    (key, variants) = xxx_todo_changeme
    return self._variant_merger.get_merged_variants(variants, key)

  def expand(self, pcoll):
    return (pcoll
            | 'MapVariantsByKey' >> beam.FlatMap(self._map_by_variant_keys)
            | 'GroupVariantsByKey' >> beam.GroupByKey()
            | 'MergeVariantsByKey' >> beam.FlatMap(self._merge_variants_by_key))
