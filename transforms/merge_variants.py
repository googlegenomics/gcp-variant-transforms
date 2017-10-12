"""A PTransform for merging variants based on a strategy."""

import apache_beam as beam

from libs.variant_merge import variant_merge_strategy

__all__ = ['MergeVariants']


class MergeVariants(beam.PTransform):
  """Merges variants according to the specified strategy."""

  def __init__(self, variant_merger):
    """Initializes the transform.

    Args:
      variant_merger (``VariantMergeStrategy``): The strategy to use for merging
        variants. Must not be null.
    """
    assert isinstance(variant_merger,
                      variant_merge_strategy.VariantMergeStrategy)
    self._variant_merger = variant_merger

  def _map_by_variant_key(self, variant):
    return (self._variant_merger.get_merge_key(variant), variant)

  def _merge_variants_by_key(self, (key, variants)):
    return self._variant_merger.get_merged_variants(variants)

  def expand(self, pcoll):
    return (pcoll
            | 'MapVariantsByKey' >> beam.Map(self._map_by_variant_key)
            | 'GroupVariantsByKey' >> beam.GroupByKey()
            | 'MergeVariantsByKey' >> beam.FlatMap(self._merge_variants_by_key))
