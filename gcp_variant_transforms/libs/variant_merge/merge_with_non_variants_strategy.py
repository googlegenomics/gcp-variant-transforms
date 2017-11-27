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

"""Variant merge stategy that can handle both Variants and Non-variants."""

from __future__ import absolute_import

from intervaltree import IntervalTree

from gcp_variant_transforms.beam_io.vcfio import Variant
from gcp_variant_transforms.libs.variant_merge import move_to_calls_strategy
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy

__all__ = ['MergeWithNonVariantsStrategy']

# Default size of windows that variants will be grouped in based on the start
# position of the variant.
DEFAULT_WINDOW_SIZE = 2500


class MergeWithNonVariantsStrategy(variant_merge_strategy.VariantMergeStrategy):
  """A merging strategy that can handle non-variant records.

  Variants will be merged across files by using reference_name and start
  position as key. INFO fields should be moved to class if they match
  `info_keys_to_move_to_calls_regex`. Otherwise, one will be chosen as
  representatve (in no particular order) among the merged variants.
  Filters will be merged across all variants matching the key and the
  highest quality score will be chosen as representative for the merged
  variants. The filters and quality fields can be optionally copied to
  their associated calls using `copy_quality_to_calls` and
  `copy_filter_to_calls` options.
  Note: if a field is set to be moved from INFO to calls, then it must
  not already exist in calls (i.e. specified by FORMAT in the VCF
  header).
  """

  def __init__(self, info_keys_to_move_to_calls_regex, copy_quality_to_calls,
               copy_filter_to_calls, window_size=DEFAULT_WINDOW_SIZE):
    """Initializes the strategy.

    Args:
      info_keys_to_move_to_calls_regex (str): A regular expression
        specifying info fields that should be moved to calls.
      copy_quality_to_calls (bool): Whether to copy the quality field to
        the associated calls in each record.
      copy_filter_to_calls (bool): Whether to copy filter field to the
        associated calls in each record.
      window_size (int): Size of windows that variants will be grouped
        in based on the start position of the variant.
    """
    self._move_to_calls_strategy = move_to_calls_strategy.MoveToCallsStrategy(
        info_keys_to_move_to_calls_regex=info_keys_to_move_to_calls_regex,
        copy_quality_to_calls=copy_filter_to_calls,
        copy_filter_to_calls=copy_filter_to_calls)
    self._window_size = window_size

  def _merge_lists(self, list1, list2):
    return sorted(set(list1 + list2))

  def _merge(self, v1, v2):
    if (v1.start == v2.start
        and v1.end == v2.end
        and v1.alternate_bases == v2.alternate_bases):
      merged_variant = Variant(reference_name=v1.reference_name,
                               start=v1.start,
                               end=v1.end,
                               reference_bases=v1.reference_bases,
                               alternate_bases=v1.alternate_bases,
                               names=self._merge_lists(v1.names, v2.names),
                               filters=self._merge_lists(v1.filters, v2.filters),
                               quality=max(v1.quality, v2.quality),
                               calls=sorted(v1.calls + v2.calls))
      self._move_to_calls_strategy.move_data_to_merged(v1, merged_variant)
      self._move_to_calls_strategy.move_data_to_merged(v2, merged_variant)
      yield merged_variant
    else:
      # TODO(msaul): Add logic for handling non-variant regions
      yield v1
      yield v2

  def _can_merge(self, v1, v2):
    return (v1.start == v2.start
            and v1.end == v2.end
            and v1.alternate_bases == v2.alternate_bases)

  def get_merged_variants(self, variants):
    tree = IntervalTree()
    for v in variants:
      self._move_to_calls_strategy.move_data_to_calls(v)
      overlapping_variants = [ov.data for ov in tree[v.start:v.end]]
      tree[v.start:v.end] = v
      for ov in overlapping_variants:
        if self._can_merge(ov, v):
          tree.removei(ov.start, ov.end, ov)
          tree.removei(v.start, v.end, v)
          new_variants = self._merge(ov, v)
          for nv in new_variants:
            tree[nv.start:nv.end] = nv
          break
    return [v.data for v in sorted(tree)]

  def get_merge_keys(self, variant):
    start = variant.start - (variant.start % self._window_size)
    for i in range(start, variant.end, self._window_size):
      yield ':'.join([str(x) for x in [variant.reference_name, i]])

  def modify_bigquery_schema(self, schema, info_keys):
    # Find the calls record so that it's easier to reference it below.
    self._move_to_calls_strategy.modify_bigquery_schema(schema, info_keys)

