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


import collections
import copy
from typing import Iterable, Set  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery  # pylint: disable=unused-import
from intervaltree import IntervalTree

from gcp_variant_transforms.beam_io import vcfio  # pylint: disable=unused-import
from gcp_variant_transforms.libs.variant_merge import move_to_calls_strategy
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy

__all__ = ['MergeWithNonVariantsStrategy']

# Default size of windows that variants will be grouped in based on the start
# position of the variant.
DEFAULT_WINDOW_SIZE = 2500
NON_VARIANT_ALTERNATES = ['<*>', '<NON_REF>']


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

  def __init__(self,
               info_keys_to_move_to_calls_regex,  # type: str
               copy_quality_to_calls,  # type: bool
               copy_filter_to_calls,  # type: bool
               window_size=DEFAULT_WINDOW_SIZE  # type: int
              ):
    # type: (...) -> None
    """Initializes the strategy.

    Args:
      info_keys_to_move_to_calls_regex: A regular expression specifying info
        fields that should be moved to calls.
      copy_quality_to_calls: Whether to copy the quality field to the associated
        calls in each record.
      copy_filter_to_calls: Whether to copy filter field to the  associated
        calls in each record.
      window_size: Size of windows that variants will be grouped in based on the
        start position of the variant.
    """
    self._move_to_calls = move_to_calls_strategy.MoveToCallsStrategy(
        info_keys_to_move_to_calls_regex=info_keys_to_move_to_calls_regex,
        copy_quality_to_calls=copy_quality_to_calls,
        copy_filter_to_calls=copy_filter_to_calls)
    self._window_size = window_size

  def get_merge_keys(self, variant):
    # type: (vcfio.Variant) -> Iterable[str]
    start = variant.start - (variant.start % self._window_size)
    for i in range(start, variant.end, self._window_size):
      yield ':'.join([str(x) for x in [variant.reference_name, i]])

  def get_merged_variants(self, variants, key=None):
    # type: (List[vcfio.Variant], str) -> Iterable[vcfio.Variant]
    non_variant_tree = IntervalTree()
    grouped_variants = collections.defaultdict(list)
    for v in variants:
      self._align_with_window(v, key)
      if self._is_non_variant(v):
        non_variant_tree.addi(v.start, v.end, v)
      else:
        try:
          group_key = next(self._move_to_calls.get_merge_keys(v))
        except StopIteration:
          continue
        else:
          grouped_variants[group_key].append(v)

    non_variants = self._merge_non_variants(non_variant_tree)
    variants = self._merge_variants(grouped_variants)

    non_variant_tree.clear()
    for nv in non_variants:
      non_variant_tree.addi(nv.start, nv.end, nv)

    splits = IntervalTree()
    for v in variants:
      non_variant_interval = non_variant_tree.search(v.start, v.end)
      if non_variant_interval:
        try:
          non_variant = next(iter(non_variant_interval)).data
        except StopIteration:
          continue
        else:
          v.calls.extend(non_variant.calls)
          v.calls = sorted(v.calls)
          self._update_splits(splits, v)
      yield v

    for non_variant in self._split_non_variants(non_variant_tree, splits):
      yield non_variant

  def _merge_non_variants(self, t):
    bounds = sorted({p for v in t for p in [v.begin, v.end]})
    intervals = [(bounds[i], bounds[i+1]) for i, _ in enumerate(bounds[:-1])]
    merged_non_variants = []
    for start, end in intervals:
      overlapping_non_variants = t[start:end]
      if not overlapping_non_variants:
        continue

      overlapping_variants = [ov.data for ov in overlapping_non_variants]
      merged_non_variant = copy.deepcopy(overlapping_variants[0])
      merged_non_variant.reference_bases = None
      merged_non_variant.start = start
      merged_non_variant.end = end

      for non_variant in overlapping_variants[1:]:
        merged_non_variant.names.extend(non_variant.names)
        merged_non_variant.filters.extend(non_variant.filters)
        if (merged_non_variant.quality is not None and
            non_variant.quality is not None):
          merged_non_variant.quality = min(merged_non_variant.quality,
                                           non_variant.quality)
        elif merged_non_variant.quality is None:
          merged_non_variant.quality = non_variant.quality
        merged_non_variant.calls.extend(non_variant.calls)

      merged_non_variant.names = sorted(set(merged_non_variant.names))
      merged_non_variant.filters = sorted(set(merged_non_variant.filters))
      merged_non_variant.calls = sorted(merged_non_variant.calls)

      merged_non_variants.append(merged_non_variant)

    return merged_non_variants

  def _merge_variants(self, grouped_variants):
    merged_variants = []
    for merge_key, variants in grouped_variants.items():
      merged_variants.extend(
          self._move_to_calls.get_merged_variants(variants, merge_key))
    return merged_variants

  def _update_splits(self, splits, v):
    start = v.start
    end = v.end
    # Increase split size to also combine adjacent splits
    overlapping_splits = splits.search(v.start - 1, v.end + 1)
    if overlapping_splits:
      for split in overlapping_splits:
        splits.remove(split)
        start = min(start, split.begin)
        end = max(end, split.end)
    splits.addi(start, end, True)

  def _split_non_variants(self, non_variant_tree, splits):
    for split in splits:
      overlapping_non_variants = non_variant_tree.search(split.begin, split.end)
      for onv_interval in overlapping_non_variants:
        non_variant_tree.remove(onv_interval)
        onv = onv_interval.data
        if split.begin <= onv.start and split.end >= onv.end:
          continue
        if split.begin <= onv.start:
          nv = copy.deepcopy(onv)
          nv.start = split.end
          non_variant_tree.addi(nv.start, nv.end, nv)
        elif split.end >= onv.end:
          nv = copy.deepcopy(onv)
          nv.end = split.begin
          non_variant_tree.addi(nv.start, nv.end, nv)
        else:
          left_nv = copy.deepcopy(onv)
          right_nv = copy.deepcopy(onv)
          left_nv.end = split.begin
          right_nv.start = split.end
          non_variant_tree.addi(left_nv.start, left_nv.end, left_nv)
          non_variant_tree.addi(right_nv.start, right_nv.end, right_nv)

    return [nv.data for nv in non_variant_tree]

  def _align_with_window(self, v, key):
    if not key:
      return
    start = self._get_window_start(key)
    end = start + self._window_size
    if v.start < start:
      v.start = start
    if v.end > end:
      v.end = end

  def _is_non_variant(self, v):
    if not v.alternate_bases:
      return True
    return any(v.alternate_bases == [a] for a in NON_VARIANT_ALTERNATES)

  def _get_window_start(self, key):
    """Returns the starting position of the window based on a merge key."""
    return int(key.split(':')[1])

  def modify_bigquery_schema(self, schema, info_keys):
    # type: (bigquery.TableSchema, Set[str]) -> None
    # Find the calls record so that it's easier to reference it below.
    self._move_to_calls.modify_bigquery_schema(schema, info_keys)
