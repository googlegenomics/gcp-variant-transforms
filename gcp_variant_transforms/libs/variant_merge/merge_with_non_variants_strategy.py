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

import copy

from intervaltree import IntervalTree

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io.vcfio import Variant
from gcp_variant_transforms.libs import variant_utils
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

  def get_merge_keys(self, variant):
    start = variant.start - (variant.start % self._window_size)
    for i in range(start, variant.end, self._window_size):
      yield ':'.join([str(x) for x in [variant.reference_name, i]])

  def get_merged_variants(self, variants, key=None):
    tree = IntervalTree()
    variants = sorted(variants)
    for v in variants:
      # If a non-variant extends multiple windows, it will be mapped once to
      # each window. We must correct the start and end position so that each
      # read position is only featured once.
      self._align_with_window(v, key)
      self._move_to_calls_strategy.move_data_to_calls(v)
      overlapping_variants = sorted([ov.data for ov in tree[v.start:v.end]])
      for ov in overlapping_variants:
        if self._can_merge(ov, v):
          tree.removei(ov.start, ov.end, ov)

          new_variants, v = self._merge(ov, v)
          for nv in new_variants:
            tree[nv.start:nv.end] = nv
          if not v:
            break
      if v:
        tree[v.start:v.end] = v
    return [v.data for v in sorted(tree)]

  def _align_with_window(self, v, key):
    if not key:
      return
    start = self._get_window_start(key)
    end = start + self._window_size
    if v.start < start:
      v.start = start
    if v.end > end:
      v.end = end

  def _get_window_start(self, key):
    """Returns the starting position of the window based on a merge key."""
    return int(key.split(':')[1])

  def _can_merge(self, source, to_merge):
    """Determines if source and to_merge can be merged."""
    return (self._can_merge_variants(source, to_merge)
            or self._both_nonvs(source, to_merge)
            or variant_utils.is_snp(source) and variant_utils.is_non_variant(to_merge)
            or variant_utils.is_non_variant(source) and variant_utils.is_snp(to_merge))

  def _can_merge_variants(self, source, to_merge):
    """Determines if variants source and to_merge can be merged."""
    return ((self._both_snps(source, to_merge) or
             self._both_mnps(source, to_merge)) and
            source.alternate_bases == to_merge.alternate_bases)

  def _both_snps(self, v1, v2):
    return variant_utils.is_snp(v1) and variant_utils.is_snp(v2)

  def _both_mnps(self, v1, v2):
    return variant_utils.is_mnp(v1) and variant_utils.is_mnp(v2)

  def _both_nonvs(self, v1, v2):
    return variant_utils.is_non_variant(v1) and variant_utils.is_non_variant(v2)

  def _merge(self, source, to_merge):
    """Helper function for merging Variants source and to_merge."""
    if self._both_snps(source, to_merge) or self._both_mnps(source, to_merge):
      return self._merge_variants(source, to_merge)
    elif self._both_nonvs(source, to_merge):
      return self._merge_non_variants(source, to_merge)
    elif variant_utils.is_snp(source) and variant_utils.is_non_variant(to_merge):
      return self._merge_snp_with_non_variant(source, to_merge)
    elif variant_utils.is_non_variant(source) and variant_utils.is_snp(to_merge):
      return self._merge_non_variant_with_snp(source, to_merge)
    else:
      return [source], to_merge

  def _merge_variants(self, source, to_merge):
    """Helper function for merging two variants."""
    merged_variant = Variant(reference_name=source.reference_name,
                             start=source.start,
                             end=source.end,
                             reference_bases=source.reference_bases,
                             alternate_bases=source.alternate_bases,
                             names=self._merge_lists(source.names, to_merge.names),
                             filters=self._merge_lists(source.filters, to_merge.filters),
                             quality=max(source.quality, to_merge.quality),
                             calls=sorted(source.calls + to_merge.calls))
    self._move_to_calls_strategy.move_data_to_merged(source, merged_variant)
    self._move_to_calls_strategy.move_data_to_merged(to_merge, merged_variant)
    return [merged_variant], None

  def _merge_lists(self, list1, list2):
    """Returns the union of list1 and list2."""
    return sorted(set(list1 + list2))

  def _merge_non_variants(self, source, to_merge):
    """Helper function for merging to non variants."""
    results = []
    leftover = None
    if source.start < to_merge.start:
      results.append(self._get_starting_interval(source, to_merge))

    if source.end > to_merge.end:
      results.append(self._get_ending_interval(source, to_merge))
    elif to_merge.end > source.end:
      leftover = self._get_ending_interval(to_merge, source)

    results.append(self._merge_non_variants_overlap(source, to_merge))
    return results, leftover

  def _get_starting_interval(self, source, to_merge):
    """Returns the first non-overlapping interval when merging two Variants."""
    starting_interval = self._copy_variant(source)
    starting_interval.end = to_merge.start
    self._move_to_calls_strategy.move_data_to_merged(source, starting_interval)
    return starting_interval

  def _get_ending_interval(self, source, to_merge):
    """Returns the last non-overlapping interval when merging two Variants."""
    ending_interval = self._copy_variant(source)
    ending_interval.start = to_merge.end
    ending_interval.reference_bases = vcfio.MISSING_FIELD_VALUE
    self._move_to_calls_strategy.move_data_to_merged(source, ending_interval)
    return ending_interval

  def _copy_variant(self, v):
    return copy.deepcopy(v)

  def _merge_non_variants_overlap(self, source, to_merge):
    """Returns the merged overlapping regions of two non-variants."""
    reference_bases = (source.reference_bases
                       if source.start > to_merge.start
                       else to_merge.reference_bases)
    merged_variant = Variant(reference_name=source.reference_name,
                             start=max(source.start, to_merge.start),
                             end=min(source.end, to_merge.end),
                             reference_bases=reference_bases,
                             alternate_bases=source.alternate_bases,
                             names=self._merge_lists(source.names, to_merge.names),
                             filters=self._merge_lists(source.filters, to_merge.filters),
                             quality=min(source.quality, to_merge.quality),
                             calls=sorted(source.calls + to_merge.calls))
    self._move_to_calls_strategy.move_data_to_merged(source, merged_variant)
    self._move_to_calls_strategy.move_data_to_merged(to_merge, merged_variant)
    return merged_variant

  def _merge_snp_with_non_variant(self, source, to_merge):
    """Helper function for merging a SNP with a non-variant."""
    leftover = None
    if to_merge.end > source.end:
      leftover = self._get_ending_interval(to_merge, source)

    merged_variant = self._copy_variant(source)
    merged_variant.calls.extend(to_merge.calls)
    merged_variant.calls.sort()
    self._move_to_calls_strategy.move_data_to_merged(to_merge, merged_variant)
    return [merged_variant], leftover

  def _merge_non_variant_with_snp(self, source, to_merge):
    results = []
    if source.start < to_merge.start:
      results.append(self._get_starting_interval(source, to_merge))
    if source.end > to_merge.end:
      results.append(self._get_ending_interval(source, to_merge))

    merged_variant = self._copy_variant(to_merge)
    merged_variant.calls.extend(source.calls)
    merged_variant.calls.sort()
    self._move_to_calls_strategy.move_data_to_merged(to_merge, merged_variant)
    results.append(merged_variant)
    return results, None

  def modify_bigquery_schema(self, schema, info_keys):
    # Find the calls record so that it's easier to reference it below.
    self._move_to_calls_strategy.modify_bigquery_schema(schema, info_keys)
