# Copyright 2018 Google Inc.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License');
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

"""Encapsulates all partitioning logic used by VCF to BigQuery pipeline.

VariantSharding class basically returns an index for a given
(reference_name, pos) pair. The main utilization of this class is in
partition_for() function used by DataFlow pipeline.
This class has 2 main operating modes:
  1) Automatic: it will partition variants based on their reference_name
  2) Based on user provided config file: Users can parition output tables as
     they wish by providing a partition config file, example config files are
     available at gcp_variant_transforms/testing/data/misc/*.yaml
"""

from __future__ import absolute_import

from collections import defaultdict
import re
from typing import List, Optional  # pylint: disable=unused-import

from apache_beam.io.filesystems import FileSystems
import intervaltree
import yaml

from gcp_variant_transforms.libs import genomic_region_parser

# At most 100 shards (distinct tables) can be set as output of VariantTransform.
_MAX_NUM_SHARDS = 100
# Each shard can contain at most 10 regions.
_MAX_NUM_REGIONS = 10
# A special literal for identifying residual partition's region name.
_RESIDUAL_REGION_LITERAL = 'residual'
_UNDEFINED_SHARD_INDEX = -1

_TABLE_NAME_REGEXP = re.compile(r'^[a-zA-Z0-9_]*$')
# yaml config file constants
_OUTPUT_TABLE = 'output_table'
_TABLE_NAME_SUFFIX = 'table_name_suffix'
_REGIONS = 'regions'
_TOTAL_BASE_PAIRS = 'total_base_pairs'


class _ChromosomeSharder(object):
  """Assigns shard indices to multiple regions inside a chromosome.

  This class logic is implemented using an interval tree, each region is
  considered as an interval and will be added to the interval tree. Note all
  regions must be pairwise disjoint, i.e. no overlapping interval is accepted.
  """

  def __init__(self):
    # Each instance contains multiple regions of one chromosome.
    self._interval_tree = intervaltree.IntervalTree()

  def add_region(self, start, end, shard_index):
    if start < 0:
      raise ValueError(
          'Start position on a region cannot be negative: {}'.format(start))
    if end <= start:
      raise ValueError('End position must be larger than start position: {} '
                       'vs {}'.format(end, start))
    if shard_index < 0:
      raise ValueError(
          'Index of a region cannot be negative {}'.format(shard_index))
    if self._interval_tree.overlaps_range(start, end):
      raise ValueError('Wrong sharding config file, regions must be unique in '
                       'config file: {}-{}'.format(start, end))
    # If everything goes well we add the new region to the interval tree.
    self._interval_tree.addi(start, end, shard_index)

  def get_shard_index(self, pos=0):
    """Finds an interval that pos falls into and return its index.

    If no interval is found returns _UNDEFINED_SHARD_INDEX.
    """
    matched_regions = self._interval_tree.search(pos)
    # Ensure at most one region is matching to the give position.
    assert len(matched_regions) <= 1
    if len(matched_regions) == 1:
      return next(iter(matched_regions)).data
    else:
      return _UNDEFINED_SHARD_INDEX

class VariantSharding(object):
  """Sharding variants based on their reference_name [and position]."""

  def __init__(self, config_file_path=None):
    if not config_file_path or not config_file_path.strip():
      raise ValueError('You must provide path to a yaml config file.')
    self._use_interval_tree = self._validate_config_and_check_intervals(
        config_file_path)
    # Residual partition will contain all remaining variants that do not match
    # to any other partition.
    self._num_shards = 0
    self._residual_index = _UNDEFINED_SHARD_INDEX
    self._should_keep_residual = False
    # If none of the regions contain interval (such as "chr1:2000-3000") then we
    # don't need interval trees and shard index only depends on CHROM value.
    if self._use_interval_tree:
      self._region_to_shard = defaultdict(_ChromosomeSharder)
    else:
      self._region_to_shard = {}

    self._table_name_suffixes = []
    self._total_base_pairs = []

    self._parse_config(config_file_path)

  def _is_residual_shard(self, regions):
    # type: (List[str]) -> bool
    return (len(regions) == 1 and
            regions[0].strip() == _RESIDUAL_REGION_LITERAL)

  def _validate_config_and_check_intervals(self, config_file_path):
    # type: (str) -> bool
    """Validates the config file and finds if any region contains interval.
    Args:
      config_file_path: name of the input partition_config file.
    Raises:
      A ValueError if any of the expected config formats are violated.
    Returns:
      True if any region is interval, for example "chr1:1000-2000" is interval.
      False if all regions are simple CHROM value, for example "chr1".
    """
    has_any_interval = False
    with FileSystems.open(config_file_path, 'r') as f:
      try:
        shards = yaml.load(f)
      except yaml.YAMLError as e:
        raise ValueError('Invalid yaml file: {}'.format(str(e)))
    if len(shards) > _MAX_NUM_SHARDS:
      raise ValueError(
          'There can be at most {} output tables but given config file '
          'contains {}'.format(_MAX_NUM_SHARDS, len(shards)))
    if not shards:
      raise ValueError('At least one output table is needed in config file.')

    existing_suffixes = set()
    existing_ref_names = set()
    residual_partition_index = _UNDEFINED_SHARD_INDEX
    for item in shards:
      output_table = item.get(_OUTPUT_TABLE, None)
      if output_table is None:
        raise ValueError('Wrong sharing config file, {} field missing.'.format(
            _OUTPUT_TABLE))
      # Validate table_name_suffix
      table_name_suffix = output_table.get(_TABLE_NAME_SUFFIX)
      if not table_name_suffix:
        raise ValueError('Wrong sharding config file, {} field missing.'.format(
            _TABLE_NAME_SUFFIX))
      table_name_suffix = table_name_suffix.strip()
      if not table_name_suffix:
        raise ValueError(
            'Wrong sharding config file, table_name_suffix can not be empty.')
      if not _TABLE_NAME_REGEXP.match(table_name_suffix):
        raise ValueError(
            'Wrong sharding config file, BigQuery table name can only contain '
            'letters (upper or lower case), numbers, and underscores.')
      if table_name_suffix in existing_suffixes:
        raise ValueError('Wrong sharding config file, table name suffixes must '
                         'be unique, "{}" is not.'.format(table_name_suffix))
      existing_suffixes.add(table_name_suffix)

      # Validate regions
      regions = output_table.get(_REGIONS, None)
      if regions is None:
        raise ValueError('Wrong sharding config file, {} field missing.'.format(
            _REGIONS))
      if len(regions) > _MAX_NUM_REGIONS:
        raise ValueError('Wrong sharding config file, at most {} CHROM '
                         'values per output table is allowed: {}'.format(
                             _MAX_NUM_REGIONS, regions))
      if self._is_residual_shard(regions):
        if residual_partition_index != _UNDEFINED_SHARD_INDEX:
          raise ValueError('Wrong sharding config file, there can be only '
                           'one residual output table.')
        residual_partition_index += 1
      else:
        for r in regions:
          ref_name, start, end = genomic_region_parser.parse_genomic_region(r)
          if (start != genomic_region_parser._DEFAULT_START_POSITION or
              end != genomic_region_parser._DEFAULT_END_POSITION):
            has_any_interval = True
          else:
            if not ref_name:
              raise ValueError('Wrong sharding config file, reference_name can '
                               'not be empty string: {}'.format(r))
            if ref_name in existing_ref_names:
              raise ValueError('Wrong sharding config file, regions must be '
                               'unique in config file: {}'.format(ref_name))
            existing_ref_names.add(ref_name)

      # Validate total_base_pairs
      total_base_pairs = output_table.get(_TOTAL_BASE_PAIRS, None)
      if not total_base_pairs:
        raise ValueError('Wrong sharding config file, {} field missing.'.format(
            _TOTAL_BASE_PAIRS))
      if not isinstance(total_base_pairs, int):
        try:
          total_base_pairs = genomic_region_parser.parse_comma_sep_int(
              total_base_pairs)
        except:
          raise ValueError('Wrong sharding config file, each output table '
                           'needs an integer for total_base_pairs > 0.')
      if total_base_pairs <= 0:
        raise ValueError('Wrong sharding config file, each output table '
                         'needs an integer for total_base_pairs > 0.')
    return has_any_interval

  def _parse_config(self, config_file_path):
    # type: (str) -> None
    """Parses the given partitioning config file.
    Args:
      config_file_path: name of the input partition_config file.
    """
    with FileSystems.open(config_file_path, 'r') as f:
      try:
        shards = yaml.load(f)
      except yaml.YAMLError as e:
        raise ValueError('Invalid yaml file: {}'.format(str(e)))

    self._num_shards = len(shards)
    for shard_index in range(self._num_shards):
      output_table = shards[shard_index].get(_OUTPUT_TABLE)
      # Store table_name_suffix
      self._table_name_suffixes.insert(
          shard_index, output_table.get(_TABLE_NAME_SUFFIX).strip())
      # Store regions
      regions = output_table.get(_REGIONS, None)
      if self._is_residual_shard(regions):
        self._residual_index = shard_index
        self._should_keep_residual = True
        continue
      for r in regions:
        ref_name, start, end = genomic_region_parser.parse_genomic_region(r)
        if self._use_interval_tree:
          self._region_to_shard[ref_name].add_region(start, end, shard_index)
        else:
          self._region_to_shard[ref_name] = shard_index
      # Store num_base_pairs
      total_base_pairs = output_table.get(_TOTAL_BASE_PAIRS)
      if not isinstance(total_base_pairs, int):
        total_base_pairs = genomic_region_parser.parse_comma_sep_int(
            total_base_pairs)
      self._total_base_pairs.insert(shard_index, total_base_pairs)

    if self._residual_index == _UNDEFINED_SHARD_INDEX:
      # We add an extra dummy partition for residuals.
      # Note, here self._should_keep_residual is False.
      self._residual_index = self._num_shards
      self._num_shards += 1

  def get_shard_index(self, chrom, pos=None):
    # type: (str, int) -> int
    """Returns output table index for the given chrom value and position."""
    if not chrom or pos < 0:
      raise ValueError('Cannot shard given {}:{}'.format(chrom, pos))
    shard_index = _UNDEFINED_SHARD_INDEX
    if self._use_interval_tree:
      sharder = self._region_to_shard.get(chrom, None)
      if sharder:
        shard_index = sharder.get_shard_index(pos)
    else:
      shard_index = self._region_to_shard.get(chrom, _UNDEFINED_SHARD_INDEX)

    if shard_index == _UNDEFINED_SHARD_INDEX:
      return self._residual_index
    else:
      return shard_index

  def get_residual_index(self):
    return self._residual_index

  def should_keep_shard(self, shard_index):
    # type: (int) -> bool
    """Returns False only for dummy extra residual partition (if was added)."""
    if shard_index == self._residual_index:
      return self._should_keep_residual
    elif self._is_index_in_the_range(shard_index):
      return True
    else:
      raise ValueError(
          'Given shard index {} is outside of expected range: '
          '[0, {})'.format(shard_index, self._num_shards))

  def _is_index_in_the_range(self, shard_index):
    if shard_index < 0:
      return False
    if self._should_keep_residual:
      if shard_index >= self._num_shards:
        return False
    else:
      if shard_index >= self._num_shards - 1:
        return False
    return True

  def get_output_table_suffix(self, shard_index):
    # type: (int) -> Optional[str]
    if not self._is_index_in_the_range(shard_index):
      raise ValueError(
          'Given shard index {} is outside of expected range: '
          '[0, {})'.format(shard_index, self._num_shards))
    return self._table_name_suffixes[shard_index]

  def get_output_table_total_base_pairs(self, shard_index):
    # type: (int) -> Optional[int]
    if not self._is_index_in_the_range(shard_index):
      raise ValueError(
          'Given shard index {} is outside of expected range: '
          '[0, {})'.format(shard_index, self._num_shards))
    return self._total_base_pairs[shard_index]

  def get_num_shards(self):
    # type: (None) -> int
    return self._num_shards
