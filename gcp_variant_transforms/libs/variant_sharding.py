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
import sys
import intervaltree
from mmh3 import hash  # pylint: disable=no-name-in-module,redefined-builtin
import yaml

from apache_beam.io.filesystems import FileSystems

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
_CHROM_VALUES = 'CHROM_values'
_TOTAL_BASE_PAIRS = 'total_base_pairs


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
      raise ValueError(
          'Cannot add overlapping region {}-{}'.format(start, end))
    # If everything goes well we add the new region to the interval tree.
    self._interval_tree.addi(start, end, shard_index)

  def get_index(self, pos=0):
    """Finds a region that includes pos, if none _UNDEFINED_PARTITION_INDEX."""
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
    self._use_interval_tree = self._validate_config(config_file_path)

    # Residual partition will contain all remaining variants that do not match
    # to any other partition.
    self._num_output_tables = 0
    self._residual_index = _UNDEFINED_PARTITION_INDEX
    self._should_keep_residual = False

    if self._use_interval_tree:
      self._region_to_shard = defaultdict(_ChromosomeSharder)
    else:
      self._region_to_shard = {}

    self._table_name_suffixes = []
    self._total_base_pairs = []

    self._parse_config(config_file_path)

  def _is_residual_table(self, regions):
    # type: (List[str]) -> bool
    return (len(regions) == 1 and
            regions[0].strip().lower() == _RESIDUAL_REGION_LITERAL)

  def _validate_config(self, config_file_path):
    # type: (str) -> bool
    # Validates the config file and also find if there are any intervals in the
    # regions, for example: chr1:1000-2000 is an interval.
    has_any_interval = False
    with FileSystems.open(config_file_path, 'r') as f:
      try:
        output_tables = yaml.load(f)
      except yaml.YAMLError as e:
        raise ValueError('Invalid yaml file: {} .'.format(str(e)))
    if len(output_tables) > _MAX_NUM_OUTPUT_TABLES:
      raise ValueError(
          'There can be at most {} output tables but given config file '
          'contains {} .'.format(_MAX_NUM_OUTPUT_TABLES, len(output_tables)))
    if not output_tables:
      raise ValueError('At least one output table is needed in config file.')

    existing_suffixes = set()
    existing_regions = set()
    residual_partition_index = _UNDEFINED_PARTITION_INDEX
    for item in output_tables:
      output_table = item.get(_OUTPUT_TABLE, None)
      if output_table is None:
        raise ValueError('Wrong yaml file format, {} field missing.'.format(
          _OUTPUT_TABLE))
      # Validate table_name_suffix
      table_name_suffix = output_table.get(_TABLE_NAME_SUFFIX)
      if not table_name_suffix:
        raise ValueError('Wrong yaml file format, {} field missing.'.format(
          _TABLE_NAME_SUFFIX))
      table_name_suffix = table_name_suffix.strip()
      if not table_name_suffix:
        raise ValueError('table_name_suffix can not be empty string.')
      if not _TABLE_NAME_REGEXP.match(table_name_suffix):
        raise ValueError('BigQuery table name can only contain letters (upper '
                         'or lower case), numbers, and underscores.')
      if table_name_suffix in existing_suffixes:
        raise ValueError('Table name suffixes must be unique, '
                         '{} is duplicated.'.format(table_name_suffix))
      existing_suffixes.add(table_name_suffix)

      # Validate regions
      regions = output_table.get(_CHROM_VALUES, None)
      if regions is None:
        raise ValueError('Wrong yaml file format, {} field missing.'.format(
          _CHROM_VALUES))
      if len(regions) > _MAX_NUM_CHROM_VALUES:
        raise ValueError(
          'At most {} CHROM values per output table is allowed: {}.'.format(_MAX_NUM_CHROM_VALUESS, regions))
      if self._is_residual_table(regions):
        if residual_partition_index != _UNDEFINED_PARTITION_INDEX:
          raise ValueError('There can be only one residual output table.')
        residual_partition_index += 1
      for r in regions:
        ref_name, start, end = genomic_region_parser.parse_genomic_region(r)
        if (start != genomic_region_parser._DEFAULT_START_POSITION or
            end != genomic_region_parser._DEFAULT_END_POSITION)
          has_any_interval = True
        else:
          ref_name = ref_name.strip().lower()
          if not ref_name:
            raise ValueError(
              'reference_name can not be empty string: {}'.format(r))
          if ref_name in existing_regions:
            raise ValueError(
              'regions must be unique in config file: {} .'.format(ref_name))
          existing_regions.add(ref_name)

      # Validate total_base_pairs
      total_base_pairs = output_table.get(_TOTAL_BASE_PAIRS, None)
      if not total_base_pairs:
        raise ValueError('Wrong yaml file format, {} field missing.'.format(
          _TOTAL_BASE_PAIRS))
      if type(total_base_pairs) is not int or total_base_pairs <= 0:
        raise ValueError('Each output table needs an int total_base_pairs > 0.')
    return has_any_interval

  def _parse_config(self, config_file_path):
    # type: (str) -> None
    """Parses the given partitioning config file.
    Args:
      config_file_path: name of the input partition_config file.
    Raises:
      A ValueError if any of the expected config formats are violated.
    """
    with FileSystems.open(config_file_path, 'r') as f:
      try:
        output_tables = yaml.load(f)
      except yaml.YAMLError as e:
        raise ValueError('Invalid yaml file: {} .'.format(str(e)))

    self._num_output_tables = len(output_tables)
    for table_index in range(self._num_output_tables):
      output_table = output_tables[table_index].get(_OUTPUT_TABLE)
      # Store table_name_suffix
      self._table_name_suffixes.insert(
        table_index, output_table.get(_TABLE_NAME_SUFFIX).strip())
      # Store regions
      regions = output_table.get(_CHROM_VALUES, None)
      if self._is_residual_table(regions):
        self._residual_index = table_index
        self._should_keep_residual = True
        continue
      for r in regions:
        if self._use_interval_tree:
          ref_name, start, end = genomic_region_parser.parse_genomic_region(r)
          ref_name = ref_name.lower()
          self._region_to_shard[ref_name].add_region(start, end, table_index)
        else:
          self._region_to_shard[r.strip().lower()] = table_index
      # Store num_base_pairs
      self._total_base_pairs.insert(table_index,
                                    output_table.get(_TOTAL_BASE_PAIRS))

    if self._residual_index == _UNDEFINED_PARTITION_INDEX:
      # We add an extra dummy partition for residuals.
      # Note, here self._should_keep_residual is False.
      self._residual_index = self._num_output_tables
      self._num_output_tables += 1

  def get_num_partitions(self):
    # type: (None) -> int
    return self._num_output_tables

  def get_output_table_index(self, chrom, pos=None):
    # type: (str, int) -> int
    """Returns output table index for the given chrom value and position."""
    if not chrom or pos < 0:
      raise ValueError('Cannot shard given {}:{}'.format(chrom, pos))
    shard_index = _UNDEFINED_SHARD_INDEX
    if self._use_interval_tree:
      sharder = self._region_to_shard.get(chrom, None)
      if sharder:
        shard_index = sharder.get_index(pos)
    else:
      shard_index = self._region_to_shard.get(chrom, _UNDEFINED_SHARD_INDEX)

    if shard_index == _UNDEFINED_SHARD_INDEX:
      return self._residual_index
    else:
      return shard_index


  def should_keep_output_table(self, output_table_index):
    # type: (int) -> bool
    """Returns False only for dummy extra residual partition (if was added)."""
    if output_table_index != self._residual_index:
      return True
    else:
      return self._should_keep_residual

  def _is_index_in_the_range(self, output_table_index):
    if output_table_index < 0:
      return False
    if self._should_keep_residual:
      if output_table_index >= self._num_output_tables:
        return False
    else:
      if output_table_index >= self._num_output_tables - 1:
        return False
    return True

  def get_output_table_suffix(self, output_table_index):
    # type: (int) -> Optional[str]
    if not self._is_index_in_the_range(partition_index):
      raise ValueError(
        'Given output index {} is outside of expected range: '
        '[0, {}]'.format(output_table_index, self._num_output_tables))
    return self._table_name_suffixes[output_table_index]

  def get_output_table_num_base_pairs(self, output_table_index):
    # type: (int) -> Optional[int]
    if not self._is_index_in_the_range(partition_index):
      raise ValueError(
        'Given output index {} is outside of expected range: '
        '[0, {}]'.format(output_table_index, self._num_output_tables))
    return self._num_base_paris[output_table_index]






















  def _validate_config(self, config_file_path):
    # type: (str) -> None
    with FileSystems.open(config_file_path, 'r') as f:
      try:
        sharding_configs = yaml.load(f)
      except yaml.YAMLError as e:
        raise ValueError('Invalid yaml file: %s' % str(e))
    if len(sharding_configs) > _MAX_NUM_SHARDS:
      raise ValueError(
          'There can be at most {} shards but given config file '
          'contains {}'.format(_MAX_NUM_SHARDS, len(sharding_configs)))
    if not sharding_configs:
      raise ValueError('There must be at least one shard in config file.')

    existing_shard_names = set()
    for shard_config in sharding_configs:
      shard = shard_config.get('partition', None)
      if shard is None:
        raise ValueError('Wrong yaml file format, shard field missing.')
      regions = shard.get('regions', None)
      if regions is None:
        raise ValueError('Each shard must have at least one region.')
      if len(regions) > _MAX_NUM_REGIONS:
        raise ValueError('At most {} regions per shard, this shard '
                         'contains {}'.format(_MAX_NUM_REGIONS, len(regions)))
      if not shard.get('partition_name', None):
        raise ValueError('Each shard must have partition_name field.')
      shard_name = shard.get('partition_name').strip()
      if not shard_name:
        raise ValueError('Shard name can not be empty string.')
      if shard_name in existing_shard_names:
        raise ValueError('Shard names must be unique, '
                         '{} is duplicated'.format(shard_name))
      existing_shard_names.add(shard_name)
    return sharding_configs

  def _parse_config(self, config_file_path):
    # type: (str) -> None
    """Parses the given sharding config file.

    Args:
      config_file_path: name of the input sharding_config file.
    Raises:
      A ValueError if any of the expected config formats are violated.
    """
    def _is_residual_shard(regions):
      # type: (List[str]) -> bool
      return (len(regions) == 1 and
              regions[0].strip().lower() == _RESIDUAL_REGION_LITERAL)

    sharding_configs = self._validate_config(config_file_path)

    self._num_shards = len(sharding_configs)
    for shard_index in range(self._num_shards):
      shard = sharding_configs[shard_index].get('partition')
      self._shard_names[shard_index] = (
          shard.get('partition_name').strip())
      regions = shard.get('regions', None)

      if _is_residual_shard(regions):
        if self._residual_shard_index != _UNDEFINED_SHARD_INDEX:
          raise ValueError('There must be only one residual shard.')
        self._residual_shard_index = shard_index
        self._should_keep_residual_shard = True
        continue

      for r in regions:
        ref_name, start, end = genomic_region_parser.parse_genomic_region(r)
        ref_name = ref_name.lower()
        self._ref_name_to_shard_map[ref_name].add_region(
            start, end, shard_index)

    if self._residual_shard_index == _UNDEFINED_SHARD_INDEX:
      # We add an extra dummy partition for residuals.
      # Note, here self._should_keep_residual_partition is False.
      self._residual_shard_index = self._num_shards
      self._num_shards += 1

  def get_num_shards(self):
    # type: (None) -> int
    return self._num_shards

  def get_shard(self, reference_name, pos=0):
    # type: (str, Optional[int]) -> int
    """Returns shard index on ref_name chromosome which pos falls into ."""
    reference_name = reference_name.strip().lower()
    if not reference_name or pos < 0:
      raise ValueError(
          'Cannot partition given input {}:{}'.format(reference_name, pos))
    if self._config_file_path_given:
      return self._get_config_shard(reference_name, pos)
    else:
      return self._get_auto_shard(reference_name)

  def _get_config_shard(self, reference_name, pos):
    # type: (str, int) -> int
    sharder = self._ref_name_to_shard_map.get(reference_name, None)
    if sharder:
      shard_index = sharder.get_shard_index(pos)
      if shard_index != _UNDEFINED_SHARD_INDEX:
        return shard_index
    # No match was found, returns residual partition index.
    return self._residual_shard_index

  def _get_auto_shard(self, reference_name):
    # type: (str) -> int
    """Automatically chooses an shard for the given reference_name.

    Given a reference_name returns an index in [0, _DEFAULT_NUM_SHARDS)
    range. In order to make this lookup less computationally intensive we first:
      1) Lookup the reference_name in _ref_name_to_shard_map dict

    If the result of lookup is None, we will try the following steps:
      2) Match the reference_name to a reg exp of common names (e.g. 'chr12') or
      3) Hash the reference_name and calculate its mod to remaining buckets
    result of 2-3 is added to _ref_name_to_shard_map for future lookups.

    Args:
      reference_name: reference name of the variant which is being sharded
    Returns:
      An integer in the range of [0, _DEFAULT_NUM_SHARDS)
    """
    sharder = self._ref_name_to_shard_map.get(reference_name, None)
    if sharder:
      return sharder.get_shard_index()
    else:
      matched = _CHROMOSOME_NAME_REGEXP.match(reference_name)
      if matched:
        # First match the reference_name to the common formats.
        _, chr_no = matched.groups()
        chr_no = int(chr_no)
        if chr_no > 0 and chr_no <= _RESERVED_AUTO_SHARDS:
          shard_index = chr_no - 1
          self._ref_name_to_shard_map[reference_name].add_region(
              0, sys.maxint, shard_index)
          return shard_index
      # If RegExp didn't match, we will find the hash of reference_name
      remaining_shards = _DEFAULT_NUM_SHARDS - _RESERVED_AUTO_SHARDS
      shard_index = (hash(reference_name) % remaining_shards +
                     _RESERVED_AUTO_SHARDS)
      # Save shard index in _reference_name_to_partition dict for future lookups
      self._ref_name_to_shard_map[reference_name].add_region(
          0, sys.maxint, shard_index)
      return shard_index

  def should_flatten(self):
    # type: (None) -> bool
    """In auto mode (no config) flattens shards, produces 1 output table."""
    return not self._config_file_path_given

  def should_keep_shard(self, shard_index):
    # type: (int) -> bool
    """Returns False only for dummy extra residual shard (if was added)."""
    if shard_index != self._residual_shard_index:
      return True
    else:
      return self._should_keep_residual_shard

  def get_shard_name(self, shard_index):
    # type: (int) -> Optional[str]
    if self._config_file_path_given:
      if shard_index >= self._num_shards or shard_index < 0:
        raise ValueError(
            'Given shard index {} is outside of expected range: '
            '[0, {}]'.format(shard_index, self._num_shards))
      return self._shard_names[shard_index]
    else:
      return None
