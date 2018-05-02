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

VariantPartition class basically returns an index for a given (
reference_name, pos) pair. The main utilization of this class is in
partition_for() function used by DataFlow pipeline.
"""

from __future__ import absolute_import

import re
from apache_beam.io.filesystems import FileSystems
from collections import defaultdict
from mmh3 import hash  # pylint: disable=no-name-in-module,redefined-builtin
import yaml
import intervaltree

# A reg exp that will match to standard reference_names such as "chr01" or "13".
_CHROMOSOME_NAME_REGEXP = re.compile(r'^(chr)?([0-9][0-9]?)$')
# Partition 0 to 21 is reserved for common reference_names such as "chr1".
_RESERVED_AUTO_PARTITIONS = 22
# If config_file_path_given == false we try to assign each chromosome to a partition.
# The first 22 partitions [0, 22) are reserved for standard reference_names.
# Every other identifiers will be matched to next available partitions [22, 27).
_DEFAULT_NUM_PARTITIONS = _RESERVED_AUTO_PARTITIONS + 5

# At most 1000 partitions can be set as output of VariantTransform.
_MAX_NUM_PARTITIONS = 1000
# Each partition can contain at most 64 regions.
_MAX_NUM_REGIONS = 64
# Matches to regions formatted as 'chr12:10,000-20,000' used in par_config.yaml
_REGION_LITERAL_REGEXP = re.compile(r'^(\S+):([0-9,]+)-([0-9,]+)$')
# A special literal for identifying default partition's region name.
_DEFAULT_REGION_LITERAL = 'residual'


class RegionIndexer(object):
  """Assigns indices to multiple regions or one index to a whole chromosome.

  If self._index != -1 then this instance is encoding one whole chromosome.
  If self._interval_tree != None this instance is encoding multiple regions.
  """

  def __init__(self):
    # Each instance contains either:
    #    one whole chromosome (in that case _index != -1).
    self._index = -1
    #    OR multiple regions of one chromosome (_interval_tree != None).
    self._interval_tree = None

  def add_interval(self, start, end, index):
    if self._index != -1:
      raise ValueError('Can not add region to an existing full chromosome.')
    if start < 0:
      raise ValueError('Start position on a region cannot be negative: {}'.
                       format(start))
    if end <= start:
      raise ValueError('End position must be larger than start position: {} '
                       'vs {}'.format(end, start))
    if index < 0:
      raise ValueError('Index of a region cannot be negative {}'.format(index))
    if not self._interval_tree:
      self._interval_tree = intervaltree.IntervalTree()
    else:
      if self._interval_tree.overlaps_range(start, end):
        raise ValueError('Cannot add overlapping region {}-{}'.
                         format(start, end))
    # If everything goes well we add the new region to the interval tree.
    self._interval_tree.addi(start, end, index)

  def add_index(self, index):
    if self._interval_tree is not None:
      raise ValueError('Can not add full chromosome to existing multi region.')
    if index < 0:
      raise ValueError('Index of a region cannot be negative {}'.format(index))
    self._index = index

  def get_index(self, pos=0):
    if self._interval_tree is None and self._index == -1:
      raise ValueError('This instance has not been assigned any role.')
    if self._interval_tree is None:
      return self._index
    else:
      matched_intervals = self._interval_tree.search(pos)
      if len(matched_intervals) > 1:
        raise ValueError('Search for pos {} returned more than 1 result'.
                         format(pos))
      if len(matched_intervals) == 1:
        return next(iter(matched_intervals)).data
      else:
        return -1

  @property
  def is_full_chromosome(self):
    return self._index != -1

class VariantPartition(object):
  """Partition variants based on their reference_name and/or position.

  This class has 2 operating modes:
    1) No config file is given (config_file_path == ''):
        Automatically partition variants based on their reference_name.
    2) A config file is given:
        partitions variants based on input config file.
  """

  def __init__(self, config_file_path=''):
    if not config_file_path:
      if _DEFAULT_NUM_PARTITIONS <= _RESERVED_AUTO_PARTITIONS:
	raise ValueError(
	    '_DEFAULT_NUM_PARTITIONS must be > _RESERVED_AUTO_PARTITIONS')
      self._config_file_path_given = False
      self._reference_name_to_partition_map = {}
    else:
      self._config_file_path_given = True
      self._parse_config(config_file_path)

  def _parse_config(self, config_file_path):
    """Parses the given partition config file.

    Args:
      config_file_path: name of the input partition_config file.
    Raises:
      A ValueError if any of the expected config formats are violated.
    """

    def _parse_position(pos_str):
      return int(pos_str.replace(',', ''))

    with FileSystems.open(config_file_path, 'r') as f:
      try:
        partition_configs = yaml.load(f)
      except yaml.YAMLError as e:
        raise ValueError('Invalid yaml file: %s' % str(e))

    if len(partition_configs) > _MAX_NUM_PARTITIONS:
      raise ValueError(
          'There can be at most {} partitions but given config file contains {}'
          .format(_MAX_NUM_PARTITIONS, len(partition_configs)))
    if len(partition_configs) == 0:
      raise ValueError('There must be at least one partition in config file.')

    self._partition_no = len(partition_configs)

    self._by_ref_name = defaultdict(RegionIndexer)

    self._suffixes = []
    table_suffix_duplicate = {}

    # Default partition will contain all remaining readings that do not match to
    # any other partition. -1 means it does not exist.
    self._default_partition_index = -1
    partition_index = 0
    for partition_config in partition_configs:
      partition = partition_config.get('partition', None)
      if partition is None:
        raise ValueError('Wrong yaml file format, partition field missing.')

      regions = partition.get('regions', None)
      if regions is None:
        raise ValueError('Each partition must have at least one region.')
      if len(regions) > _MAX_NUM_REGIONS:
        raise ValueError(
            'At most {} regions per partition, thie partition  contains {}'
            .format(_MAX_NUM_REGIONS, len(regions)))
      # Check whether this is the default region.
      if (len(regions) == 1 and
          regions[0].strip().lower() == _DEFAULT_REGION_LITERAL):
        if self._default_partition_index != -1:
          raise ValueError(
            'There must be only one default partition intercepted at least 2')
        else:
          self._default_partition_index = partition_index
      for r in regions:
        matched = _REGION_LITERAL_REGEXP.match(r)
        if matched:
          ref_name, start, end = matched.groups()
          ref_name = ref_name.strip().lower()
          start = _parse_position(start)
          end = _parse_position(end)

          region_indexer = self._by_ref_name.get(ref_name, None)
          if region_indexer and region_indexer.is_full_chromosome:
            raise ValueError(
                'Can not add region to an existing full chromosome.')
          self._by_ref_name[ref_name].add_interval(start, end,
                                                   partition_index)
        else:
          # This region includes a full chromosome
          ref_name = r.strip().lower()
          if self._by_ref_name.get(ref_name, None):
            raise ValueError('A full chromosome must be disjoint from all '
                             'other regions, {} is not'.format(ref_name))
          self._by_ref_name[ref_name].add_index(partition_index)

      if partition.get('partition_name', None) is None:
        raise ValueError('Each partition must have partition_name field.')
      suffix = partition.get('partition_name').strip()
      if table_suffix_duplicate.get(suffix, None) is not None:
        raise ValueError('Table names must be unique, {} is duplicated'
                         .format(suffix))
      table_suffix_duplicate[suffix] = True
      self._suffixes.append(suffix)
      partition_index += 1

  def get_num_partitions(self):
    """Returns the number of partitions."""
    if not self._config_file_path_given:
      return _DEFAULT_NUM_PARTITIONS
    else:
      if self.is_default_partition_absent():
        # In this case we have a dummy partition for all remaining variants.
        return self._partition_no + 1
      else:
        return self._partition_no

  def get_partition(self, reference_name, pos=0):
    # type: (str, int) -> int
    reference_name = reference_name.strip().lower()
    if not reference_name or pos < 0:
      raise ValueError('Cannot partition given input {}:{}'
                       .format(reference_name, pos))
    if self._config_file_path_given:
      return self._get_config_partition(reference_name, pos)
    else:
      return self._get_auto_partition(reference_name)

  def _get_config_partition(self, reference_name, pos):
    # Lookup _by_ref_name dict to see if we have an entry for this ref_name.
    indexer = self._by_ref_name.get(reference_name, None)
    if indexer is not None:
      index = indexer.get_index(pos)
      if index != -1:
        return index
    # No full/partial chromosome match, return default partition.
    return self.get_default_partition_index()

  def _get_auto_partition(self, reference_name):
    # type: (str) -> int
    """Automatically chooses an partition for the given reference_name.

    Given a reference_name returns an index in [0, _DEFAULT_NUM_PARTITIONS)
    range. In order to make this lookup less computationally intensive we first:
      1) Lookup the reference_name in _reference_name_to_partition_map dict

    If the result of lookup is None, we will try the following steps:
      2) Match the reference_name to a reg exp of common names (eg 'chr12')
      3) Hash the reference_name and calculate its mod to remaining buckets
    result will be added to _reference_name_to_partition_map for future lookups.

    Args:
      reference_name: reference name of the variant which is being partitioned
    Returns:
      An integer in the range of [0, _DEFAULT_NUM_PARTITIONS)
    """
    partition = self._reference_name_to_partition_map.get(reference_name, None)
    if partition is None:
      matched = _CHROMOSOME_NAME_REGEXP.match(reference_name)
      if matched:
        # First match the reference_name to the common formats.
        _, chr_no = matched.groups()
        chr_no = int(chr_no)
        if chr_no > 0 and chr_no <= _RESERVED_AUTO_PARTITIONS:
          partition = chr_no - 1
          self._reference_name_to_partition_map[reference_name] = partition
          return partition
      # If RegExp didn't match, we will find the hash of reference_name
      remaining_partitions = _DEFAULT_NUM_PARTITIONS - _RESERVED_AUTO_PARTITIONS
      partition = (hash(reference_name) % remaining_partitions +
                   _RESERVED_AUTO_PARTITIONS)
      # Save partition in _reference_name_to_partition dict for future lookups
      self._reference_name_to_partition_map[reference_name] = partition
    return partition

  def is_default_partition_absent(self):
    if self._config_file_path_given:
      return self._default_partition_index == -1
    else:
      # default partition does not exist, return -1.
      return False

  def get_default_partition_index(self):
    # If not _config_file_path_given default partition does not exist, return -1.
    if not self._config_file_path_given:
      return -1

    if self.is_default_partition_absent():
      return self._partition_no
    else:
      return self._default_partition_index

  def get_suffix(self, index):
    if index >= self._partition_no or index < 0:
      raise ValueError('Given partition index is outside of expected range.')
    return self._suffixes[index]
