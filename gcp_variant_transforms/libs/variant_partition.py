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

VariantPartition class basically returns an index for a given
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
import intervaltree
from mmh3 import hash  # pylint: disable=no-name-in-module,redefined-builtin
import yaml

from apache_beam.io.filesystems import FileSystems

# A reg exp that will match to standard reference_names such as "chr01" or "13".
_CHROMOSOME_NAME_REGEXP = re.compile(r'^(chr)?([0-9][0-9]?)$')
# Partition 0 to 21 is reserved for common reference_names such as "chr1".
_RESERVED_AUTO_PARTITIONS = 22
# We try to assign each chromosome to a partition. The first 22 partitions
# [0, 22) are reserved for standard reference_names. Every other identifiers
# will be matched to next available partitions [22, 27).
_DEFAULT_NUM_PARTITIONS = _RESERVED_AUTO_PARTITIONS + 5

# At most 1000 partitions can be set as output of VariantTransform.
_MAX_NUM_PARTITIONS = 1000
# Each partition can contain at most 64 regions.
_MAX_NUM_REGIONS = 64
# Matches to regions formatted as 'chr12:10,000-20,000' used in par_config.yaml
_REGION_LITERAL_REGEXP = re.compile(r'^(\S+):([0-9,]+)-([0-9,]+)$')
# A special literal for identifying residual partition's region name.
_RESIDUAL_REGION_LITERAL = 'residual'


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
      # interval tree has not been initiated.
      self._interval_tree = intervaltree.IntervalTree()
    else:
      # Check the existing interval tree for possible overlap with new interval.
      if self._interval_tree.overlaps_range(start, end):
        raise ValueError('Cannot add overlapping region {}-{}'.
                         format(start, end))
    # If everything goes well we add the new region to the interval tree.
    self._interval_tree.addi(start, end, index)

  def add_index(self, index):
    if self._interval_tree is not None:
      raise ValueError('Can not add full chromosome to existing multi region.')
    if self._index != -1:
      raise ValueError('This instance has already been assinged an index.')
    if index < 0:
      raise ValueError('Index of a region cannot be negative {}'.format(index))
    self._index = index

  def get_index(self, pos=0):
    if self._interval_tree is None and self._index == -1:
      raise ValueError('This instance has not been assigned any index.')
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

  def __init__(self, config_file_path=None):
    if _DEFAULT_NUM_PARTITIONS <= _RESERVED_AUTO_PARTITIONS:
      raise ValueError(
          '_DEFAULT_NUM_PARTITIONS must be > _RESERVED_AUTO_PARTITIONS')
    self._num_partitions = _DEFAULT_NUM_PARTITIONS
    # This variable determines the operation mode auto (default mode) vs config.
    self._config_file_path_given = False
    # Residual partition will contain all remaining readings that do not match to
    # any other partition. -1 means it does not exist.
    self._residual_partition_index = -1
    self._by_ref_name = defaultdict(RegionIndexer)
    self._partition_names = []

    if config_file_path:
      self._config_file_path_given = True
      self._parse_config(config_file_path)

  def _validate_config(self, config_file_path):
    with FileSystems.open(config_file_path, 'r') as f:
      try:
        partition_configs = yaml.load(f)
      except yaml.YAMLError as e:
        raise ValueError('Invalid yaml file: %s' % str(e))
    if len(partition_configs) > _MAX_NUM_PARTITIONS:
      raise ValueError(
          'There can be at most {} partitions but given config file contains {}'
          .format(_MAX_NUM_PARTITIONS, len(partition_configs)))
    if not partition_configs:
      raise ValueError('There must be at least one partition in config file.')

    partition_name_duplicate = {}
    residual_partition_exist = False
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
      # Check whether this is the residual region.
      if (len(regions) == 1 and
          regions[0].strip().lower() == _RESIDUAL_REGION_LITERAL):
        if residual_partition_exist:
          raise ValueError(
              'There must be only one residual partition intercepted at least 2')
        else:
          residual_partition_exist = True
      # Check the partition_name is unique.
      if not partition.get('partition_name', None):
        raise ValueError('Each partition must have partition_name field.')
      partition_name = partition.get('partition_name').strip()
      if partition_name_duplicate.get(partition_name, None):
        raise ValueError('Table names must be unique, {} is duplicated'
                         .format(partition_name))
      partition_name_duplicate[partition_name] = True
    return partition_configs

  def _parse_config(self, config_file_path):
    """Parses the given partition config file.

    Args:
      config_file_path: name of the input partition_config file.
    Raises:
      A ValueError if any of the expected config formats are violated.
    """
    def _parse_position(pos_str):
      return int(pos_str.replace(',', ''))

    partition_configs = self._validate_config(config_file_path)

    self._num_partitions = len(partition_configs)
    for partition_index in range(self._num_partitions):
      partition = partition_configs[partition_index].get('partition')
      self._partition_names.append(partition.get('partition_name').strip())
      regions = partition.get('regions', None)
      # Check whether this is the residual partition.
      if (len(regions) == 1 and
          regions[0].strip().lower() == _RESIDUAL_REGION_LITERAL):
          self._residual_partition_index = partition_index
          continue
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

    if not self.has_residual_partition():
      # We add an extra dummy partition for residuals.
      self._num_partitions += 1

  def get_num_partitions(self):
    """Returns the number of partitions."""
    if not self._config_file_path_given:
      return _DEFAULT_NUM_PARTITIONS
    else:
      return self._num_partitions

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
    if indexer:
      index = indexer.get_index(pos)
      if index != -1:
        return index
    # No full/partial chromosome match, return residual partition.
    return self.get_residual_partition_index()

  def _get_auto_partition(self, reference_name):
    # type: (str) -> int
    """Automatically chooses an partition for the given reference_name.

    Given a reference_name returns an index in [0, _DEFAULT_NUM_PARTITIONS)
    range. In order to make this lookup less computationally intensive we first:
      1) Lookup the reference_name in _by_ref_name dict

    If the result of lookup is None, we will try the following steps:
      2) Match the reference_name to a reg exp of common names (eg 'chr12')
      3) Hash the reference_name and calculate its mod to remaining buckets
    result will be added to _by_ref_name for future quick lookups.

    Args:
      reference_name: reference name of the variant which is being partitioned
    Returns:
      An integer in the range of [0, _DEFAULT_NUM_PARTITIONS)
    """
    indexer = self._by_ref_name.get(reference_name, None)
    if indexer:
      # In auto mode all partitions are whole chromosome. 
      return indexer.get_index()
    else:
      matched = _CHROMOSOME_NAME_REGEXP.match(reference_name)
      if matched:
        # First match the reference_name to the common formats.
        _, chr_no = matched.groups()
        chr_no = int(chr_no)
        if chr_no > 0 and chr_no <= _RESERVED_AUTO_PARTITIONS:
          partition_index = chr_no - 1
          self._by_ref_name[reference_name].add_index(partition_index)
          return partition_index
      # If RegExp didn't match, we will find the hash of reference_name
      remaining_partitions = _DEFAULT_NUM_PARTITIONS - _RESERVED_AUTO_PARTITIONS
      partition_index = (hash(reference_name) % remaining_partitions +
                         _RESERVED_AUTO_PARTITIONS)
      # Save partition in _reference_name_to_partition dict for future lookups
      self._by_ref_name[reference_name].add_index(partition_index)
      return partition_index

  def has_residual_partition(self):
    return self._residual_partition_index != -1

  def should_flatten(self):
    # In case we are in auto mode (ie no config) we flatten partitions and
    # produce only one output table.
    return not self._config_file_path_given

  def get_residual_partition_index(self):
    # In auto mode residual partition doesnt exist, return -1.
    if not self._config_file_path_given:
      return -1

    # If residual partition is absent in the config file, we still need an index
    # for all residual variants. In this case we add a dummy paritition indexed
    # _num_partitions - 1 which will be ignored right after the partitioning.
    if self.has_residual_partition():
      return self._residual_partition_index
    else:
      return self._num_partitions - 1

  def get_partition_name(self, index):
    if self._config_file_path_given:
      if index >= self._num_partitions or index < 0:
        raise ValueError('Partition index is outside of expected range: {}'.
                         format(index))
      # Note, a '_' is automatically added, so output table name would be:
      #   known_args.output_table + '_' + partition_name
      return '_' + self._partition_names[index]
    else:
      if index != 0:
        raise ValueError('No config file is given, only index 0 is accepted.')
      return ''
