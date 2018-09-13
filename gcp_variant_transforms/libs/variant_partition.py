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
import sys
import intervaltree
from mmh3 import hash  # pylint: disable=no-name-in-module,redefined-builtin
import yaml

from apache_beam.io.filesystems import FileSystems

from gcp_variant_transforms.libs import genomic_region_parser

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
# A special literal for identifying residual partition's region name.
_RESIDUAL_REGION_LITERAL = 'residual'
_UNDEFINED_PARTITION_INDEX = -1


class _ChromosomePartitioner(object):
  """Assigns partition indices to multiple regions inside a chromosome.

  This class logic is implemented using an interval tree, each region is
  considered as an interval and will be added to the interval tree. Note all
  regions must be pairwise disjoint, i.e. no overlapping interval is accepted.
  """

  def __init__(self):
    # Each instance contains multiple regions of one chromosome.
    self._interval_tree = intervaltree.IntervalTree()

  def add_region(self, start, end, partition_index):
    if start < 0:
      raise ValueError(
          'Start position on a region cannot be negative: {}'.format(start))
    if end <= start:
      raise ValueError('End position must be larger than start position: {} '
                       'vs {}'.format(end, start))
    if partition_index < 0:
      raise ValueError(
          'Index of a region cannot be negative {}'.format(partition_index))
    if self._interval_tree.overlaps_range(start, end):
      raise ValueError(
          'Cannot add overlapping region {}-{}'.format(start, end))
    # If everything goes well we add the new region to the interval tree.
    self._interval_tree.addi(start, end, partition_index)

  def get_partition_index(self, pos=0):
    """Finds a region that includes pos, if none _UNDEFINED_PARTITION_INDEX."""
    matched_regions = self._interval_tree.search(pos)
    # Ensure at most one region is matching to the give position.
    assert len(matched_regions) <= 1
    if len(matched_regions) == 1:
      return next(iter(matched_regions)).data
    else:
      return _UNDEFINED_PARTITION_INDEX

class VariantPartition(object):
  """Partition variants based on their reference_name and position.

  This class has 2 operating modes:
    1) No config file is given (config_file_path == None):
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
    # Residual partition will contain all remaining variants that do not match
    # to any other partition.
    self._residual_partition_index = _UNDEFINED_PARTITION_INDEX
    self._should_keep_residual_partition = False
    self._ref_name_to_partitions_map = defaultdict(_ChromosomePartitioner)
    self._partition_names = {}

    if config_file_path:
      self._config_file_path_given = True
      self._parse_config(config_file_path)

  def _validate_config(self, config_file_path):
    # type: (str) -> None
    with FileSystems.open(config_file_path, 'r') as f:
      try:
        partition_configs = yaml.load(f)
      except yaml.YAMLError as e:
        raise ValueError('Invalid yaml file: %s' % str(e))
    if len(partition_configs) > _MAX_NUM_PARTITIONS:
      raise ValueError(
          'There can be at most {} partitions but given config file '
          'contains {}'.format(_MAX_NUM_PARTITIONS, len(partition_configs)))
    if not partition_configs:
      raise ValueError('There must be at least one partition in config file.')

    existing_partition_names = set()
    for partition_config in partition_configs:
      partition = partition_config.get('partition', None)
      if partition is None:
        raise ValueError('Wrong yaml file format, partition field missing.')
      regions = partition.get('regions', None)
      if regions is None:
        raise ValueError('Each partition must have at least one region.')
      if len(regions) > _MAX_NUM_REGIONS:
        raise ValueError('At most {} regions per partition, thie partition '
                         'contains {}'.format(_MAX_NUM_REGIONS, len(regions)))
      if not partition.get('partition_name', None):
        raise ValueError('Each partition must have partition_name field.')
      partition_name = partition.get('partition_name').strip()
      if not partition_name:
        raise ValueError('Partition name can not be empty string.')
      if partition_name in existing_partition_names:
        raise ValueError('Partition names must be unique, '
                         '{} is duplicated'.format(partition_name))
      existing_partition_names.add(partition_name)
    return partition_configs

  def _parse_config(self, config_file_path):
    # type: (str) -> None
    """Parses the given partition config file.

    Args:
      config_file_path: name of the input partition_config file.
    Raises:
      A ValueError if any of the expected config formats are violated.
    """
    def _is_residual_partition(regions):
      # type: (List[str]) -> bool
      return (len(regions) == 1 and
              regions[0].strip().lower() == _RESIDUAL_REGION_LITERAL)

    partition_configs = self._validate_config(config_file_path)

    self._num_partitions = len(partition_configs)
    for partition_index in range(self._num_partitions):
      partition = partition_configs[partition_index].get('partition')
      self._partition_names[partition_index] = (
          partition.get('partition_name').strip())
      regions = partition.get('regions', None)

      if _is_residual_partition(regions):
        if self._residual_partition_index != _UNDEFINED_PARTITION_INDEX:
          raise ValueError('There must be only one residual partition.')
        self._residual_partition_index = partition_index
        self._should_keep_residual_partition = True
        continue

      for r in regions:
        ref_name, start, end = genomic_region_parser.parse_genomic_region(r)
        self._ref_name_to_partitions_map[ref_name].add_region(
            start, end, partition_index)

    if self._residual_partition_index == _UNDEFINED_PARTITION_INDEX:
      # We add an extra dummy partition for residuals.
      # Note, here self._should_keep_residual_partition is False.
      self._residual_partition_index = self._num_partitions
      self._num_partitions += 1

  def get_num_partitions(self):
    # type: (None) -> int
    return self._num_partitions

  def get_partition(self, reference_name, pos=0):
    # type: (str, Optional[int]) -> int
    """Returns partition index on ref_name chromosome which pos falls into ."""
    reference_name = reference_name.strip().lower()
    if not reference_name or pos < 0:
      raise ValueError(
          'Cannot partition given input {}:{}'.format(reference_name, pos))
    if self._config_file_path_given:
      return self._get_config_partition(reference_name, pos)
    else:
      return self._get_auto_partition(reference_name)

  def _get_config_partition(self, reference_name, pos):
    # type: (str, int) -> int
    partitioner = self._ref_name_to_partitions_map.get(reference_name, None)
    if partitioner:
      partition_index = partitioner.get_partition_index(pos)
      if partition_index != _UNDEFINED_PARTITION_INDEX:
        return partition_index
    # No match was found, returns residual partition index.
    return self._residual_partition_index

  def _get_auto_partition(self, reference_name):
    # type: (str) -> int
    """Automatically chooses an partition for the given reference_name.

    Given a reference_name returns an index in [0, _DEFAULT_NUM_PARTITIONS)
    range. In order to make this lookup less computationally intensive we first:
      1) Lookup the reference_name in _ref_name_to_partitions_map dict

    If the result of lookup is None, we will try the following steps:
      2) Match the reference_name to a reg exp of common names (e.g. 'chr12') or
      3) Hash the reference_name and calculate its mod to remaining buckets
    result of 2-3 is added to _ref_name_to_partitions_map for future lookups.

    Args:
      reference_name: reference name of the variant which is being partitioned
    Returns:
      An integer in the range of [0, _DEFAULT_NUM_PARTITIONS)
    """
    partitioner = self._ref_name_to_partitions_map.get(reference_name, None)
    if partitioner:
      return partitioner.get_partition_index()
    else:
      matched = _CHROMOSOME_NAME_REGEXP.match(reference_name)
      if matched:
        # First match the reference_name to the common formats.
        _, chr_no = matched.groups()
        chr_no = int(chr_no)
        if chr_no > 0 and chr_no <= _RESERVED_AUTO_PARTITIONS:
          partition_index = chr_no - 1
          self._ref_name_to_partitions_map[reference_name].add_region(
              0, sys.maxint, partition_index)
          return partition_index
      # If RegExp didn't match, we will find the hash of reference_name
      remaining_partitions = _DEFAULT_NUM_PARTITIONS - _RESERVED_AUTO_PARTITIONS
      partition_index = (hash(reference_name) % remaining_partitions +
                         _RESERVED_AUTO_PARTITIONS)
      # Save partition in _reference_name_to_partition dict for future lookups
      self._ref_name_to_partitions_map[reference_name].add_region(
          0, sys.maxint, partition_index)
      return partition_index

  def should_flatten(self):
    # type: (None) -> bool
    """In auto mode (no config) flattens partitions, produces 1 output table."""
    return not self._config_file_path_given

  def should_keep_partition(self, partition_index):
    # type: (int) -> bool
    """Returns False only for dummy extra residual partition (if was added)."""
    if partition_index != self._residual_partition_index:
      return True
    else:
      return self._should_keep_residual_partition

  def get_partition_name(self, partition_index):
    # type: (int) -> Optional[str]
    if self._config_file_path_given:
      if partition_index >= self._num_partitions or partition_index < 0:
        raise ValueError(
            'Given partition index {} is outside of expected range: '
            '[0, {}]'.format(partition_index, self._num_partitions))
      return self._partition_names[partition_index]
    else:
      return None
