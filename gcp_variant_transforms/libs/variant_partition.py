# Copyright 2018 Google Inc.  All Rights Reserved.
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

"""Processes raw variant information using header information.

Note that for creating instances of the data objects in this module, there is a
factory function create_processed_variant. Other than that function, these
objects should be used as non-mutable in other scopes, hence all mutating
functions are "private".
"""

from __future__ import absolute_import

import re
from collections import defaultdict
from mmh3 import hash  # pylint: disable=no-name-in-module,redefined-builtin
import yaml
import intervaltree

# A reg exp that will match to standard reference_names such as "chr01" or "13".
_CHROMOSOME_NAME_REGEXP = re.compile(r'^(chr)?([0-9][0-9]?)$')
# If config_file_given == false we try to assign each chromosome to a partition.
# The first 22 partitions [0, 22) are reserved for standard reference_names.
# Every other identifers will be matched to next available partitions [22, 27).
_DEFAULT_PARTITION_NO = 22 + 1
# Partition 0 to 21 is reserved for common reference_names such as "chr1".
_RESERVED_PARTITIONS = 22


# At most 1000 partitions can be set as output of VariantTransform.
_MAX_PARTITIONS_NO = 1000
# Each partition can contain at most 64 regions.
_MAX_REGIONS_NO = 64

_REGION_LITERAL_REGEXP = re.compile(r'^(\S+):([0-9,]+)-([0-9,]+)$')


class VariantPartition(object):
  """ Reads and validates partition_config.yaml files."""

  def __init__(self, config_file=''):
    if not config_file:
      self._config_file_given = False
      self._reference_name_to_parition_map = {}
    else:
      self._config_file_given = True
      if not self._parse_config(config_file):
        raise ValueError('Given partition config file has wronge format: {}'
                         .format(config_file))

  def _parse_config(self, config_file):
    """ Parses the given partition config file.

        Args:
          config_file: name of the input partition_config file.
        Returns:
          A boolean indicating whether or not file was parsed successfully.
        Raises:
          A ValueError if any of the expected config formats are violated.
    """

    def _parse_position(pos_str):
      return int(pos_str.replace(',', ''))


    with open(config_file, 'r') as stream:
      try:
        config = yaml.load(stream)
      except yaml.YAMLError as exc:
        print exc

    if len(config) > _MAX_PARTITIONS_NO:
      raise ValueError(
          'There can be at most {} partitions but given config file contains {}'
          .format(_MAX_PARTITIONS_NO, len(config)))

    self._partition_no = len(config)

    self._by_chr = {}
    self._by_region = defaultdict(intervaltree.IntervalTree)
    self._suffixes = []
    table_suffix_duplicate = {}

    # Default partition will contain all remaining readings that do not match to
    # any other partition. -1 means it does not exist.
    self._default_partition_index = -1
    partition_index = 0
    for p in config:
      regions = p.get("partition").get("regions")
      if regions is None:
        if self._default_partition_index != -1:
          raise ValueError(
              'There must be only one default partition intercepted at least 2')
        else:
          self._default_partition_index = partition_index
      else:
        if len(regions) > _MAX_REGIONS_NO:
          raise ValueError(
              'At most {} regions per partition, thie partition  contains {}'
              .format(_MAX_REGIONS_NO, len(regions)))

        for r in regions:
          matched = _REGION_LITERAL_REGEXP.match(r)
          if matched:
            ref_name, start, end = matched.groups()
            ref_name = ref_name.strip().lower()
            start = _parse_position(start)
            end = _parse_position(end)

            tree = self._by_region.get(ref_name, None)
            if (tree is None) or (not tree.overlaps_range(start, end)):
              self._by_region[ref_name].addi(start, end, partition_index)
            else:
              raise ValueError('Cannot add overlapping region "{}:{}-{}"'
                               .format(ref_name, start, end))
          else:
            # This region includes a full chromosome
            ref_name = r.strip().lower()
            # Making sure this full-chromosome region is unique.
            if ((self._by_region.get(ref_name, None) is not None)
                or (self._by_chr.get(ref_name, None) is not None)):
              raise ValueError('A full chromosome must be disjoint from all '
                               'other regions, {} is not'.format(ref_name))
            self._by_chr[ref_name] = partition_index

      suffix = p.get("partition").get("partition_name").strip()
      if table_suffix_duplicate.get(suffix, None) is not None:
        raise ValueError('Table names need to be unique, {} is duplicated'
                         .format(suffix))
      table_suffix_duplicate[suffix] = True
      self._suffixes.append(suffix)
      partition_index += 1

    return len(self._suffixes) == self._partition_no

  def get_num_partitions(self):
    "Returns the number of partitions."

    if not self._config_file_given:
      return _DEFAULT_PARTITION_NO
    else:
      if self.is_default_partition_absent():
        # In this case we have a dummy partition for all remaining variants.
        return self._partition_no + 1
      else:
        return self._partition_no

  def get_partition(self, reference_name, pos=0):
    reference_name = reference_name.strip().lower()
    if ((not reference_name) or (pos < 0)):
      raise ValueError('Cannot partition given input {}:{}'
                       .format(reference_name, pos))
    if self._config_file_given:
      return self._get_config_partition(reference_name, pos)
    else:
      return self._get_auto_partition(reference_name)

  def _get_config_partition(self, reference_name, pos):
    # First look up full chromosome regions.
    partition = self._by_chr.get(reference_name, None)
    if partition is not None:
      return partition

    # Second look up partial chromosome regions (saved an interval tree).
    chr_regions = self._by_region.get(reference_name, None)
    if chr_regions is not None:
      interval_set = chr_regions.search(pos)
      if len(interval_set) > 1:
        raise ValueError('Point {}:{} falls into more than one region'
                         .format(reference_name, pos))
      if len(interval_set) == 1:
        return next(iter(interval_set)).data

    # No full/partial chromosome match, return default partition.
    return self.get_default_partition_index()

  def _get_auto_partition(self, reference_name):
    """A function that chooses an partition for the given reference_name.

    Given a reference_name returns an index in [0, _DEFAULT_PARTITION_NO) range.
    In order to make this look up less computation intensive we follow:
      1) Look up the reference_name in _reference_name_to_parition_map dict

    If the result of look up is None, we will try the following steps:
      2) Match the reference_name to a reg exp of common names (eg 'chr12')
      3) Hash the reference_name and calculate its mod to remaining buckets
    result will be added to _reference_name_to_parition_map for future look ups.

    Args:
      reference_name: reference name of the variant which is being partitioned
    Returns:
      An integer in the range of [0, _DEFAULT_PARTITION_NO)
    """
    partition = self._reference_name_to_parition_map.get(reference_name, None)
    if partition is None:
      matched = _CHROMOSOME_NAME_REGEXP.match(reference_name)
      if matched:
        # First match the reference_name to the common formats.
        _, chr_no = matched.groups()
        chr_no = int(chr_no)
        if 0 < chr_no <= _RESERVED_PARTITIONS:
          partition = chr_no - 1
          self._reference_name_to_parition_map[reference_name] = partition
          return partition
      # If RegExp didn't match, we will find the hash of reference_name
      remaining_partitions = _DEFAULT_PARTITION_NO - _RESERVED_PARTITIONS
      if remaining_partitions == 0:
        raise ValueError("partition_no must be > _RESERVED_PARTITIONS")
      partition = hash(reference_name) % remaining_partitions + \
                  _RESERVED_PARTITIONS
      # Save partition in _reference_name_to_partition dict for future look ups
      self._reference_name_to_parition_map[reference_name] = partition
    return partition

  def is_default_partition_absent(self):
    if self._config_file_given:
      return self._default_partition_index == -1
    else:
      # default partition does not exist, return -1.
      return False

  def get_default_partition_index(self):
    # If not _config_file_given default partition does not exist, return -1.
    if not self._config_file_given:
      return -1

    if self.is_default_partition_absent():
      return self._partition_no
    else:
      return self._default_partition_index

  def get_suffix(self, index):
    if index >= self._partition_no or index < 0:
      raise ValueError('given partition index is larger than the no of '
                       'partitions {} vs {}'.format(index, self._partition_no))
    return self._suffixes[index]
