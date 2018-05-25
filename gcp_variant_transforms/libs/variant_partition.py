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
from mmh3 import hash  # pylint: disable=no-name-in-module,redefined-builtin


# A reg exp that will match to standard reference_names such as "chr01" or "13".
_CHROMOSOME_NAME_REGEXP = re.compile(r'^(chr)?([0-9][0-9]?)$')
# Partition 0 to 21 is reserved for common reference_names such as "chr1".
_RESERVED_AUTO_PARTITIONS = 22
# We try to assign each chromosome to a partition. The first 22 partitions
# [0, 22) are reserved for standard reference_names. Every other identifiers
# will be matched to next available partitions [22, 27).
_DEFAULT_NUM_PARTITIONS = _RESERVED_AUTO_PARTITIONS + 5


class VariantPartition(object):
  """Automatically partition variants based on their reference_name."""

  def __init__(self):
    if _DEFAULT_NUM_PARTITIONS <= _RESERVED_AUTO_PARTITIONS:
      raise ValueError(
          '_DEFAULT_NUM_PARTITIONS must be > _RESERVED_AUTO_PARTITIONS')
    self._reference_name_to_partition_map = {}

  def get_num_partitions(self):
    """Returns the number of partitions."""
    return _DEFAULT_NUM_PARTITIONS

  def get_partition(self, reference_name, pos=0):
    # type: (str, int) -> int
    reference_name = reference_name.strip().lower()
    if not reference_name or pos < 0:
      raise ValueError('Cannot partition given input {}:{}'
                       .format(reference_name, pos))
    return self._get_auto_partition(reference_name)

  def _get_auto_partition(self, reference_name):
    # type: (str) -> int
    """Automatically chooses an partition for the given reference_name.

    Given a reference_name returns an index in [0, _DEFAULT_NUM_PARTITIONS)
    range. In order to make this lookup less computationally intensive we first:
      1) Lookup the reference_name in _reference_name_to_partition_map dict

    If the result of lookup is None, we will try the following steps:
      2) Match the reference_name to a reg exp of common names (eg 'chr12')
      3) Hash the reference_name and calculate its mod to remaining buckets
    result will be added to _reference_name_to_partition_map for future lookup.

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
