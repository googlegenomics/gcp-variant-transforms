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

"""A PTransform for partitioning variants based on their reference_name."""

from __future__ import absolute_import

import apache_beam as beam
import mmh3
import re

# A reg exp that will match to names such as "chr01", "chr22", or simply "13"
_CHROMOSOME_NAME_REGEXP = re.compile(r'^(chr)?([0-9][0-9]?)$')
# Partition 0 to 21 is reserved for common reference_names such as "chr1".
_RESERVED_PARTITIONS = 22


class PartitionVariants(beam.PartitionFn):
  """Partitions variants based on their reference_name."""

  def __init__(self):
    self._ref_name_to_parition_map = {}

  def partition_for(self,
                    variant,  # type: Variant
                    num_partitions  # type: int
                    ):
    """A function that chooses an output partition for an variant.

    Given a variant and the # of partitions returns an index in the range of
    [0, num_partitions) for the variant (based on its reference_name).
    In order to make this look up less computation intensive we follow:
      1) Look up the reference_name in _ref_name_to_parition_map dict

    If the result of look up is None, we will call _ref_name_to_partition()
    and do the following steps:
      2) Try to match the ref_name to a reg exp of common names (eg 'chr12')
      3) Hash the reference_name and calculate its mod to remaining buckets
    result will be added to _ref_name_to_parition_map for future quick look ups.

    Args:
      variant: the variant to be partitioned
      num_partitions - the total number of partitions (> _RESERVED_PARTITIONS)
    Returns:
      An integer in the range of [0, num_partitions)
    Raises:
      ValueError: If num_partitions <= _RESERVED_PARTITIONS
    """
    ref_name = variant.reference_name.strip().lower()
    partition = self._ref_name_to_parition_map.get(ref_name, None)
    if partition is None:
      partition = self._ref_name_to_partition(ref_name, num_partitions)
      self._ref_name_to_parition_map[ref_name] = partition
    return partition

  def _ref_name_to_partition(self, ref_name, num_partitions):
    """Returns an index in [0, num_partitions) range for a given ref_name."""
    # First match the reference_name to the common formats.
    matched = _CHROMOSOME_NAME_REGEXP.match(ref_name)
    if matched:
      _, chr_no = matched.groups()
      chr_no = int(chr_no)
      if chr_no > 0 and chr_no <= _RESERVED_PARTITIONS:
        return chr_no - 1

    # If RegExp didn't work, we will find the hash of reference_name
    remaining_partitions = num_partitions - _RESERVED_PARTITIONS
    if remaining_partitions == 0:
      raise ValueError("paritition_no must be > _RESERVED_PARTITIONS")
    return (mmh3.hash(ref_name) % remaining_partitions) + _RESERVED_PARTITIONS
