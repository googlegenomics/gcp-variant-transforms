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

"""A PTransform for sharding variants based on their reference_name."""

from __future__ import absolute_import

import apache_beam as beam
from gcp_variant_transforms.beam_io import vcfio # pylint: disable=unused-import
from gcp_variant_transforms.libs import variant_sharding # pylint: disable=unused-import


class ShardVariants(beam.PartitionFn):
  """Shards variants based on their reference_name."""

  def __init__(self, shard):
    # type: (variant_sharding.VariantSharding) -> None
    self._shard = shard

  def partition_for(self, variant, _):
    # type: (vcfio.Variant, int) -> int
    return self._shard.get_shard_index(variant.reference_name.strip(),
                                       variant.start)

