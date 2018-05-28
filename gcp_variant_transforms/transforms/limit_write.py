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

"""A PTransform to limit BQ sink from producing too many files (shards)

This is a work around to avoid the following failure:
    BigQuery execution failed., Error:
    Message: Too many sources provided: xxxxx. Limit is 10000.
To limit sink we generate a random dummy key and group by input elements (which
are BigQuery rows) based on that key before writing them to output table.
"""

from __future__ import absolute_import

import random
import apache_beam as beam


class _RoundRobinKeyFn(beam.DoFn):
  def __init__(self, count):
    # type: (int) -> None
    self._count = count
    # This attribute will be properly initiated at each worker by start_bundle()
    self._counter = 0

  def start_bundle(self):
    # type: () -> None
    self._counter = random.randint(0, self._count - 1)

  def process(self, element):
    self._counter += 1
    if self._counter >= self._count:
      self._counter -= self._count
    yield self._counter, element


class LimitWrite(beam.PTransform):
  def __init__(self, count):
    # type: (int) -> None
    self._count = count

  def expand(self, pcoll):
    return (pcoll
            | beam.ParDo(_RoundRobinKeyFn(self._count))
            | beam.GroupByKey()
            | beam.FlatMap(lambda kv: kv[1]))
