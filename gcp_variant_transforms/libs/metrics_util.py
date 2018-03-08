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

"""Wrapper for Metrics API of Beam.

This is used to avoid direct dependency on Beam in our library methods/classes
such that they are more modular and easier to test. Preferably instances
of classes in this module should be injected into library classes/methods
instead of direct instantiation.

This is an example of how to use the factory and counter objects:

```
factory = CounterFactory()
my_counter = factory.create_counter('my_counter_name')
my_counter.inc(4)
```

"""

from __future__ import absolute_import

import logging

from apache_beam.runners import runner  # pylint: disable=unused-import
from apache_beam import metrics
from apache_beam.metrics import metric

# The name space in which all metrics created by this class are.
_METRICS_NAMESPACE = 'VT_metrics_namespace'

# The name of the entry for counters in the dictionary that metrics().query() of
# Beam returns.
_COUNTERS = 'counters'


class CounterInterface(object):
  """The interface of counter objects"""

  def inc(self, n=1):
    # type: (int) -> None
    """Subclass implementations should do increment by `n`."""
    raise NotImplementedError


class _NoOpCounter(CounterInterface):
  """A counter that does nothing, good to be used when counter is optional."""

  def inc(self, n=1):
    # type: (int) -> None
    pass


class _CounterWrapper(CounterInterface):
  """A wrapper for Beam counters."""

  def __init__(self, counter_name):
    # type: (str) -> None
    self._counter_name = counter_name
    self._counter = metrics.Metrics.counter(_METRICS_NAMESPACE, counter_name)

  def inc(self, n=1):
    # type: (int) -> None
    """Increments the counter by `n`"""
    self._counter.inc(n)


class CounterFactoryInterface(object):
  """The interface for counter factories."""

  def create_counter(self, counter_name):
    # type: (str) -> CounterInterface
    """Returns a counter with the given name."""
    raise NotImplementedError


class NoOpCounterFactory(CounterFactoryInterface):
  """A factory that creates counters that do nothing."""

  def create_counter(self, counter_name):
    # type: (str) -> CounterInterface
    return _NoOpCounter()


class CounterFactory(CounterFactoryInterface):

  def create_counter(self, counter_name):
    # type: (str) -> CounterInterface
    return _CounterWrapper(counter_name)


def log_all_counters(pipeline_result):
  """Logs all counters that belong to _METRICS_NAME_SPACE."""
  counter_filter = metric.MetricsFilter().with_namespace(_METRICS_NAMESPACE)
  query_result = pipeline_result.metrics().query(counter_filter)
  if query_result[_COUNTERS]:
    for counter in query_result[_COUNTERS]:
      logging.info('Counter %s = %d', counter, counter.committed)
