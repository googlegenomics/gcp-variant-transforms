# Copyright 2019 Google LLC.
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

"""A PTransform to prevent fusion of the surrounding transforms.

Read more:
  https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#fusion-optimization
"""

import apache_beam as beam


class FusionBreak(beam.PTransform):
  """PTransform that returns a PCollection equivalent to its input.

  It prevents fusion of the surrounding transforms.
  """
  def expand(self, pcoll):
    # Create an empty PCollection that depends on pcoll.
    empty = pcoll | beam.FlatMap(lambda x: ())
    return pcoll | beam.Map(lambda x, unused: x, beam.pvalue.AsIter(empty))
