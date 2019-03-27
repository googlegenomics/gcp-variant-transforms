# Copyright 2019 Google Inc.  All Rights Reserved.
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

"""Util class used to optimize default values for flags, based on provided
input size.

If any of the flags were manually supplied during the command's invocation,
they will not be overriden.

The class uses 5 signals extracted from input, for flag adjustment:
 - estimated total number of variants.
 - estimated total number of samples.
 - estimated number of records (variant data for sample).
 - total size of the input.
 - amount of supplied files.
"""

import operator

from apache_beam.runners import runner  # pylint: disable=unused-import


class Dimensions(object):
  """Contains dimensions of the input data and the manually supplied args."""
  def __init__(self,
               line_count=None, # type: int
               sample_count=None, # type: int
               record_count=None, # type: int
               files_size=None, # type: int
               file_count=None, # type: int
               supplied_args=None # type: List[str]
              ):
    # type(...) -> None
    self.line_count = line_count
    self.sample_count = sample_count
    self.record_count = record_count
    self.files_size = files_size
    self.file_count = file_count
    self.supplied_args = supplied_args


class Threshold(Dimensions):
  """Describes the limits the input needs to pass to enable a certain flag.

    Unlike Dimensions object, should not have supplied_args set and not all
    dimensions need to be defined.
  """
  def __init__(self,
               flag_name, # type: str
               line_count=None, # type: int
               sample_count=None, # type: int
               record_count=None, # type: int
               files_size=None, # type: int
               file_count=None # type: int
              ):
    super(Threshold, self).__init__(line_count,
                                    sample_count,
                                    record_count,
                                    files_size,
                                    file_count)
    self.flag_name = flag_name

  def not_supplied(self, state):
    # type(Dimensions) -> bool
    """Verify that flag was not manually supplied."""
    return self.flag_name not in state.supplied_args

  def hard_pass(self, state, cond=operator.gt):
    # type(Dimensions, Callable) -> bool
    """Verifies that all of set dimensions of the threshold are satisfied."""
    return self.not_supplied(state) and (
        (not self.line_count or cond(state.line_count, self.line_count)) and
        (not self.sample_count or
         cond(state.sample_count, self.sample_count)) and
        (not self.record_count or
         cond(state.record_count, self.record_count)) and
        (not self.files_size or cond(state.files_size, self.files_size)) and
        (not self.file_count or cond(state.file_count, self.file_count)))

  def soft_pass(self, state, cond=operator.gt):
    # type(Dimensions, Callable) -> bool
    """Verifies that at least one of the set dimensions is satisfied."""
    return self.not_supplied(state) and (
        (self.line_count and cond(state.line_count, self.line_count)) or
        (self.sample_count and cond(state.sample_count, self.sample_count)) or
        (self.record_count and cond(state.record_count, self.record_count)) or
        (self.files_size and cond(state.files_size, self.files_size)) or
        (self.file_count and cond(state.file_count, self.file_count)))


OPTIMIZE_FOR_LARGE_INPUTS_TS = Threshold(
    'optimize_for_large_inputs',
    record_count=3000000000,
    file_count=50000)
INFER_HEADERS_TS = Threshold(
    'infer_headers',
    record_count=5000000000
)
INFER_ANNOTATION_TYPES_TS = Threshold(
    'infer_annotation_types',
    record_count=5000000000
)
NUM_BIGQUERY_WRITE_SHARDS_TS = Threshold(
    'num_bigquery_write_shards',
    record_count=1000000000,
    files_size=500000000000
)
NUM_WORKERS_TS = Threshold(
    'num_workers',
    record_count=1000000000
)
SHARD_VARIANTS_TS = Threshold(
    'shard_variants',
    record_count=1000000000,
)

def _optimize_known_args(known_args, input_dimensions):
  if OPTIMIZE_FOR_LARGE_INPUTS_TS.soft_pass(input_dimensions):
    known_args.optimize_for_large_inputs = True
  if INFER_HEADERS_TS.soft_pass(input_dimensions, operator.le):
    known_args.infer_headers = True
  if NUM_BIGQUERY_WRITE_SHARDS_TS.soft_pass(input_dimensions):
    known_args.num_bigquery_write_shards = 20
  if INFER_ANNOTATION_TYPES_TS.soft_pass(input_dimensions, operator.le):
    known_args.infer_annotation_types = True
  if SHARD_VARIANTS_TS.soft_pass(input_dimensions, operator.le):
    known_args.shard_variants = False

def _optimize_pipeline_args(pipeline_args, known_args, input_dimensions):
  if NUM_WORKERS_TS.hard_pass(input_dimensions):
    pipeline_args.num_workers = 100
  if (known_args.run_annotation_pipeline and
      NUM_WORKERS_TS.not_supplied(input_dimensions)):
    pipeline_args.num_workers = 400

def optimize_flags(supplied_args, known_args, pipeline_args):
  # type(Namespace, List[str]) -> None
  input_dimensions = Dimensions(line_count=known_args.estimated_line_count,
                                sample_count=known_args.estimated_sample_count,
                                record_count=known_args.estimated_record_count,
                                files_size=known_args.files_size,
                                file_count=known_args.file_count,
                                supplied_args=supplied_args)

  _optimize_known_args(known_args, input_dimensions)
  _optimize_pipeline_args(pipeline_args, known_args, input_dimensions)
