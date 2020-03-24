# Copyright 2020 Google LLC.  All Rights Reserved.
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

"""A PTransform to convert BigQuery table rows to a PCollection of `Variant`."""

from typing import Dict, List  # pylint: disable=unused-import

import apache_beam as beam

SAMPLE_ID_COLUMN = 'sample_id'
SAMPLE_NAME_COLUMN = 'sample_name'
FILE_PATH_COLUMN = 'file_path'
WITH_FILE_SAMPLE_TEMPLATE = "{FILE_PATH}/{SAMPLE_NAME}"
SAMPLE_NAME_TEMPLATE = "{SAMPLE_NAME}_{IND}"


class SampleIdToNameDict(beam.PTransform):
  """Transforms BigQuery table rows to PCollection of `Variant`."""

  def _convert_bq_row(self, row):
    sample_id = row[SAMPLE_ID_COLUMN]
    sample_name = row[SAMPLE_NAME_COLUMN]
    file_path = row[FILE_PATH_COLUMN]
    return (sample_id, (sample_name, file_path))

  def _process_hash_table(self, hash_table):
    sample_names = []
    file_ind = {}
    ind = 0
    for value in hash_table.values():
      sample_names.append(value[0])
      if value[1] not in file_ind:
        ind += 1
        file_ind[value[1]] = ind

    parsed_dict = {}
    if len(sample_names) == len(set(sample_names)):
      for k, v in hash_table.items():
        parsed_dict[k] = v[0]
    else:
      for k, v in hash_table.items():
        parsed_dict[k] = SAMPLE_NAME_TEMPLATE.format(SAMPLE_NAME=v[0],
                                                     IND=file_ind[v[1]])
    return parsed_dict

  def expand(self, pcoll):
    return (pcoll
            | 'BigQueryToMapping' >> beam.Map(self._convert_bq_row)
            | 'CombineToDict' >> beam.combiners.ToDict()
            | 'ProcessDict' >> beam.Map(self._process_hash_table))

class GetSampleNames(beam.PTransform):
  """Transforms sample_ids to sample_names"""

  def __init__(self, hash_table):
    # type: (Dict[int, Tuple(str, str)]) -> None
    self._hash_table = hash_table

  def _get_sample_id(self, sample_id, hash_table):
    # type: (int, Dict[int, Tuple(str, str)]) -> str
    return hash_table[sample_id]

  def expand(self, pcoll):
    return pcoll | beam.Map(self._get_sample_id, self._hash_table)

class ToDictAccumulateCombineFn(beam.CombineFn):
  """CombineFn to create dictionary, but appending values for same keys."""

  def create_accumulator(self):
    return dict()

  def add_input(self, accumulator, element):
    key, value = element
    if key in accumulator:
      accumulator[key].append(value)
    else:
      accumulator[key] = [value]
    return accumulator

  def merge_accumulators(self, accumulators):
    result = dict()
    for a in accumulators:
      for k, v in a.items():
        if k in result:
          result[k].extend(v)
        else:
          result[k] = v
    return result

  def extract_output(self, accumulator):
    return accumulator

class SampleNameToIdDict(beam.PTransform):
  """Transforms BigQuery table rows to PCollection of `Variant`."""

  def _convert_bq_row(self, row):
    sample_id = row[SAMPLE_ID_COLUMN]
    sample_name = row[SAMPLE_NAME_COLUMN]
    return (sample_name, sample_id)

  def expand(self, pcoll):
    return (pcoll
            | 'BigQueryToMapping' >> beam.Map(self._convert_bq_row)
            | 'CombineToDict' >> beam.CombineGlobally(
                ToDictAccumulateCombineFn()))

class GetSampleIds(beam.PTransform):
  """Transform sample_names to sample_ids"""

  def __init__(self, hash_table):
    # type: (Dict[str, int)]) -> None
    self._hash_table = hash_table

  def _get_sample_name(self, sample_name, hash_table):
    # type: (str, Dict[str, int]) -> int
    return list(set(hash_table[sample_name]))

  def print_row(self, row):
    return row

  def expand(self, pcoll):
    return pcoll | beam.FlatMap(self._get_sample_name, self._hash_table)
