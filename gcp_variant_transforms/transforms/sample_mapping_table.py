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


class SampleIdToNameDict(beam.PTransform):
  """Transforms BigQuery table rows to PCollection of `Variant`."""

  def _convert_bq_row(self, row):
    sample_id = row[SAMPLE_ID_COLUMN]
    sample_name = row[SAMPLE_NAME_COLUMN]
    file_path = row[FILE_PATH_COLUMN]
    return (sample_id, (sample_name, file_path))

  def expand(self, pcoll):
    return (pcoll
            | 'BigQueryToMapping' >> beam.Map(self._convert_bq_row)
            | 'CombineToDict' >> beam.combiners.ToDict())


class SampleNameToIdDict(beam.PTransform):
  """Transforms BigQuery table rows to PCollection of `Variant`."""

  def _convert_bq_row(self, row):
    sample_id = row[SAMPLE_ID_COLUMN]
    sample_name = row[SAMPLE_NAME_COLUMN]
    return (sample_name, sample_id)

  def expand(self, pcoll):
    return (pcoll
            | 'BigQueryToMapping' >> beam.Map(self._convert_bq_row)
            | 'CombineToDict' >> beam.combiners.ToDict())

class GetSampleNames(beam.PTransform):
  """Transforms sample_ids to sample_names"""

  def __init__(self, hash_table):
    # type: (Dict[int, Tuple(str, str)]) -> None
    self._hash_table = hash_table

  def _get_encoding_type(self, hash_table):
    # type: (Dict[int, Tuple(str, str)]) -> bool
    sample_names = [c[0] for c in hash_table.values()]
    return len(sample_names) == len(set(sample_names))

  def _get_sample_id(self, sample_id, hash_table):
    # type: (int, Dict[int, Tuple(str, str)]) -> str
    sample = hash_table[sample_id]
    if self._get_encoding_type(hash_table):
      return sample[0]
    else:
      return WITH_FILE_SAMPLE_TEMPLATE.format(SAMPLE_NAME=sample[0],
                                              FILE_PATH=sample[1])

  def expand(self, pcoll):
    return pcoll | beam.Map(self._get_sample_id, self._hash_table)

class GetSampleIds(beam.PTransform):
  """Transform sample_names to sample_ids"""

  def __init__(self, hash_table):
    # type: (Dict[str, int)]) -> None
    self._hash_table = hash_table

  def _get_sample_name(self, sample_name, hash_table):
    # type: (str, Dict[str, int]) -> int
    return hash_table[sample_name]

  def expand(self, pcoll):
    return pcoll | beam.Map(self._get_sample_name, self._hash_table)
