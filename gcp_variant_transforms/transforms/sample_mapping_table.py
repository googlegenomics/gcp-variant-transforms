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

from gcp_variant_transforms.libs import sample_info_table_schema_generator

SAMPLE_ID_COLUMN = sample_info_table_schema_generator.SAMPLE_ID
SAMPLE_NAME_COLUMN = sample_info_table_schema_generator.SAMPLE_NAME


class SampleIdToNameDict(beam.PTransform):
  """Generate Id-to-Name hashing table from sample info table."""

  def _extract_id_name(self, row):
    sample_id = row[SAMPLE_ID_COLUMN]
    sample_name = row[SAMPLE_NAME_COLUMN]
    return (sample_id, sample_name)

  def expand(self, pcoll):
    return (pcoll
            | 'ExtractIdNameTuples' >> beam.Map(self._extract_id_name)
            | 'CombineToDict' >> beam.combiners.ToDict())


class SampleNameToIdDict(beam.PTransform):
  """Generate Name-to-ID hashing table from sample info table."""

  def _extract_id_name(self, row):
    sample_id = row[SAMPLE_ID_COLUMN]
    sample_name = row[SAMPLE_NAME_COLUMN]
    return (sample_name, sample_id)

  def expand(self, pcoll):
    return (pcoll
            | 'ExtractNameIdTuples' >> beam.Map(self._extract_id_name)
            | 'CombineToDict' >> beam.combiners.ToDict())

class GetSampleNames(beam.PTransform):
  """Looks up sample_names corresponding to the given sample_ids"""

  def __init__(self, id_to_name_dict):
    # type: (Dict[int, str]) -> None
    self._id_to_name_dict = id_to_name_dict

  def _get_sample_name(self, sample_id, id_to_name_dict):
    # type: (int, Dict[int, str]) -> str
    if sample_id in id_to_name_dict:
      return id_to_name_dict[sample_id]
    raise ValueError('Sample ID `{}` was not found.'.format(sample_id))

  def expand(self, pcoll):
    return (pcoll
            | 'Generate Name to ID Mapping'
            >> beam.Map(self._get_sample_name, self._id_to_name_dict))

class GetSampleIds(beam.PTransform):
  """Looks up sample_ids corresponding to the given sample_names"""

  def __init__(self, name_to_id_dict):
    # type: (Dict[str, int)]) -> None
    self._name_to_id_dict = name_to_id_dict

  def _get_sample_id(self, sample_name, name_to_id_dict):
    # type: (str, Dict[str, int]) -> int
    if sample_name in name_to_id_dict:
      return name_to_id_dict[sample_name]
    raise ValueError('Sample `{}` was not found.'.format(sample_name))

  def expand(self, pcoll):
    return (pcoll
            | 'Generate Name to ID Mapping'
            >> beam.Map(self._get_sample_id, self._name_to_id_dict))
