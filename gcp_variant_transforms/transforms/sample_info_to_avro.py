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

from typing import Dict, Union  # pylint: disable=unused-import

import time

import apache_beam as beam
import avro

from gcp_variant_transforms.beam_io import vcf_header_io  # pylint: disable=unused-import
from gcp_variant_transforms.beam_io import vcf_parser
from gcp_variant_transforms.libs import hashing_util
from gcp_variant_transforms.libs import sample_info_table_schema_generator
from gcp_variant_transforms.libs import schema_converter

SampleNameEncoding = vcf_parser.SampleNameEncoding
_SECS_IN_MIN = 60
_MICROS_IN_SEC = 1000000


class ConvertSampleInfoToRow(beam.DoFn):
  """Extracts sample info from `VcfHeader` and converts it to a BigQuery row."""

  def __init__(self, sample_name_encoding):
    # type: (int) -> None
    self._sample_name_encoding = sample_name_encoding

  def _get_now_to_minute(self):
    return int(time.time()) / _SECS_IN_MIN * _SECS_IN_MIN * _MICROS_IN_SEC

  def process(self, vcf_header):
    # type: (vcf_header_io.VcfHeader, bool) -> Dict[str, Union[int, str]]
    current_minute = self._get_now_to_minute()
    for sample in vcf_header.samples:
      if self._sample_name_encoding == SampleNameEncoding.WITH_FILE_PATH:
        sample = hashing_util.create_composite_sample_name(sample,
                                                           vcf_header.file_path)
      sample_id = hashing_util.generate_sample_id(sample)

      row = {
          sample_info_table_schema_generator.SAMPLE_ID: sample_id,
          sample_info_table_schema_generator.SAMPLE_NAME: sample,
          sample_info_table_schema_generator.FILE_PATH: vcf_header.file_path,
          sample_info_table_schema_generator.INGESTION_DATETIME: current_minute
      }
      yield row


class SampleInfoToAvro(beam.PTransform):
  """Writes sample info to BigQuery."""

  def __init__(self, output_path, sample_name_encoding):
    # type: (str, Dict[str, str], bool, int) -> None
    """Initializes the transform.

    Args:
      output_path: The output path of the sample file in the avro directory.
      sample_name_encoding: If SampleNameEncoding.WITHOUT_FILE_PATH is supplied,
        sample_id would only use sample_name in to get a hashed name; otherwise
        both sample_name and file_name will be used.
    """
    self._output_path = output_path
    self._sample_name_encoding = sample_name_encoding
    bq_schema = sample_info_table_schema_generator.generate_schema()
    self._avro_schema = avro.schema.parse(
        schema_converter.convert_table_schema_to_json_avro_schema(bq_schema))

  def expand(self, pcoll):
    return (pcoll
            | 'ConvertSampleInfoToAvroTableRow' >> beam.ParDo(
                ConvertSampleInfoToRow(self._sample_name_encoding))
            | 'WriteToAvroFiles' >> beam.io.WriteToAvro(
                self._output_path, self._avro_schema))
