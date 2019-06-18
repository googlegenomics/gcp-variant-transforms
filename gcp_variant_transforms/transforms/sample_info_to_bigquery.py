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

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcf_header_io  # pylint: disable=unused-import
from gcp_variant_transforms.beam_io import vcf_parser
from gcp_variant_transforms.libs import sample_info_table_schema_generator


class ConvertSampleInfoToRow(beam.DoFn):
  """Extracts sample info from vcf_header and converts it to a BigQuery row."""

  def __init__(self, file_path_to_file_hash):
    # type: (Dict[str, str]) -> None
    self._file_path_to_file_hash = file_path_to_file_hash

  def process(self, vcf_header):
    # type: (vcf_header_io.VcfHeader) -> Dict[str, Union[int, str]]
    for sample in vcf_header.samples:
      hash_code = vcf_parser.generate_int64_hash_code(
          self._file_path_to_file_hash.get(vcf_header.file_name), sample)
      row = {
          sample_info_table_schema_generator.SAMPLE_ID: hash_code,
          sample_info_table_schema_generator.FILE_PATH: vcf_header.file_name}
      yield row


class SampleInfoToBigQuery(beam.PTransform):
  """Writes sample info to BigQuery."""

  def __init__(self, output_table, file_path_to_file_hash, append=False):
    # type: (str, Dict[str, str], bool) -> None
    """Initializes the transform.

    Args:
      output_table: Full path of the output BigQuery table.
      file_path_to_file_hash: A map that maps file path to the corresponding
        hash that uniquely identifies a file.
    """

    self._output_table = output_table
    self._file_path_to_file_hash = file_path_to_file_hash
    self._append = append
    self._schema = sample_info_table_schema_generator.generate_schema()

  def expand(self, pcoll):
    return (pcoll
            | 'ConvertSampleInfoToBigQueryTableRow' >> beam.ParDo(
                ConvertSampleInfoToRow(self._file_path_to_file_hash))
            | 'WriteSampleInfoToBigQuery' >> beam.io.WriteToBigQuery(
                self._output_table,
                schema=self._schema,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                    if self._append
                    else beam.io.BigQueryDisposition.WRITE_TRUNCATE),
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS))
