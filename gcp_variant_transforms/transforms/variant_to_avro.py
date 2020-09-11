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


import apache_beam as beam
import fastavro

from gcp_variant_transforms.beam_io import vcf_header_io  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_row_generator
from gcp_variant_transforms.libs import bigquery_schema_descriptor
from gcp_variant_transforms.libs import schema_converter
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy  # pylint: disable=unused-import
from gcp_variant_transforms.transforms import variant_to_bigquery


# TODO(bashir2): Refactor common parts of VariantToAvroFiles and
# VariantToBigQuery into a class that is shared by both. It is mostly the
# schema generation that is different (and of course the sink). There is also
# some logic for updating schema etc. that is not needed for the Avro case.
@beam.typehints.with_input_types(processed_variant.ProcessedVariant)
class VariantToAvroFiles(beam.PTransform):
  """Writes PCollection of `ProcessedVariant` records to Avro files."""

  def __init__(
      self,
      output_path,  # type: str
      schema,  # type: bigquery.TableSchema
      allow_incompatible_records=False,  # type: bool
      omit_empty_sample_calls=False,  # type: bool
      null_numeric_value_replacement=None  # type: int
      ):
    # type: (...) -> None
    """Initializes the transform.

    Args:
      output_path: The path under which output Avro files are generated.
      schema: Schema of the table to be generated.
      allow_incompatible_records: If true, field values are casted to Bigquery
+       schema if there is a mismatch.
      omit_empty_sample_calls: If true, samples that don't have a given call
        will be omitted.
      null_numeric_value_replacement: the value to use instead of null for
        numeric (float/int/long) lists. For instance, [0, None, 1] will become
        [0, `null_numeric_value_replacement`, 1]. If not set, the value will set
        to bigquery_util._DEFAULT_NULL_NUMERIC_VALUE_REPLACEMENT.
    """
    self._output_path = output_path
    self._fastavro_schema = fastavro.parse_schema(
        schema_converter.convert_schema_to_avro_dict(schema))
    self._bigquery_row_generator = (
        bigquery_row_generator.VariantCallRowGenerator(
            bigquery_schema_descriptor.SchemaDescriptor(schema),
            vcf_field_conflict_resolver.FieldConflictResolver(
                resolve_always=allow_incompatible_records),
            null_numeric_value_replacement))

    self._allow_incompatible_records = allow_incompatible_records
    self._omit_empty_sample_calls = omit_empty_sample_calls

  def expand(self, pcoll):
    avro_records = pcoll | 'ConvertToAvroRecords' >> beam.ParDo(
        variant_to_bigquery.ConvertVariantToRow(
            self._bigquery_row_generator,
            self._allow_incompatible_records,
            self._omit_empty_sample_calls))
    return (avro_records
            | 'WriteToAvroFiles' >>
            beam.io.WriteToAvro(self._output_path, self._fastavro_schema))
