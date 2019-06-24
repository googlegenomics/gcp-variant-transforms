# Copyright 2017 Google Inc.  All Rights Reserved.
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

"""A PTransform to output a PCollection of ``Variant`` records to BigQuery."""

from __future__ import absolute_import

from typing import Dict, List  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcf_header_io  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_row_generator
from gcp_variant_transforms.libs import bigquery_schema_descriptor
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy  # pylint: disable=unused-import


@beam.typehints.with_input_types(processed_variant.ProcessedVariant)
class ConvertVariantToRow(beam.DoFn):
  """Converts a ``Variant`` record to a BigQuery row."""

  def __init__(
      self,
      row_generator,  # type: bigquery_vcf_data_converter.BigQueryRowGenerator
      allow_incompatible_records=False,  # type: bool
      omit_empty_sample_calls=False  # type: bool
  ):
    # type: (...) -> None
    super(ConvertVariantToRow, self).__init__()
    self._allow_incompatible_records = allow_incompatible_records
    self._omit_empty_sample_calls = omit_empty_sample_calls
    self._bigquery_row_generator = row_generator

  def process(self, record):
    return self._bigquery_row_generator.get_rows(
        record, self._allow_incompatible_records, self._omit_empty_sample_calls)


@beam.typehints.with_input_types(processed_variant.ProcessedVariant)
class VariantToBigQuery(beam.PTransform):
  """Writes PCollection of `ProcessedVariant` records to BigQuery."""

  def __init__(
      self,
      output_table,  # type: str
      schema,  # type: bigquery.TableSchema
      append=False,  # type: bool
      update_schema_on_append=False,  # type: bool
      allow_incompatible_records=False,  # type: bool
      omit_empty_sample_calls=False,  # type: bool
      null_numeric_value_replacement=None  # type: int,

      ):
    # type: (...) -> None
    """Initializes the transform.

    Args:
      output_table: Full path of the output BigQuery table.
      schema: Schema of the table to be generated.
      append: If true, existing records in output_table will not be
        overwritten. New records will be appended to those that already exist.
      update_schema_on_append: If true, BigQuery schema will be updated by
        combining the existing schema and the new schema if they are compatible.
      allow_incompatible_records: If true, field values are casted to Bigquery
+       schema if there is a mismatch.
      omit_empty_sample_calls: If true, samples that don't have a given call
        will be omitted.
      null_numeric_value_replacement: the value to use instead of null for
        numeric (float/int/long) lists. For instance, [0, None, 1] will become
        [0, `null_numeric_value_replacement`, 1]. If not set, the value will set
        to bigquery_util._DEFAULT_NULL_NUMERIC_VALUE_REPLACEMENT.
    """
    self._output_table = output_table
    self._append = append
    self._schema = schema
    # Resolver makes extra effort to resolve conflict when flag
    # allow_incompatible_records is set.
    self._bigquery_row_generator = (
        bigquery_row_generator.VariantCallRowGenerator(
            bigquery_schema_descriptor.SchemaDescriptor(self._schema),
            vcf_field_conflict_resolver.FieldConflictResolver(
                resolve_always=allow_incompatible_records),
            null_numeric_value_replacement))

    self._allow_incompatible_records = allow_incompatible_records
    self._omit_empty_sample_calls = omit_empty_sample_calls
    if update_schema_on_append:
      bigquery_util.update_bigquery_schema_on_append(self._schema.fields,
                                                     self._output_table)

  def expand(self, pcoll):
    bq_rows = pcoll | 'ConvertToBigQueryTableRow' >> beam.ParDo(
        ConvertVariantToRow(
            self._bigquery_row_generator,
            self._allow_incompatible_records,
            self._omit_empty_sample_calls))
    return (bq_rows
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                self._output_table,
                schema=self._schema,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                    if self._append
                    else beam.io.BigQueryDisposition.WRITE_TRUNCATE),
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=self._temp_location))
