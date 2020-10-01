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


import random
from typing import Dict, List  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcf_header_io  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_row_generator
from gcp_variant_transforms.libs import bigquery_schema_descriptor
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy  # pylint: disable=unused-import
from gcp_variant_transforms.transforms import limit_write


# TODO(samanvp): remove this hack when BQ custom sink is added to Python SDK,
# see: https://issues.apache.org/jira/browse/BEAM-2801
# This has to be less than 10000.
_WRITE_SHARDS_LIMIT = 1000


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
    super().__init__()
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
      allow_incompatible_records=False,  # type: bool
      omit_empty_sample_calls=False,  # type: bool
      num_bigquery_write_shards=1,  # type: int
      null_numeric_value_replacement=None,  # type: int
      include_call_name=False  # type: bool
      ):
    # type: (...) -> None
    """Initializes the transform.

    Args:
      output_table: Full path of the output BigQuery table.
      schema: Schema of the table to be generated.
      append: If true, existing records in output_table will not be
        overwritten. New records will be appended to those that already exist.
      allow_incompatible_records: If true, field values are casted to Bigquery
        schema if there is a mismatch.
      omit_empty_sample_calls: If true, samples that don't have a given call
        will be omitted.
      num_bigquery_write_shards: If > 1, we will limit number of sources which
        are used for writing to the output BigQuery table.
      null_numeric_value_replacement: the value to use instead of null for
        numeric (float/int/long) lists. For instance, [0, None, 1] will become
        [0, `null_numeric_value_replacement`, 1]. If not set, the value will set
        to bigquery_util._DEFAULT_NULL_NUMERIC_VALUE_REPLACEMENT.
      include_call_name: If true, sample name will be included in addition to
        sample ID.
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
            null_numeric_value_replacement,
            include_call_name))

    self._allow_incompatible_records = allow_incompatible_records
    self._omit_empty_sample_calls = omit_empty_sample_calls
    self._num_bigquery_write_shards = num_bigquery_write_shards

  def expand(self, pcoll):
    bq_rows = pcoll | 'ConvertToBigQueryTableRow' >> beam.ParDo(
        ConvertVariantToRow(
            self._bigquery_row_generator,
            self._allow_incompatible_records,
            self._omit_empty_sample_calls))
    if self._num_bigquery_write_shards > 1:
      # We split data into self._num_bigquery_write_shards random partitions
      # and then write each part to final BQ by appending them together.
      # Combined with LimitWrite transform, this will avoid the BQ failure.
      bq_row_partitions = bq_rows | beam.Partition(
          lambda _, n: random.randint(0, n - 1),
          self._num_bigquery_write_shards)
      bq_writes = []
      for i in range(self._num_bigquery_write_shards):
        bq_rows = (bq_row_partitions[i] | 'LimitWrite' + str(i) >>
                   limit_write.LimitWrite(_WRITE_SHARDS_LIMIT))
        bq_writes.append(
            bq_rows | 'WriteToBigQuery' + str(i) >>
            beam.io.Write(beam.io.BigQuerySink(
                self._output_table,
                schema=self._schema,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND))))
      return bq_writes
    else:
      return (bq_rows
              | 'WriteToBigQuery' >> beam.io.Write(beam.io.BigQuerySink(
                  self._output_table,
                  schema=self._schema,
                  create_disposition=(
                      beam.io.BigQueryDisposition.CREATE_NEVER),
                  write_disposition=(
                      beam.io.BigQueryDisposition.WRITE_APPEND
                      if self._append
                      else beam.io.BigQueryDisposition.WRITE_EMPTY))))
