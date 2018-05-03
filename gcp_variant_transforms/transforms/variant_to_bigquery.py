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

import random
import apache_beam as beam

from gcp_variant_transforms.beam_io import vcf_header_io  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_row_generator
from gcp_variant_transforms.libs import bigquery_schema_descriptor  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_vcf_schema
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_field_conflict_resolver
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy  # pylint: disable=unused-import
from gcp_variant_transforms.transforms import limit_write


# TODO(samanvp): remove this hack when BQ custom sink is added to Python SDK,
# see: https://issues.apache.org/jira/browse/BEAM-2801
# This has to be less than 10000.
_WRITE_SHARDS_LIMIT = 1000
_NUM_BQ_LIMITED_WRITE_PARTITIONS = 20


@beam.typehints.with_input_types(processed_variant.ProcessedVariant)
class _ConvertToBigQueryTableRow(beam.DoFn):
  """Converts a ``Variant`` record to a BigQuery row."""

  def __init__(self,
               row_generator,
               allow_incompatible_records=False,
               omit_empty_sample_calls=False):
    # type: (bigquery_row_generator.BigQueryRowGenerator, bool, bool) -> None
    super(_ConvertToBigQueryTableRow, self).__init__()
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
      header_fields,  # type: vcf_header_io.VcfHeader
      variant_merger=None,  # type: variant_merge_strategy.VariantMergeStrategy
      proc_var_factory=None,  # type: processed_variant.ProcessedVariantFactory
      append=False,  # type: bool
      allow_incompatible_records=False,  # type: bool
      omit_empty_sample_calls=False,  # type: bool
      limited_write=False  # type: bool
      ):
    """Initializes the transform.

    Args:
      output_table: Full path of the output BigQuery table.
      header_fields: Representative header fields for all variants. This is
        needed for dynamically generating the schema.
      variant_merger: The strategy used for merging variants (if any). Some
        strategies may change the schema, which is why this may be needed here.
      proc_var_factory: The factory class that knows how to convert Variant
        instances to ProcessedVariant. As a side effect it also knows how to
        modify BigQuery schema based on the ProcessedVariants that it generates.
        The latter functionality is what is needed here.
      append: If true, existing records in output_table will not be
        overwritten. New records will be appended to those that already exist.
      allow_incompatible_records: If true, field values are casted to Bigquery
+        schema if there is a mismatch.
      omit_empty_sample_calls: If true, samples that don't have a given call
        will be omitted.
      limited_write: If true, we will limit number of sources which are used
        for writing to the output BigQuery table.
    """
    self._output_table = output_table
    self._header_fields = header_fields
    self._variant_merger = variant_merger
    self._proc_var_factory = proc_var_factory
    self._append = append
    self._schema = bigquery_vcf_schema.generate_schema_from_header_fields(
        self._header_fields, self._proc_var_factory, self._variant_merger)
    # Resolver makes extra effort to resolve conflict when flag
    # allow_incompatible_records is set.
    self._bigquery_row_generator = bigquery_row_generator.BigQueryRowGenerator(
        bigquery_schema_descriptor.SchemaDescriptor(self._schema),
        vcf_field_conflict_resolver.FieldConflictResolver(
            resolve_always=allow_incompatible_records))

    self._allow_incompatible_records = allow_incompatible_records
    self._omit_empty_sample_calls = omit_empty_sample_calls
    self._limited_write = limited_write

  def expand(self, pcoll):
    bq_rows = pcoll | 'ConvertToBigQueryTableRow' >> beam.ParDo(
        _ConvertToBigQueryTableRow(
            self._bigquery_row_generator,
            self._allow_incompatible_records,
            self._omit_empty_sample_calls))
    if self._limited_write:
      # We split data into _NUM_BQ_LIMITED_WRITE_PARTITIONS random partitions
      # and then write each part to final BQ by appending them together.
      # Combined with LimitWrite transform, this will avoid the BQ failure.
      bq_row_partitions = bq_rows | beam.Partition(
          lambda _, n: random.randint(0, n - 1),
          _NUM_BQ_LIMITED_WRITE_PARTITIONS)
      bq_writes = []
      for i in range(_NUM_BQ_LIMITED_WRITE_PARTITIONS):
        bq_rows = (bq_row_partitions[i] | 'LimitWrite' + str(i) >>
                   limit_write.LimitWrite(_WRITE_SHARDS_LIMIT))
        bq_writes.append(
            bq_rows | 'WriteToBigQuery' + str(i) >>
            beam.io.Write(beam.io.BigQuerySink(
                self._output_table,
                schema=self._schema,
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND))))
      return bq_writes
    else:
      return (bq_rows
              | 'WriteToBigQuery' >> beam.io.Write(beam.io.BigQuerySink(
                  self._output_table,
                  schema=self._schema,
                  create_disposition=(
                      beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                  write_disposition=(
                      beam.io.BigQueryDisposition.WRITE_APPEND
                      if self._append
                      else beam.io.BigQueryDisposition.WRITE_TRUNCATE))))
