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

import apache_beam as beam

from gcp_variant_transforms.libs import bigquery_vcf_schema
from gcp_variant_transforms.libs import processed_variant
from gcp_variant_transforms.libs import vcf_header_parser  #pylint: disable=unused-import
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy  #pylint: disable=unused-import

__all__ = ['VariantToBigQuery']


@beam.typehints.with_input_types(processed_variant.ProcessedVariant)
class _ConvertToBigQueryTableRow(beam.DoFn):
  """Converts a ``Variant`` record to a BigQuery row."""

  def __init__(self, omit_empty_sample_calls=False):
    super(_ConvertToBigQueryTableRow, self).__init__()
    self._omit_empty_sample_calls = omit_empty_sample_calls

  def process(self, record):
    return bigquery_vcf_schema.get_rows_from_variant(
        record, self._omit_empty_sample_calls)


@beam.typehints.with_input_types(processed_variant.ProcessedVariant)
class VariantToBigQuery(beam.PTransform):
  """Writes PCollection of `ProcessedVariant` records to BigQuery."""

  def __init__(
      self,
      output_table,  # type: str
      header_fields,  # type: vcf_header_parser.HeaderFields
      variant_merger=None,  # type: variant_merge_strategy.VariantMergeStrategy
      proc_var_factory=None,  # type: processed_variant.ProcessedVariantFactory
      append=False,  # type: bool
      omit_empty_sample_calls=False  # type: bool
  ):
    """Initializes the transform.

    Args:
      output_table: Full path of the output BigQuery table.
      header_fields: A `namedtuple` containing representative header fields for
        all variants. This is needed for dynamically generating the schema.
      variant_merger: The strategy used for merging variants (if any). Some
        strategies may change the schema, which is why this may be needed here.
      proc_var_factory: The factory class that knows how to convert Variant
        instances to ProcessedVariant. As a side effect it also knows how to
        modify BigQuery schema based on the ProcessedVariants that it generates.
        The latter functionality is what is needed here.
      append: If true, existing records in output_table will not be
        overwritten. New records will be appended to those that already exist.
      omit_empty_sample_calls: If true, samples that don't have a given call
        will be omitted.
    """
    self._output_table = output_table
    self._header_fields = header_fields
    self._variant_merger = variant_merger
    self._proc_var_factory = proc_var_factory
    self._append = append
    self._omit_empty_sample_calls = omit_empty_sample_calls

  def expand(self, pcoll):
    return (pcoll
            | 'ConvertToBigQueryTableRow' >> beam.ParDo(
                _ConvertToBigQueryTableRow(self._omit_empty_sample_calls))
            | 'WriteToBigQuery' >> beam.io.Write(beam.io.BigQuerySink(
                self._output_table,
                schema=bigquery_vcf_schema.generate_schema_from_header_fields(
                    self._header_fields,
                    self._proc_var_factory,
                    self._variant_merger),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                    if self._append
                    else beam.io.BigQueryDisposition.WRITE_TRUNCATE))))
