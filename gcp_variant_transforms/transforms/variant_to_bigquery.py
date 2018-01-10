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

__all__ = ['VariantToBigQuery']


class _ConvertToBigQueryTableRow(beam.DoFn):
  """Converts a ``Variant`` record to a BigQuery row."""

  def __init__(self, split_alternate_allele_info_fields=True):
    super(_ConvertToBigQueryTableRow, self).__init__()
    self._split_alternate_allele_info_fields = (
        split_alternate_allele_info_fields)

  def process(self, record, *args, **kwargs):
    return bigquery_vcf_schema.get_rows_from_variant(
        record, self._split_alternate_allele_info_fields)


class VariantToBigQuery(beam.PTransform):
  """Writes PCollection of ``Variant`` records to BigQuery."""

  def __init__(self, output_table, header_fields, variant_merger=None,
               split_alternate_allele_info_fields=True, append=False):
    """Initializes the transform.

    Args:
      output_table (str): Full path of the output BigQuery table.
      header_fields (``HeaderFields``): A ``namedtuple`` containing
        representative header fields for all ``Variant`` records.
        This is needed for dynamically generating the schema.
      variant_merger (``VariantMergeStrategy``): The strategy used for merging
        variants (if any). Some strategies may change the schema, which is why
        this may be needed here.
      split_alternate_allele_info_fields (bool): If true, all INFO fields with
        `Number=A` (i.e. one value for each alternate allele) will be stored
        under the `alternate_bases` record. If false, they will be stored with
        the rest of the INFO fields.
      append (bool): If true, existing records in output_table will not be
        overwritten. New records will be appended to those that already exist.
    """
    self._output_table = output_table
    self._header_fields = header_fields
    self._variant_merger = variant_merger
    self._split_alternate_allele_info_fields = (
        split_alternate_allele_info_fields)
    self._append = append

  def expand(self, pcoll):
    return (pcoll
            | 'ConvertToBigQueryTableRow' >> beam.ParDo(
                _ConvertToBigQueryTableRow(
                    self._split_alternate_allele_info_fields))
            | 'WriteToBigQuery' >> beam.io.Write(beam.io.BigQuerySink(
                self._output_table,
                schema=bigquery_vcf_schema.generate_schema_from_header_fields(
                    self._header_fields,
                    self._variant_merger,
                    self._split_alternate_allele_info_fields),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                    if self._append
                    else beam.io.BigQueryDisposition.WRITE_TRUNCATE))))
