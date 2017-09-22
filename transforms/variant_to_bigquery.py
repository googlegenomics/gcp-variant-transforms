"""A PTransform to output a PCollection of ``Variant`` records to BigQuery."""

import apache_beam as beam

from libs import bigquery_vcf_schema

__all__ = ['VariantToBigQuery']


class _ConvertToBigQueryTableRow(beam.DoFn):
  """Converts a ``Variant`` record to a BigQuery row."""

  def __init__(self, split_alternate_allele_info_fields=True):
    super(_ConvertToBigQueryTableRow, self).__init__()
    self._split_alternate_allele_info_fields = (
        split_alternate_allele_info_fields)

  def process(self, record):
    yield bigquery_vcf_schema.get_row_from_variant(
        record, self._split_alternate_allele_info_fields)


class VariantToBigQuery(beam.PTransform):
  """Writes PCollection of ``Variant`` records to BigQuery."""

  def __init__(self, output_table, header_fields,
               split_alternate_allele_info_fields=True):
    """Initializes the transform.

    Args:
      output_table (str): Full path of the output BigQuery table.
      header_fields (``libs.vcf_header_parser.HeaderFields``): A ``namedtuple``
        containing representative header fields for all ``Variant`` records.
        This is needed for dynamically generating the schema.
      split_alternate_allele_info_fields (bool): If true, all INFO fields with
        `Number=A` (i.e. one value for each alternate allele) will be stored
        under the `alternate_bases` record. If false, they will be stored with
        the rest of the INFO fields.
    """
    self._output_table = output_table
    self._header_fields = header_fields
    self._split_alternate_allele_info_fields = (
        split_alternate_allele_info_fields)

  def expand(self, pcoll):
    return (pcoll
            | 'ConvertToBigQueryTableRow' >> beam.ParDo(
                _ConvertToBigQueryTableRow(
                    self._split_alternate_allele_info_fields))
            | 'WriteToBigQuery' >> beam.io.Write(beam.io.BigQuerySink(
                self._output_table,
                schema=bigquery_vcf_schema.generate_schema_from_header_fields(
                    self._header_fields,
                    self._split_alternate_allele_info_fields),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_TRUNCATE))))
