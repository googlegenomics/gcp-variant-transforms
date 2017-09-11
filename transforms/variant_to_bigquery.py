"""A PTransform to output a PCollection of ``Variant`` records to BigQuery."""

import apache_beam as beam

from libs import bigquery_vcf_schema

__all__ = ['VariantToBigQuery']


class _ConvertToBigQueryTableRow(beam.DoFn):
  """Converts a ``Variant`` record to a BigQuery row."""

  def __init__(self):
    super(_ConvertToBigQueryTableRow, self).__init__()

  def process(self, record):
    yield bigquery_vcf_schema.get_row_from_variant(record)


class VariantToBigQuery(beam.PTransform):
  """Writes PCollection of ``Variant`` records to BigQuery."""

  def __init__(self, output_table):
    """Initializes the transform.

    Args:
      output_table (str): Full path of the output BigQuery table.
    """
    self._output_table = output_table

  def expand(self, pcoll):
    return (pcoll
            | 'ConvertToBigQueryTableRow' >> beam.ParDo(
                _ConvertToBigQueryTableRow())
            | 'WriteToBigQuery' >> beam.io.Write(beam.io.BigQuerySink(
                self._output_table,
                schema=bigquery_vcf_schema.generate_schema(),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_TRUNCATE))))
