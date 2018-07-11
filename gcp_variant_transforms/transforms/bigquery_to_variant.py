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

"""A PTransform to convert BigQuery table rows to a PCollection of `Variant`."""

from typing import Any, Dict, List  # pylint: disable=unused-import

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_util

# Constants for column names in the BigQuery schema.
BQ_COLUMN_KEY_CONSTANTS = [bigquery_util.ColumnKeyConstants.REFERENCE_NAME,
                           bigquery_util.ColumnKeyConstants.START_POSITION,
                           bigquery_util.ColumnKeyConstants.END_POSITION,
                           bigquery_util.ColumnKeyConstants.REFERENCE_BASES,
                           bigquery_util.ColumnKeyConstants.ALTERNATE_BASES,
                           bigquery_util.ColumnKeyConstants.NAMES,
                           bigquery_util.ColumnKeyConstants.QUALITY,
                           bigquery_util.ColumnKeyConstants.FILTER,
                           bigquery_util.ColumnKeyConstants.CALLS]

VARIANT_CALL_CONSTANTS = [bigquery_util.ColumnKeyConstants.CALLS_NAME,
                          bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE,
                          bigquery_util.ColumnKeyConstants.CALLS_PHASESET]


class BigQueryToVariant(beam.PTransform):
  """Transforms BigQuery table rows to PCollection of `Variant`."""

  def expand(self, pcoll):
    return (pcoll | 'BigQuery to Variant' >> beam.Map(
        self._convert_bq_row_to_variant))

  def _convert_bq_row_to_variant(self, row):
    # type: (Dict[str, Any]) -> vcfio.Variant
    return vcfio.Variant(
        reference_name=row[bigquery_util.ColumnKeyConstants.REFERENCE_NAME],
        start=row[bigquery_util.ColumnKeyConstants.START_POSITION],
        end=row[bigquery_util.ColumnKeyConstants.END_POSITION],
        reference_bases=row[bigquery_util.ColumnKeyConstants.REFERENCE_BASES],
        alternate_bases=self._get_alternate_bases(
            row[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES]),
        names=row[bigquery_util.ColumnKeyConstants.NAMES],
        quality=row[bigquery_util.ColumnKeyConstants.QUALITY],
        filters=row[bigquery_util.ColumnKeyConstants.FILTER],
        info=self._get_variant_info(row),
        calls=self._get_variant_calls(
            row[bigquery_util.ColumnKeyConstants.CALLS])
    )

  def _get_alternate_bases(self, alternate_base_records):
    # type: (List[Dict[str, Any]]) -> List[str]
    alternate_bases = []
    for record in alternate_base_records:
      alternate_bases.append(
          record[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES_ALT])
    return alternate_bases

  def _get_variant_info(self, row):
    # type: (Dict[str, Any]) -> Dict[str, Any]
    info = {}
    for key, value in row.iteritems():
      if key not in BQ_COLUMN_KEY_CONSTANTS and self._is_value_valid(value):
        info.update({key: value})
    for alt_base in row[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES]:
      for key, value in alt_base.iteritems():
        if (key != bigquery_util.ColumnKeyConstants.ALTERNATE_BASES_ALT and
            self._is_value_valid(value)):
          if key not in info:
            info.update({key: []})
          info[key].append(value)
    return info

  def _get_variant_calls(self, variant_call_records):
    # type: (List[Dict[str, Any]]) -> List[vcfio.VariantCall]
    variant_calls = []
    for call_record in variant_call_records:
      info = {}
      for key, value in call_record.iteritems():
        if key not in VARIANT_CALL_CONSTANTS and self._is_value_valid(value):
          info.update({key: value})
      variant_call = vcfio.VariantCall(
          name=call_record[bigquery_util.ColumnKeyConstants.CALLS_NAME],
          genotype=call_record[bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE],
          phaseset=call_record[bigquery_util.ColumnKeyConstants.CALLS_PHASESET],
          info=info)
      variant_calls.append(variant_call)
    return variant_calls

  def _is_value_valid(self, value):
    # type: (Any) -> bool
    if value is None:
      return False
    if isinstance(value, list) and not value:
      return False
    return True
