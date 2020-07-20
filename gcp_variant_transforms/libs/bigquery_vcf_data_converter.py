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

"""Converts BigQuery row to variant."""




from typing import Any, Dict, List  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs.annotation import annotation_parser
from gcp_variant_transforms.libs import bigquery_schema_descriptor  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import processed_variant  # pylint: disable=unused-import


# Reserved constants for column names in the BigQuery schema.
RESERVED_BQ_COLUMNS = [bigquery_util.ColumnKeyConstants.REFERENCE_NAME,
                       bigquery_util.ColumnKeyConstants.START_POSITION,
                       bigquery_util.ColumnKeyConstants.END_POSITION,
                       bigquery_util.ColumnKeyConstants.REFERENCE_BASES,
                       bigquery_util.ColumnKeyConstants.ALTERNATE_BASES,
                       bigquery_util.ColumnKeyConstants.NAMES,
                       bigquery_util.ColumnKeyConstants.QUALITY,
                       bigquery_util.ColumnKeyConstants.FILTER,
                       bigquery_util.ColumnKeyConstants.CALLS]

RESERVED_VARIANT_CALL_COLUMNS = [
    bigquery_util.ColumnKeyConstants.CALLS_SAMPLE_ID,
    bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE,
    bigquery_util.ColumnKeyConstants.CALLS_PHASESET
]


class VariantGenerator(object):
  """Class to generate variant from one BigQuery row."""

  def __init__(self, annotation_id_to_annotation_names=None):
    # type: (Dict[str, List[str]]) -> None
    """Initializes an object of `VariantGenerator`.

    Args:
      annotation_id_to_annotation_names: A map where the key is the annotation
        id (e.g., `CSQ`) and the value is a list of annotation names (e.g.,
        ['allele', 'Consequence', 'IMPACT', 'SYMBOL']). The annotation str
        (e.g., 'A|upstream_gene_variant|MODIFIER|PSMF1|||||') is reconstructed
        in the same order as the annotation names.
    """
    self._annotation_str_builder = annotation_parser.AnnotationStrBuilder(
        annotation_id_to_annotation_names)

  def convert_bq_row_to_variant(self, row):
    """Converts one BigQuery row to `Variant`."""
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
    return [record[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES_ALT]
            for record in alternate_base_records]

  def _get_variant_info(self, row):
    # type: (Dict[str, Any]) -> Dict[str, Any]
    info = {}
    for key, value in list(row.items()):
      if key not in RESERVED_BQ_COLUMNS and not self._is_null_or_empty(value):
        info.update({key: value})
    for alt_base in row[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES]:
      for key, value in list(alt_base.items()):
        if (key != bigquery_util.ColumnKeyConstants.ALTERNATE_BASES_ALT and
            not self._is_null_or_empty(value)):
          if key not in info:
            info[key] = []
          if self._annotation_str_builder.is_valid_annotation_id(key):
            info[key].extend(
                self._annotation_str_builder.reconstruct_annotation_str(
                    key, value))
          else:
            info[key].append(value)
    return info

  def _get_variant_calls(self, variant_call_records):
    # type: (List[Dict[str, Any]]) -> List[vcfio.VariantCall]
    variant_calls = []
    for call_record in variant_call_records:
      info = {}
      for key, value in list(call_record.items()):
        if (key not in RESERVED_VARIANT_CALL_COLUMNS and
            not self._is_null_or_empty(value)):
          info.update({key: value})
      variant_call = vcfio.VariantCall(
          sample_id=call_record[
              bigquery_util.ColumnKeyConstants.CALLS_SAMPLE_ID],
          genotype=call_record[bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE],
          phaseset=call_record[bigquery_util.ColumnKeyConstants.CALLS_PHASESET],
          info=info)
      variant_calls.append(variant_call)
    return variant_calls

  def _is_null_or_empty(self, value):
    # type: (Any) -> bool
    if value is None:
      return True
    if isinstance(value, list) and not value:
      return True
    return False
