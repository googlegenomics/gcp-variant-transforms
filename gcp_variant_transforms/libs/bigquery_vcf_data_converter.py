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

"""Handles the conversion between BigQuery row and variant."""

from __future__ import absolute_import

import copy
import json
from typing import Any, Dict, List  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs.annotation import annotation_parser
from gcp_variant_transforms.libs import bigquery_schema_descriptor  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_sanitizer
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import processed_variant  # pylint: disable=unused-import
from gcp_variant_transforms.libs import vcf_field_conflict_resolver  # pylint: disable=unused-import


# Maximum size of a BigQuery row is 10MB. See
# https://cloud.google.com/bigquery/quotas#import for details.
# We set it to 10MB - 10KB to leave a bit of room for error in case jsonifying
# the object is not exactly the same in different libraries.
_MAX_BIGQUERY_ROW_SIZE_BYTES = 10 * 1024 * 1024 - 10 * 1024
# Number of bytes to add to the object size when concatenating calls (i.e.
# to account for ", "). We use 5 bytes to be conservative.
_JSON_CONCATENATION_OVERHEAD_BYTES = 5
_BigQuerySchemaSanitizer = bigquery_sanitizer.SchemaSanitizer

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
    bigquery_util.ColumnKeyConstants.CALLS_NAME,
    bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE,
    bigquery_util.ColumnKeyConstants.CALLS_PHASESET
]


class BigQueryRowGenerator(object):
  """Class to generate BigQuery row from a variant."""

  def __init__(
      self,
      schema_descriptor,  # type: bigquery_schema_descriptor.SchemaDescriptor
      conflict_resolver=None,
      # type: vcf_field_conflict_resolver.ConflictResolver
      null_numeric_value_replacement=None  # type: int
      ):
    # type: (...) -> None
    self._schema_descriptor = schema_descriptor
    self._conflict_resolver = conflict_resolver
    self._bigquery_field_sanitizer = bigquery_sanitizer.FieldSanitizer(
        null_numeric_value_replacement)

  def get_rows(self,
               variant,
               allow_incompatible_records=False,
               omit_empty_sample_calls=False):
    # type: (processed_variant.ProcessedVariant, bool, bool) -> Dict
    """Yields BigQuery rows according to the schema from the given variant.

    There is a 10MB limit for each BigQuery row, which can be exceeded by having
    a large number of calls. This method may split up a row into multiple rows
    if it exceeds 10MB.

    Args:
      variant: Variant to convert to a row.
      allow_incompatible_records: If true, field values are casted to Bigquery
        schema if there is a mismatch.
      omit_empty_sample_calls: If true, samples that don't have a given
        call will be omitted.
    Yields:
      A dict representing a BigQuery row from the given variant. The row may
      have a subset of the calls if it exceeds the maximum allowed BigQuery
      row size.
    Raises:
      ValueError: If variant data is inconsistent or invalid.
    """

    base_row = self._get_base_row_from_variant(
        variant, allow_incompatible_records)
    base_row_size_in_bytes = self._get_json_object_size(base_row)
    row_size_in_bytes = base_row_size_in_bytes
    row = copy.deepcopy(base_row)  # Keep base_row intact.

    call_record_schema_descriptor = (
        self._schema_descriptor.get_record_schema_descriptor(
            bigquery_util.ColumnKeyConstants.CALLS))
    for call in variant.calls:
      call_record, empty = self._get_call_record(
          call, call_record_schema_descriptor, allow_incompatible_records)
      if omit_empty_sample_calls and empty:
        continue

      # Add a few bytes to account for surrounding characters when
      # concatenating.
      call_record_size_in_bytes = (
          self._get_json_object_size(call_record) +
          _JSON_CONCATENATION_OVERHEAD_BYTES)
      if (row_size_in_bytes + call_record_size_in_bytes >=
          _MAX_BIGQUERY_ROW_SIZE_BYTES):
        yield row
        row = copy.deepcopy(base_row)
        row_size_in_bytes = base_row_size_in_bytes
      row[bigquery_util.ColumnKeyConstants.CALLS].append(call_record)
      row_size_in_bytes += call_record_size_in_bytes
    yield row

  def _get_call_record(
      self,
      call,  # type: VariantCall
      call_record_schema_descriptor,
      # type: bigquery_schema_descriptor.SchemaDescriptor
      allow_incompatible_records,  # type: bool
      ):
    # type: (...) -> (Dict[str, Any], bool)
    """A helper method for ``get_rows`` to get a call as JSON.

    Args:
      call: Variant call to convert.
      call_record_schema_descriptor: Descriptor for the BigQuery schema of
        call record.

    Returns:
      BigQuery call value (dict).
    """
    call_record = {
        bigquery_util.ColumnKeyConstants.CALLS_NAME:
            self._bigquery_field_sanitizer.get_sanitized_field(call.name),
        bigquery_util.ColumnKeyConstants.CALLS_PHASESET: call.phaseset,
        bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE: call.genotype or []
    }
    is_empty = (not call.genotype or
                set(call.genotype) == set((vcfio.MISSING_GENOTYPE_VALUE,)))
    for key, data in call.info.iteritems():
      if data is not None:
        field_name, field_data = self._get_bigquery_field_entry(
            key, data, call_record_schema_descriptor,
            allow_incompatible_records)
        call_record[field_name] = field_data
        is_empty = is_empty and self._is_empty_field(field_data)
    return call_record, is_empty

  def _get_base_row_from_variant(self, variant, allow_incompatible_records):
    # type: (processed_variant.ProcessedVariant, bool) -> Dict[str, Any]
    row = {
        bigquery_util.ColumnKeyConstants.REFERENCE_NAME: variant.reference_name,
        bigquery_util.ColumnKeyConstants.START_POSITION: variant.start,
        bigquery_util.ColumnKeyConstants.END_POSITION: variant.end,
        bigquery_util.ColumnKeyConstants.REFERENCE_BASES:
        variant.reference_bases
    }  # type: Dict[str, Any]
    if variant.names:
      row[bigquery_util.ColumnKeyConstants.NAMES] = (
          self._bigquery_field_sanitizer.get_sanitized_field(variant.names))
    if variant.quality is not None:
      row[bigquery_util.ColumnKeyConstants.QUALITY] = variant.quality
    if variant.filters:
      row[bigquery_util.ColumnKeyConstants.FILTER] = (
          self._bigquery_field_sanitizer.get_sanitized_field(variant.filters))
    # Add alternate bases.
    row[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES] = []
    for alt in variant.alternate_data_list:
      alt_record = {bigquery_util.ColumnKeyConstants.ALTERNATE_BASES_ALT:
                    alt.alternate_bases}
      for key, data in alt.info.iteritems():
        alt_record[_BigQuerySchemaSanitizer.get_sanitized_field_name(key)] = (
            data if key in alt.annotation_field_names else
            self._bigquery_field_sanitizer.get_sanitized_field(data))
      row[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES].append(alt_record)
    # Add info.
    for key, data in variant.non_alt_info.iteritems():
      if data is not None:
        field_name, field_data = self._get_bigquery_field_entry(
            key, data, self._schema_descriptor, allow_incompatible_records)
        row[field_name] = field_data

    # Set calls to empty for now (will be filled later).
    row[bigquery_util.ColumnKeyConstants.CALLS] = []
    return row

  def _get_bigquery_field_entry(
      self,
      key,  # type: str
      data,  # type: Union[Any, List[Any]]
      schema_descriptor,  # type: bigquery_schema_descriptor.SchemaDescriptor
      allow_incompatible_records,  # type: bool
  ):
    # type: (...) -> (str, Any)
    if data is None:
      return None, None
    field_name = _BigQuerySchemaSanitizer.get_sanitized_field_name(key)
    if not schema_descriptor.has_simple_field(field_name):
      raise ValueError('BigQuery schema has no such field: {}.\n'
                       'This can happen if the field is not defined in '
                       'the VCF headers, or is not inferred automatically. '
                       'Retry pipeline with --infer_headers.'
                       .format(field_name))
    sanitized_field_data = self._bigquery_field_sanitizer.get_sanitized_field(
        data)
    field_schema = schema_descriptor.get_field_descriptor(field_name)
    field_data, is_compatible = self._check_and_resolve_schema_compatibility(
        field_schema, sanitized_field_data)
    if is_compatible or allow_incompatible_records:
      return field_name, field_data
    else:
      raise ValueError('Value and schema do not match for field {}. '
                       'Value: {} Schema: {}.'.format(
                           field_name, sanitized_field_data, field_schema))

  def _check_and_resolve_schema_compatibility(self, field_schema, field_data):
    resolved_field_data = self._conflict_resolver.resolve_schema_conflict(
        field_schema, field_data)
    return resolved_field_data, resolved_field_data == field_data

  def _is_empty_field(self, value):
    return (value in (vcfio.MISSING_FIELD_VALUE, [vcfio.MISSING_FIELD_VALUE]) or
            (not value and value != 0))

  def _get_json_object_size(self, obj):
    return len(json.dumps(obj))


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
    for key, value in row.iteritems():
      if key not in RESERVED_BQ_COLUMNS and not self._is_null_or_empty(value):
        info.update({key: value})
    for alt_base in row[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES]:
      for key, value in alt_base.iteritems():
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
      for key, value in call_record.iteritems():
        if (key not in RESERVED_VARIANT_CALL_COLUMNS and
            not self._is_null_or_empty(value)):
          info.update({key: value})
      variant_call = vcfio.VariantCall(
          name=call_record[bigquery_util.ColumnKeyConstants.CALLS_NAME],
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
