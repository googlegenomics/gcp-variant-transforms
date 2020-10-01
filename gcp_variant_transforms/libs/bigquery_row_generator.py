# Copyright 2019 Google LLC.
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

"""Converts `Variant` to BigQuery row."""

from typing import Any, Dict, List, Union  # pylint: disable=unused-import

import copy
import json

from gcp_variant_transforms.beam_io import vcf_parser
from gcp_variant_transforms.libs import bigquery_sanitizer
from gcp_variant_transforms.libs import bigquery_schema_descriptor  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import processed_variant  # pylint: disable=unused-import
from gcp_variant_transforms.libs import vcf_field_conflict_resolver  # pylint: disable=unused-import

_BigQuerySchemaSanitizer = bigquery_sanitizer.SchemaSanitizer


class BigQueryRowGenerator():
  """Base abstract class for BigQuery row generator.

  The base class provides the common functionalities when generating BigQuery
  row (e.g., sanitizing the BigQuery field, resolving the conflicts between the
  schema and data).
  Derived classes must implement `get_rows`.
  """

  def __init__(
      self,
      schema_descriptor,  # type: bigquery_schema_descriptor.SchemaDescriptor
      conflict_resolver=None,
      # type: vcf_field_conflict_resolver.ConflictResolver
      null_numeric_value_replacement=None,  # type: int
      include_call_name=False  # type: bool
  ):
    # type: (...) -> None
    self._schema_descriptor = schema_descriptor
    self._conflict_resolver = conflict_resolver
    self._bigquery_field_sanitizer = bigquery_sanitizer.FieldSanitizer(
        null_numeric_value_replacement)
    self._include_call_name = include_call_name

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
    field_name = bigquery_sanitizer.SchemaSanitizer.get_sanitized_field_name(
        key)
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

  def _get_call_record(
      self,
      call,  # type: vcf_parser.VariantCall
      schema_descriptor,  # type: bigquery_schema_descriptor.SchemaDescriptor
      allow_incompatible_records,  # type: bool
  ):
    # type: (...) -> (Dict[str, Any], bool)
    """A helper method for ``get_rows`` to get a call as JSON.

    Args:
      call: Variant call to convert.
      schema_descriptor: Descriptor for the BigQuery schema of call record.

    Returns:
      BigQuery call value (dict).
    """
    call_record = {
        bigquery_util.ColumnKeyConstants.CALLS_PHASESET: call.phaseset,
        bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE: call.genotype or []
    }

    is_empty = (not call.genotype or
                set(call.genotype) == {vcf_parser.MISSING_GENOTYPE_VALUE})
    for key, data in call.info.items():
      if data is not None:
        field_name, field_data = self._get_bigquery_field_entry(
            key, data, schema_descriptor,
            allow_incompatible_records)
        call_record[field_name] = field_data
        is_empty = is_empty and self._is_empty_field(field_data)
    return call_record, is_empty

  def _get_base_variant_record(self, variant):
    # type: (processed_variant.ProcessedVariant) -> Dict[str, Any]
    return {
        bigquery_util.ColumnKeyConstants.REFERENCE_NAME: variant.reference_name,
        bigquery_util.ColumnKeyConstants.START_POSITION: variant.start,
        bigquery_util.ColumnKeyConstants.END_POSITION: variant.end,
        bigquery_util.ColumnKeyConstants.REFERENCE_BASES:
            variant.reference_bases
    }

  def _get_variant_meta_record(self, variant, allow_incompatible_records):
    row = {}
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
      for key, data in alt.info.items():
        alt_record[_BigQuerySchemaSanitizer.get_sanitized_field_name(key)] = (
            data if key in alt.annotation_field_names else
            self._bigquery_field_sanitizer.get_sanitized_field(data))
      row[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES].append(alt_record)
    # Add info.
    for key, data in variant.non_alt_info.items():
      if data is not None:
        field_name, field_data = self._get_bigquery_field_entry(
            key, data, self._schema_descriptor, allow_incompatible_records)
        row[field_name] = field_data

    return row

  def _is_empty_field(self, value):
    return (
        value in (vcf_parser.MISSING_FIELD_VALUE,
                  [vcf_parser.MISSING_FIELD_VALUE]) or
        (not value and value != 0))

  def get_rows(self, data, **kwargs):
    # type:(Any, ** str) -> Dict
    """Yields BigQuery rows."""
    raise NotImplementedError


class VariantCallRowGenerator(BigQueryRowGenerator):
  """Class to generate BigQuery row from a variant and its call."""

  # Constants for calculating row size. This is needed because the maximum size
  # of a BigQuery row is 100MB. See
  # https://cloud.google.com/bigquery/quotas#import for details.
  # We set it to 90MB to leave some room for error as our row estimate is based
  # on sampling rather than exact byte size.
  _MAX_BIGQUERY_ROW_SIZE_BYTES = 90 * 1024 * 1024
  # Number of calls to sample for BigQuery row size estimation.
  _NUM_CALL_SAMPLES = 5
  # Row size estimation based on sampling calls can be expensive and unnecessary
  # when there are not enough calls, so it's only enabled when there are at
  # least this many calls.
  _MIN_NUM_CALLS_FOR_ROW_SIZE_ESTIMATION = 100

  def get_rows(self,
               variant,
               allow_incompatible_records=False,
               omit_empty_sample_calls=False):
    # type: (processed_variant.ProcessedVariant, bool, bool) -> Dict
    """Yields BigQuery rows according to the schema from the given variant.

    There is a 100MB limit for each BigQuery row, which can be exceeded by
    having a large number of calls. This method may split up a row into
    multiple rows if it exceeds the limit.

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
    call_limit_per_row = self._get_call_limit_per_row(variant)
    if call_limit_per_row < len(variant.calls):
      # Keep base_row intact if we need to split rows.
      row = copy.deepcopy(base_row)
    else:
      row = base_row

    call_record_schema_descriptor = (
        self._schema_descriptor.get_record_schema_descriptor(
            bigquery_util.ColumnKeyConstants.CALLS))
    num_calls_in_row = 0
    for call in variant.calls:
      call_record, empty = self._get_call_record(
          call,
          call_record_schema_descriptor,
          allow_incompatible_records)

      if omit_empty_sample_calls and empty:
        continue
      num_calls_in_row += 1
      if num_calls_in_row > call_limit_per_row:
        yield row
        num_calls_in_row = 1
        row = copy.deepcopy(base_row)
      row[bigquery_util.ColumnKeyConstants.CALLS].append(call_record)
    yield row

  def _get_call_record(
      self,
      call,  # type: vcfio.VariantCall
      schema_descriptor,  # type: bigquery_schema_descriptor.SchemaDescriptor
      allow_incompatible_records  # type: bool
  ):
    # type: (...) -> (Dict[str, Any], bool)
    call_record, is_empty = super()._get_call_record(
            call, schema_descriptor, allow_incompatible_records)
    call_record.update({
        bigquery_util.ColumnKeyConstants.CALLS_SAMPLE_ID:
            self._bigquery_field_sanitizer.get_sanitized_field(call.sample_id)
    })
    if self._include_call_name:
      call_record.update({
          bigquery_util.ColumnKeyConstants.CALLS_NAME:
              self._bigquery_field_sanitizer.get_sanitized_field(call.name)
      })
    return call_record, is_empty

  def _get_base_row_from_variant(self, variant, allow_incompatible_records):
    # type: (processed_variant.ProcessedVariant, bool) -> Dict[str, Any]
    row = super()._get_base_variant_record(variant)
    meta = super()._get_variant_meta_record(
        variant, allow_incompatible_records)
    row.update(meta)
    # Set calls to empty for now (will be filled later).
    row[bigquery_util.ColumnKeyConstants.CALLS] = []
    return row

  def _get_call_limit_per_row(self, variant):
    # type: (processed_variant.ProcessedVariant) -> int
    """Returns an estimate of maximum number of calls per BigQuery row.

    This method works by sampling `variant.calls` and getting an estimate based
    on the serialized JSON value.
    """
    # Assume that the non-call part of the variant is negligible compared to the
    # call part.
    num_calls = len(variant.calls)
    if num_calls < self._MIN_NUM_CALLS_FOR_ROW_SIZE_ESTIMATION:
      return num_calls
    average_call_size_bytes = self._get_average_call_size_bytes(variant.calls)
    if average_call_size_bytes * num_calls <= self._MAX_BIGQUERY_ROW_SIZE_BYTES:
      return num_calls
    else:
      return self._MAX_BIGQUERY_ROW_SIZE_BYTES // average_call_size_bytes

  def _get_average_call_size_bytes(self, calls):
    # type: (List[vcfio.VariantCall]) -> int
    """Returns the average call size based on sampling."""
    sum_sampled_call_size_bytes = 0
    num_sampled_calls = 0
    call_record_schema_descriptor = (
        self._schema_descriptor.get_record_schema_descriptor(
            bigquery_util.ColumnKeyConstants.CALLS))
    for call in calls[::len(calls) // self._NUM_CALL_SAMPLES]:
      call_record, _ = self._get_call_record(
          call, call_record_schema_descriptor, allow_incompatible_records=True)
      sum_sampled_call_size_bytes += self._get_json_object_size(call_record)
      num_sampled_calls += 1
    return sum_sampled_call_size_bytes // num_sampled_calls

  def _get_json_object_size(self, obj):
    return len(json.dumps(obj))
