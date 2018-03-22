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

"""Handles generation and processing of BigQuery schema for variants."""

from __future__ import absolute_import

import copy
import json
from typing import Dict, Any  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_schema_descriptor  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import processed_variant  # pylint: disable=unused-import
from gcp_variant_transforms.libs import vcf_field_conflict_resolver  # pylint: disable=unused-import
from gcp_variant_transforms.libs import vcf_header_parser # pylint: disable=unused-import
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy # pylint: disable=unused-import


# Maximum size of a BigQuery row is 10MB. See
# https://cloud.google.com/bigquery/quotas#import for details.
# We set it to 10MB - 10KB to leave a bit of room for error in case jsonifying
# the object is not exactly the same in different libraries.
_MAX_BIGQUERY_ROW_SIZE_BYTES = 10 * 1024 * 1024 - 10 * 1024
# Number of bytes to add to the object size when concatenating calls (i.e.
# to account for ", "). We use 5 bytes to be conservative.
_JSON_CONCATENATION_OVERHEAD_BYTES = 5


def generate_schema_from_header_fields(
    header_fields,  # type: vcf_header_parser.HeaderFields
    proc_variant_factory,  # type: processed_variant.ProcessedVariantFactory
    variant_merger=None  # type: variant_merge_strategy.VariantMergeStrategy
    ):
  """Returns a ``TableSchema`` for the BigQuery table storing variants.

  Args:
    header_fields: A `namedtuple` containing representative header fields for
      all variant records. This specifies custom INFO and FORMAT fields in the
      VCF file(s).
    proc_variant_factory: The factory class that knows how to convert Variant
      instances to ProcessedVariant. As a side effect it also knows how to
      modify BigQuery schema based on the ProcessedVariants that it generates.
      The latter functionality is what is needed here.
    variant_merger: The strategy used for merging variants (if any). Some
      strategies may change the schema, which is why this may be needed here.
  """
  schema = bigquery.TableSchema()
  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.REFERENCE_NAME,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='Reference name.'))
  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.START_POSITION,
      type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('Start position (0-based). Corresponds to the first base '
                   'of the string of reference bases.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.END_POSITION,
      type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('End position (0-based). Corresponds to the first base '
                   'after the last base in the reference allele.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.REFERENCE_BASES,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='Reference bases.'))

  schema.fields.append(proc_variant_factory.create_alt_bases_field_schema())

  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.NAMES,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
      description='Variant names (e.g. RefSNP ID).'))
  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.QUALITY,
      type=bigquery_util.TableFieldConstants.TYPE_FLOAT,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('Phred-scaled quality score (-10log10 prob(call is wrong)). '
                   'Higher values imply better quality.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.FILTER,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
      description=('List of failed filters (if any) or "PASS" indicating the '
                   'variant has passed all filters.')))

  # Add calls.
  calls_record = bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.CALLS,
      type=bigquery_util.TableFieldConstants.TYPE_RECORD,
      mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
      description='One record for each call.')
  calls_record.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.CALLS_NAME,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='Name of the call.'))
  calls_record.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE,
      type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
      mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
      description=('Genotype of the call. "-1" is used in cases where the '
                   'genotype is not called.')))
  calls_record.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.CALLS_PHASESET,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('Phaseset of the call (if any). "*" is used in cases where '
                   'the genotype is phased, but no phase set ("PS" in FORMAT) '
                   'was specified.')))
  for key, field in header_fields.formats.iteritems():
    # GT and PS are already included in 'genotype' and 'phaseset' fields.
    if key in (vcfio.GENOTYPE_FORMAT_KEY, vcfio.PHASESET_FORMAT_KEY):
      continue
    calls_record.fields.append(bigquery.TableFieldSchema(
        name=bigquery_util.get_bigquery_sanitized_field_name(key),
        type=bigquery_util.get_bigquery_type_from_vcf_type(field.type),
        mode=_get_bigquery_mode_from_vcf_num(field.num),
        description=bigquery_util.get_bigquery_sanitized_field(field.desc)))
  schema.fields.append(calls_record)

  # Add info fields.
  info_keys = set()
  for key, field in header_fields.infos.iteritems():
    # END info is already included by modifying the end_position.
    if (key == vcfio.END_INFO_KEY or
        proc_variant_factory.info_is_in_alt_bases(key)):
      continue
    schema.fields.append(bigquery.TableFieldSchema(
        name=bigquery_util.get_bigquery_sanitized_field_name(key),
        type=bigquery_util.get_bigquery_type_from_vcf_type(field.type),
        mode=_get_bigquery_mode_from_vcf_num(field.num),
        description=bigquery_util.get_bigquery_sanitized_field(field.desc)))
    info_keys.add(key)
  if variant_merger:
    variant_merger.modify_bigquery_schema(schema, info_keys)
  return schema


# TODO: refactor this to use a class instead.
def get_rows_from_variant(variant,
                          schema_descriptor,
                          conflict_resolver=None,
                          allow_incompatible_records=False,
                          omit_empty_sample_calls=False):
  # type: (processed_variant.ProcessedVariant,
  #        bigquery_schema_descriptor.SchemaDescriptor,
  #        vcf_field_conflict_resolver.ConflictResolver,
  #        processed_variant.ProcessedVariant,
  #        bool) -> Dict
  """Yields BigQuery rows according to the schema from the given variant.

  There is a 10MB limit for each BigQuery row, which can be exceeded by having
  a large number of calls. This method may split up a row into multiple rows if
  it exceeds 10MB.

  Args:
    variant: Variant to convert to a row.
    schema_descriptor: Descriptor for the BigQuery schema.
    conflict_resolver: Used to resolve conflicts between schema and variant data
      (if any).
    allow_incompatible_records: If true, field values are casted to Bigquery
      schema if there is a mistmatch.
    omit_empty_sample_calls: If true, samples that don't have a given
      call will be omitted.
  Yields:
    A dict representing a BigQuery row from the given variant. The row may have
    a subset of the calls if it exceeds the maximum allowed BigQuery row size.
  Raises:
    ValueError: If variant data is inconsistent or invalid.
  """
  base_row = _get_base_row_from_variant(
      variant, schema_descriptor, conflict_resolver, allow_incompatible_records)
  base_row_size_in_bytes = _get_json_object_size(base_row)
  row_size_in_bytes = base_row_size_in_bytes
  row = copy.deepcopy(base_row)  # Keep base_row intact.

  call_record_schema_descriptor = (
      schema_descriptor.get_record_schema_descriptor(
          bigquery_util.ColumnKeyConstants.CALLS))
  for call in variant.calls:
    call_record, empty = _get_call_record(
        call, call_record_schema_descriptor, conflict_resolver,
        allow_incompatible_records)
    if omit_empty_sample_calls and empty:
      continue

    # Add a few bytes to account for surrounding characters when concatenating.
    call_record_size_in_bytes = (
        _get_json_object_size(call_record) + _JSON_CONCATENATION_OVERHEAD_BYTES)
    if (row_size_in_bytes + call_record_size_in_bytes >=
        _MAX_BIGQUERY_ROW_SIZE_BYTES):
      yield row
      row = copy.deepcopy(base_row)
      row_size_in_bytes = base_row_size_in_bytes
    row[bigquery_util.ColumnKeyConstants.CALLS].append(call_record)
    row_size_in_bytes += call_record_size_in_bytes
  yield row


def _get_call_record(
    call, # type: VariantCall
    schema_descriptor, # type: bigquery_schema_descriptor.SchemaDescriptor
    conflict_resolver=None, # type: vcf_field_conflict_resolver.ConflictResolver
    allow_incompatible_records=False # type: bool
    ):
  """A helper method for ``get_rows_from_variant`` to get a call as JSON.

  Args:
    call: Variant call to convert.
    schema_descriptor: Descriptor for the BigQuery schema.
    conflict_resolver: Used to resolve conflicts between schema and variant
      data (if any).
    allow_incompatible_records: If true, field values are casted to Bigquery
      schema if there is a mistmatch.

  Returns:
    BigQuery call value (dict).
  """
  call_record = {
      bigquery_util.ColumnKeyConstants.CALLS_NAME:
          bigquery_util.get_bigquery_sanitized_field(call.name),
      bigquery_util.ColumnKeyConstants.CALLS_PHASESET: call.phaseset,
      bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE: call.genotype or []
  }
  is_empty = (not call.genotype or
              set(call.genotype) == set((vcfio.MISSING_GENOTYPE_VALUE,)))
  for key, data in call.info.iteritems():
    if data is not None:
      field_name = bigquery_util.get_bigquery_sanitized_field_name(key)
      if not schema_descriptor.has_simple_field(field_name):
        raise ValueError('BigQuery schema has no such field:%s.\n'
                         'This happen if the field is not defined in the VCF '
                         'headers, nor is inferred automatically. If latter, '
                         'try piepline again with --infer_undefined_headers '
                         'flag.')
      field_data = bigquery_util.get_bigquery_sanitized_field(data)
      if allow_incompatible_records:
        field_data = _make_field_data_compatible_with_schema(
            field_name, field_data, schema_descriptor, conflict_resolver)
      call_record[field_name] = field_data
      is_empty = is_empty and _is_empty_field(field_data)
  return call_record, is_empty


def _make_field_data_compatible_with_schema(field_name,
                                            field_data,
                                            schema_descriptor,
                                            conflict_resolver):
  # Check for conflict between field data and schema. Resolve it if any.
  field_data = conflict_resolver.resolve_schema_conflict(
      schema_descriptor.get_field_descriptor(field_name), field_data)
  return field_data


def _get_base_row_from_variant(variant,
                               schema_descriptor,
                               conflict_resolver=None,
                               allow_incompatible_records=False):
  # type: (processed_variant.ProcessedVariant,
  #        bigquery_schema_descriptor.SchemaDescriptor,
  #        vcf_field_conflict_resolver.ConflictResolver) -> Dict[str, Any]
  """A helper method for ``get_rows_from_variant`` to get row without calls."""
  row = {
      bigquery_util.ColumnKeyConstants.REFERENCE_NAME: variant.reference_name,
      bigquery_util.ColumnKeyConstants.START_POSITION: variant.start,
      bigquery_util.ColumnKeyConstants.END_POSITION: variant.end,
      bigquery_util.ColumnKeyConstants.REFERENCE_BASES: variant.reference_bases
  }  # type: Dict[str, Any]
  if variant.names:
    row[bigquery_util.ColumnKeyConstants.NAMES] = (
        bigquery_util.get_bigquery_sanitized_field(variant.names))
  if variant.quality is not None:
    row[bigquery_util.ColumnKeyConstants.QUALITY] = variant.quality
  if variant.filters:
    row[bigquery_util.ColumnKeyConstants.FILTER] = (
        bigquery_util.get_bigquery_sanitized_field(variant.filters))
  # Add alternate bases.
  row[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES] = []
  for alt in variant.alternate_data_list:
    alt_record = {bigquery_util.ColumnKeyConstants.ALTERNATE_BASES_ALT:
                  alt.alternate_bases}
    for key, data in alt.info.iteritems():
      alt_record[bigquery_util.get_bigquery_sanitized_field_name(key)] = data
    row[bigquery_util.ColumnKeyConstants.ALTERNATE_BASES].append(alt_record)
  # Add info.
  for key, data in variant.non_alt_info.iteritems():
    if data is not None:
      field_name = bigquery_util.get_bigquery_sanitized_field_name(key)
      field_data = bigquery_util.get_bigquery_sanitized_field(data)
      if allow_incompatible_records:
        field_data = _make_field_data_compatible_with_schema(
            field_name, field_data, schema_descriptor, conflict_resolver)
      row[field_name] = field_data
  # Set calls to empty for now (will be filled later).
  row[bigquery_util.ColumnKeyConstants.CALLS] = []
  return row


def _get_bigquery_mode_from_vcf_num(vcf_num):
  if vcf_num in (0, 1):
    return bigquery_util.TableFieldConstants.MODE_NULLABLE
  else:
    return bigquery_util.TableFieldConstants.MODE_REPEATED


def _is_empty_field(value):
  return (value in (vcfio.MISSING_FIELD_VALUE, [vcfio.MISSING_FIELD_VALUE]) or
          (not value and value != 0))


def _get_json_object_size(obj):
  return len(json.dumps(obj))
