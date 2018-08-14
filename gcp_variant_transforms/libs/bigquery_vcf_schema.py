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

from typing import Dict, Any  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import bigquery_schema_descriptor  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import processed_variant  # pylint: disable=unused-import
from gcp_variant_transforms.libs import vcf_field_conflict_resolver  # pylint: disable=unused-import
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy  # pylint: disable=unused-import


# An alias for the header key constants to make referencing easier.
_HeaderKeyConstants = vcf_header_io.VcfParserHeaderKeyConstants


def generate_schema_from_header_fields(
    header_fields,  # type: vcf_header_io.VcfHeader
    proc_variant_factory,  # type: processed_variant.ProcessedVariantFactory
    variant_merger=None  # type: variant_merge_strategy.VariantMergeStrategy
    ):
  # type: (...) -> bigquery.TableSchema
  """Returns a ``TableSchema`` for the BigQuery table storing variants.

  Args:
    header_fields: Representative header fields for all variants.
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
        type=bigquery_util.get_bigquery_type_from_vcf_type(
            field[_HeaderKeyConstants.TYPE]),
        mode=bigquery_util.get_bigquery_mode_from_vcf_num(
            field[_HeaderKeyConstants.NUM]),
        description=bigquery_util.get_bigquery_sanitized_field(
            field[_HeaderKeyConstants.DESC])))
  schema.fields.append(calls_record)

  # Add info fields.
  info_keys = set()
  for key, field in header_fields.infos.iteritems():
    # END info is already included by modifying the end_position.
    if (key == vcfio.END_INFO_KEY or
        proc_variant_factory.info_is_in_alt_bases(key) or
        key in set(proc_variant_factory.gen_all_info_type_keys())):
      continue
    schema.fields.append(bigquery.TableFieldSchema(
        name=bigquery_util.get_bigquery_sanitized_field_name(key),
        type=bigquery_util.get_bigquery_type_from_vcf_type(
            field[_HeaderKeyConstants.TYPE]),
        mode=bigquery_util.get_bigquery_mode_from_vcf_num(
            field[_HeaderKeyConstants.NUM]),
        description=bigquery_util.get_bigquery_sanitized_field(
            field[_HeaderKeyConstants.DESC])))
    info_keys.add(key)
  if variant_merger:
    variant_merger.modify_bigquery_schema(schema, info_keys)
  return schema
