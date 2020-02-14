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

"""Handles the conversion between BigQuery/Avro schema and VCF header."""

from __future__ import absolute_import

from collections import OrderedDict
import json
import logging
from typing import Dict, Union  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery
from apitools.base.protorpclite import messages  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcf_parser
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import processed_variant  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_sanitizer
from gcp_variant_transforms.libs import vcf_reserved_fields
from gcp_variant_transforms.libs.annotation import annotation_parser
from gcp_variant_transforms.libs.variant_merge import variant_merge_strategy  # pylint: disable=unused-import

# An alias for the header key constants to make referencing easier.
_HeaderKeyConstants = vcf_header_io.VcfParserHeaderKeyConstants

# The Constant fields included below are not part of the INFO or FORMAT in the
# VCF header.
_NON_INFO_OR_FORMAT_CONSTANT_FIELDS = [
    bigquery_util.ColumnKeyConstants.REFERENCE_NAME,
    bigquery_util.ColumnKeyConstants.START_POSITION,
    bigquery_util.ColumnKeyConstants.END_POSITION,
    bigquery_util.ColumnKeyConstants.REFERENCE_BASES,
    bigquery_util.ColumnKeyConstants.NAMES,
    bigquery_util.ColumnKeyConstants.QUALITY,
    bigquery_util.ColumnKeyConstants.FILTER
]

_CONSTANT_CALL_FIELDS = [bigquery_util.ColumnKeyConstants.CALLS_SAMPLE_ID,
                         bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE,
                         bigquery_util.ColumnKeyConstants.CALLS_PHASESET]

_CONSTANT_ALTERNATE_BASES_FIELDS = [
    bigquery_util.ColumnKeyConstants.ALTERNATE_BASES_ALT]


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
      name=bigquery_util.ColumnKeyConstants.CALLS_SAMPLE_ID,
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
        name=bigquery_sanitizer.SchemaSanitizer.get_sanitized_field_name(key),
        type=bigquery_util.get_bigquery_type_from_vcf_type(
            field[_HeaderKeyConstants.TYPE]),
        mode=bigquery_util.get_bigquery_mode_from_vcf_num(
            field[_HeaderKeyConstants.NUM]),
        description=bigquery_sanitizer.SchemaSanitizer.get_sanitized_string(
            field[_HeaderKeyConstants.DESC])))
  schema.fields.append(calls_record)

  # Add info fields.
  info_keys = set()
  annotation_info_type_keys_set = set(
      proc_variant_factory.gen_annotation_info_type_keys())
  for key, field in header_fields.infos.iteritems():
    # END info is already included by modifying the end_position. Info type
    # fields exist only to indicate the type of corresponding annotation fields,
    # and should not be added to the schema.
    if (key == vcfio.END_INFO_KEY or
        proc_variant_factory.info_is_in_alt_bases(key) or
        key in annotation_info_type_keys_set):
      continue
    schema.fields.append(bigquery.TableFieldSchema(
        name=bigquery_sanitizer.SchemaSanitizer.get_sanitized_field_name(key),
        type=bigquery_util.get_bigquery_type_from_vcf_type(
            field[_HeaderKeyConstants.TYPE]),
        mode=bigquery_util.get_bigquery_mode_from_vcf_num(
            field[_HeaderKeyConstants.NUM]),
        description=bigquery_sanitizer.SchemaSanitizer.get_sanitized_string(
            field[_HeaderKeyConstants.DESC])))
    info_keys.add(key)
  if variant_merger:
    variant_merger.modify_bigquery_schema(schema, info_keys)
  return schema


def _convert_repeated_field_to_avro_array(field, fields_list):
  # type: (messages.MessageField) -> Dict
  """Converts a repeated field to an Avro Array representation.

  For example the return value can be: {"type": "array", "items": "string"}
  """
  array_dict = {
      bigquery_util.AvroConstants.TYPE: bigquery_util.AvroConstants.ARRAY
  }
  if field.fields:
    array_dict[bigquery_util.AvroConstants.ITEMS] = {
        bigquery_util.AvroConstants.TYPE: bigquery_util.AvroConstants.RECORD,
        bigquery_util.AvroConstants.NAME: field.name,
        bigquery_util.AvroConstants.FIELDS: fields_list
    }
  else:
    array_dict[bigquery_util.AvroConstants.ITEMS] = {
        bigquery_util.AvroConstants.NAME: field.name,
        bigquery_util.AvroConstants.TYPE:
        bigquery_util.get_avro_type_from_bigquery_type_mode(
            field.type, field.mode)
    }
  # All repeated fields are nullable.
  return [bigquery_util.AvroConstants.NULL, array_dict]


def _convert_field_to_avro_dict(field):
  # type: (messages.MessageField) -> Dict
  field_dict = {}
  fields_list = []
  if field.fields:
    fields_list = [
        _convert_field_to_avro_dict(child_f) for child_f in field.fields]
  if field.mode == bigquery_util.TableFieldConstants.MODE_REPEATED:
    # TODO(bashir2): In this case both the name of the array and also individual
    # records in the array is f.name. Make sure this is according to Avro
    # spec then remove this TODO.
    field_dict[bigquery_util.AvroConstants.NAME] = field.name
    field_dict[bigquery_util.AvroConstants.TYPE] = (
        _convert_repeated_field_to_avro_array(field, fields_list))
  else:
    field_dict[bigquery_util.AvroConstants.NAME] = field.name
    field_dict[bigquery_util.AvroConstants.TYPE] = (
        bigquery_util.get_avro_type_from_bigquery_type_mode(
            field.type, field.mode))
    if field.fields:
      field_dict[bigquery_util.AvroConstants.FIELDS] = fields_list
  return field_dict


def _convert_schema_to_avro_dict(schema):
  # type: (bigquery.TableSchema) -> Dict
  fields_dict = {}
  # TODO(bashir2): Check if we need `namespace` and `name` at the top level.
  fields_dict[bigquery_util.AvroConstants.NAME] = 'TBD'
  fields_dict[
      bigquery_util.AvroConstants.TYPE] = bigquery_util.AvroConstants.RECORD
  fields_dict[bigquery_util.AvroConstants.FIELDS] = [
      _convert_field_to_avro_dict(f) for f in schema.fields]
  return fields_dict


def convert_table_schema_to_json_avro_schema(schema):
  # type: (bigquery.TableSchema) -> str
  """Returns the Avro equivalent of the given `schema` in json format.

  For writing to Avro files, the only piece that is different is the schema. In
  other words the exact same `Dict` that represents a BigQuery row can be
  written to an Avro file if the schema of that file is equivalent to the
  BigQuery Table schema. This function generates that equivalent Avro schema.

  For details of Avro schema spec, see:
  https://avro.apache.org/docs/1.8.2/spec.html

  For concrete examples relevant to our BigQuery schema, consider the following
  three required fields:

  {
    "fields": [
      {
        "type": [ "string", "null"],
        "name": "reference_name"
      },
      {
        "type": ["int", "null"],
        "name": "start_position"
      },
      {
        "type": ["int", "null"],
        "name": "end_position"
      },
      ...
    ],
    "type": "record",
    "name": "TBD"
  }

  Note that the whole schema is represented as a `record` which has several
  `fields`. In the above example, only the first three `fields` are shown.
  A `NULLABLE` type in BigQuery schema is equivalent to a `type` array where
  `null` is one of the members.

  `REPEATED` fields, specially `REPEATED` `RECORD` fields, are a little more
  complex in Avro schema format. Here is one example for `alternate_bases`:
  {
    "type": [{
      "items": {
        "type": "record",
        "name": "alternate_bases",
        "fields": [
          {
            "type": ["string", "null"],
            "name": "alt"
          },
          {
            "type": ["float", "null"],
            "name": "AF"
          }
        ]
      },
      "type": "array"
    }, "null" ],
    "name": "alternate_bases"
  },

  Args:
    schema: This is the BigQuery table schema that is generated from input VCFs.
  """
  if not isinstance(schema, bigquery.TableSchema):
    raise ValueError(
        'Expected an instance of bigquery.TableSchema got {}'.format(
            type(schema)))
  schema_dict = _convert_schema_to_avro_dict(schema)
  json_str = json.dumps(schema_dict)
  logging.info('The Avro schema is: %s', json_str)
  return json_str

def _add_bq_field(field):
  sub_schema = OrderedDict()
  sub_schema["description"] = field.description
  if field.fields:
    sub_schema["fields"] = [_add_bq_field(elem) for elem in field.fields]
  sub_schema["mode"] = field.mode
  sub_schema["name"] = field.name
  sub_schema["type"] = field.type
  return sub_schema

def convert_table_schema_to_json_bq_schema(schema):
  # type: (bigquery.TableSchema) -> str
  """Returns the Bigquery equivalent of the given `schema` in json format."""
  if not isinstance(schema, bigquery.TableSchema):
    raise ValueError(
        'Expected an instance of bigquery.TableSchema got {}'.format(
            type(schema)))
  return json.dumps([_add_bq_field(elem) for elem in schema.fields])


def generate_header_fields_from_schema(schema, allow_incompatible_schema=False):
  # type: (bigquery.TableSchema, bool) -> vcf_header_io.VcfHeader
  """Returns header fields converted from BigQuery schema.

  This is a best effort reconstruction of header fields. Only INFO and FORMAT
  are considered. For each header field, the type is mapped from BigQuery
  schema field type to VCF type, and the number is inferred based on BigQuery
  schema field type and mode.

  Args:
    schema: BigQuery schema that is used to convert to header fields.
    allow_incompatible_schema: If true, the type and mode compatibility
      validation between `schema` and the reserved fields are skipped.
  Raises:
    ValueError: If the field schema type/mode is not consistent with the
      reserved type/mode.
  """
  infos = OrderedDict()  # type: OrderedDict[str, _Info]
  formats = OrderedDict()  # type: OrderedDict[str, _Format]
  for field in schema.fields:
    if (field.type not in bigquery_util.get_supported_bigquery_schema_types()
        or field.name in _NON_INFO_OR_FORMAT_CONSTANT_FIELDS):
      continue
    elif field.name == bigquery_util.ColumnKeyConstants.CALLS:
      _add_format_fields(field, formats, allow_incompatible_schema)
    else:
      _add_info_fields(field, infos, allow_incompatible_schema)

  return vcf_header_io.VcfHeader(infos=infos, formats=formats)


def _add_format_fields(schema, formats, allow_incompatible_schema=False):
  # type: (bigquery.TableFieldSchema, Dict[str, _Format], bool) -> None
  for field in schema.fields:
    if field.name in _CONSTANT_CALL_FIELDS:
      continue
    elif (field.name in vcf_reserved_fields.FORMAT_FIELDS.keys() and
          not allow_incompatible_schema):
      reserved_definition = vcf_reserved_fields.FORMAT_FIELDS.get(field.name)
      _validate_reserved_field(field, reserved_definition)
      formats.update({field.name: vcf_header_io.CreateFormatField(
          field.name,
          reserved_definition.num,
          reserved_definition.type,
          _remove_special_characters(field.description or
                                     reserved_definition.desc))})
    else:
      formats.update({field.name: vcf_header_io.CreateFormatField(
          field.name,
          bigquery_util.get_vcf_num_from_bigquery_schema(field.mode,
                                                         field.type),
          bigquery_util.get_vcf_type_from_bigquery_type(field.type),
          _remove_special_characters(field.description))})


def _add_info_fields(field, infos, allow_incompatible_schema=False):
  # type: (bigquery.TableFieldSchema, Dict[str, _Info], bool) -> None
  if field.name == bigquery_util.ColumnKeyConstants.ALTERNATE_BASES:
    _add_info_fields_from_alternate_bases(field,
                                          infos,
                                          allow_incompatible_schema)
  elif (field.name in vcf_reserved_fields.INFO_FIELDS.keys() and
        not allow_incompatible_schema):
    reserved_definition = vcf_reserved_fields.INFO_FIELDS.get(field.name)
    _validate_reserved_field(field, reserved_definition)
    infos.update({field.name: vcf_header_io.CreateInfoField(
        field.name,
        reserved_definition.num,
        reserved_definition.type,
        _remove_special_characters(field.description or
                                   reserved_definition.desc))})
  else:
    infos.update({field.name: vcf_header_io.CreateInfoField(
        field.name,
        bigquery_util.get_vcf_num_from_bigquery_schema(field.mode,
                                                       field.type),
        bigquery_util.get_vcf_type_from_bigquery_type(field.type),
        _remove_special_characters(field.description))})


def _add_info_fields_from_alternate_bases(schema,
                                          infos,
                                          allow_incompatible_schema=False):
  # type: (bigquery.TableFieldSchema, Dict[str, _Info], bool) -> None
  """Adds schema nested fields in alternate bases to `infos`.

  Notice that the validation of field mode is skipped for reserved fields since
  the mode (NULLABLE) of field in alternate bases is expected to be different
  from the mode (REPEATED) in reserved field definition.

  Any `Record` field within alternate bases is considered as an annotation
  field.
  """
  for field in schema.fields:
    if field.name in _CONSTANT_ALTERNATE_BASES_FIELDS:
      continue
    elif field.type == bigquery_util.TableFieldConstants.TYPE_RECORD:
      infos.update({field.name: vcf_header_io.CreateInfoField(
          field.name,
          vcfio.MISSING_FIELD_VALUE,
          bigquery_util._VcfHeaderTypeConstants.STRING,
          _remove_special_characters(_get_annotation_description(field)))})
    elif (field.name in vcf_reserved_fields.INFO_FIELDS.keys() and
          not allow_incompatible_schema):
      reserved_definition = vcf_reserved_fields.INFO_FIELDS.get(field.name)
      _validate_reserved_field_type(field, reserved_definition)
      infos.update({field.name: vcf_header_io.CreateInfoField(
          field.name,
          reserved_definition.num,
          reserved_definition.type,
          _remove_special_characters(field.description or
                                     reserved_definition.desc))})
    else:
      infos.update({field.name: vcf_header_io.CreateInfoField(
          field.name,
          vcf_parser.FIELD_COUNT_ALTERNATE_ALLELE,
          bigquery_util.get_vcf_type_from_bigquery_type(field.type),
          _remove_special_characters(field.description))})


def _validate_reserved_field(field_schema, reserved_definition):
  # type: (bigquery.TableFieldSchema, Union[_Format, _Info]) -> None
  """Validates the reserved field.

  Raises:
    ValueError: If the field schema type/mode is not consistent with the
      reserved type/mode.
  """
  _validate_reserved_field_type(field_schema, reserved_definition)
  _validate_reserved_field_mode(field_schema, reserved_definition)


def _validate_reserved_field_type(field_schema, reserved_definition):
  schema_type = bigquery_util.get_vcf_type_from_bigquery_type(field_schema.type)
  reserved_type = reserved_definition.type
  if schema_type != reserved_type:
    raise ValueError(
        'The type of field {} is different from the VCF spec: {} vs {}.'
        .format(field_schema.name, schema_type, reserved_type))


def _validate_reserved_field_mode(field_schema, reserved_definition):
  schema_mode = (field_schema.mode or
                 bigquery_util.TableFieldConstants.MODE_NULLABLE)
  reserved_mode = bigquery_util.get_bigquery_mode_from_vcf_num(
      reserved_definition.num)
  if schema_mode != reserved_mode:
    raise ValueError(
        'The mode of field {} is different from the VCF spec: {} vs {}.'
        .format(field_schema.name, schema_mode, reserved_mode))


def _get_annotation_description(field):
  return ' '.join([field.description,
                   annotation_parser.reconstruct_annotation_description(
                       [sub_field.name for sub_field in field.fields])])


def _remove_special_characters(description):
  return description.replace('\n', ' ') if description else ''
