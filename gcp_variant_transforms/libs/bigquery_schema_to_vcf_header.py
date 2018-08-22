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

"""Handles generation of VCF header from BigQuery schema."""

from typing import Dict  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery  # pylint: disable=unused-import

from vcf import parser

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import bigquery_util


# The Constant fields included below are not part of the INFO or FORMAT in the
# VCF header.
_NON_RECORD_CONSTANT_FIELDS = [bigquery_util.ColumnKeyConstants.REFERENCE_NAME,
                               bigquery_util.ColumnKeyConstants.START_POSITION,
                               bigquery_util.ColumnKeyConstants.END_POSITION,
                               bigquery_util.ColumnKeyConstants.REFERENCE_BASES,
                               bigquery_util.ColumnKeyConstants.NAMES,
                               bigquery_util.ColumnKeyConstants.QUALITY,
                               bigquery_util.ColumnKeyConstants.FILTER]

_CONSTANT_CALL_FIELDS = [bigquery_util.ColumnKeyConstants.CALLS_NAME,
                         bigquery_util.ColumnKeyConstants.CALLS_GENOTYPE,
                         bigquery_util.ColumnKeyConstants.CALLS_PHASESET]

_CONSTANT_ALTERNATE_BASES_FIELDS = [
    bigquery_util.ColumnKeyConstants.ALTERNATE_BASES_ALT]

_BIG_QUERY_TYPE_TO_VCF_TYPE_MAP = bigquery_util.BIG_QUERY_TYPE_TO_VCF_TYPE_MAP
_Format = parser._Format
_Info = parser._Info


def generate_header_fields_from_schema(schema):
  # type: (bigquery.TableSchema) -> vcf_header_io.VcfHeader
  """Returns header fields converted from BigQuery schema.

  This is done on a best effort basis. Only INFO and FORMAT are considered. For
  each header field, the type is mapped from BigQuery schema field type to
  VCF type. Since the Number information is not included in BigQuery schema,
  the Number remains to be an unknown value (Number=`.`). One exception is the
  sub fields in alternate_bases, which have `Number=A`.
  """
  infos = {}  # type: Dict[str, _Info]
  formats = {}  # type: Dict[str, _Format]
  for field in schema.fields:
    if field.name not in _NON_RECORD_CONSTANT_FIELDS:
      if field.name == bigquery_util.ColumnKeyConstants.CALLS:
        _add_format_fields(field, formats)
      elif field.name == bigquery_util.ColumnKeyConstants.ALTERNATE_BASES:
        _add_info_fields_from_alternate_bases(field, infos)
      else:
        infos.update({
            field.name: _Info(field.name,
                              vcfio.MISSING_FIELD_VALUE,
                              _BIG_QUERY_TYPE_TO_VCF_TYPE_MAP.get(field.type),
                              field.description,
                              'src',
                              'v')})

  return vcf_header_io.VcfHeader(infos=infos, formats=formats)


def _add_format_fields(schema, formats):
  # type: (bigquery.TableFieldSchema, Dict[str, _Format]) -> None
  for field in schema.fields:
    if field not in _CONSTANT_CALL_FIELDS:
      formats.update({
          field.name: _Format(field.name,
                              vcfio.MISSING_FIELD_VALUE,
                              _BIG_QUERY_TYPE_TO_VCF_TYPE_MAP.get(field.type),
                              field.description)})


def _add_info_fields_from_alternate_bases(schema, infos):
  # type: (bigquery.TableFieldSchema, Dict[str, _Info]) -> None
  for field in schema.fields:
    if field.name not in _CONSTANT_ALTERNATE_BASES_FIELDS:
      infos.update({
          field.name: _Info(field.name,
                            vcfio.FIELD_COUNT_ALTERNATE_ALLELE,
                            _BIG_QUERY_TYPE_TO_VCF_TYPE_MAP.get(field.type),
                            field.description,
                            'src',
                            'v')})
