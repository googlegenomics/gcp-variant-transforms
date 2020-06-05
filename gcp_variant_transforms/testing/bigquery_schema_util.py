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

"""Utility functions for creating BigQuery schema used by unit tests."""

from __future__ import absolute_import

from apache_beam.io.gcp.internal.clients import bigquery

from gcp_variant_transforms.libs import bigquery_util


def get_sample_table_schema(with_annotation_fields=False):
  # type: () -> bigquery.TableSchema
  """Creates a sample BigQuery table schema for multiple unit tests.

  The schema contains the fixed schema fields for VCF. Besides that, it also
  has:
    - One sub field (`AF`) for `alternate_bases`.
    - Two sub fields (`FB` and `GQ`) for `call`.
    - Three INFO fields (`AA`, `IFR`, `IS`).
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
      description=('End position. Corresponds to the first base '
                   'after the last base in the reference allele.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.REFERENCE_BASES,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='Reference bases.'))

  alternate_bases_record = bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.ALTERNATE_BASES,
      type=bigquery_util.TableFieldConstants.TYPE_RECORD,
      mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
      description='One record for each alternate base (if any).')
  alternate_bases_record.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.ALTERNATE_BASES_ALT,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='Alternate base.'))
  alternate_bases_record.fields.append(bigquery.TableFieldSchema(
      name='AF',
      type=bigquery_util.TableFieldConstants.TYPE_FLOAT,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='desc'))

  if with_annotation_fields:
    annotation_record = bigquery.TableFieldSchema(
        name='CSQ',
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='desc')
    annotation_record.fields.append(bigquery.TableFieldSchema(
        name='Consequence',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='desc.'))
    annotation_record.fields.append(bigquery.TableFieldSchema(
        name='IMPACT',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='desc.'))
    alternate_bases_record.fields.append(annotation_record)

  schema.fields.append(alternate_bases_record)
  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.NAMES,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
      description='Variant names (e.g. RefSNP ID).'))
  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.QUALITY,
      type=bigquery_util.TableFieldConstants.TYPE_FLOAT,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('Higher values imply better quality.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=bigquery_util.ColumnKeyConstants.FILTER,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
      description=('List of failed filters (if any) or "PASS" indicating the '
                   'variant has passed all filters.')))
    # Call record.
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
  calls_record.fields.append(bigquery.TableFieldSchema(
      name='FB',
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='desc'))
  calls_record.fields.append(bigquery.TableFieldSchema(
      name='GQ',
      type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='desc'))
  schema.fields.append(calls_record)

  schema.fields.append(bigquery.TableFieldSchema(
      name='AA',
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='desc'))
  schema.fields.append(bigquery.TableFieldSchema(
      name='IFR',
      type=bigquery_util.TableFieldConstants.TYPE_FLOAT,
      mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
      description='desc'))
  schema.fields.append(bigquery.TableFieldSchema(
      name='IS',
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='desc'))

  return schema
