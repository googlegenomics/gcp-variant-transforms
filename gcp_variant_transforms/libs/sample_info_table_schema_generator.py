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

"""Generates sample_info table schema."""

from apache_beam.io.gcp.internal.clients import bigquery

from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import partitioning

SAMPLE_ID = 'sample_id'
SAMPLE_NAME = 'sample_name'
FILE_PATH = 'file_path'
INGESTION_DATETIME = 'ingestion_datetime'

SAMPLE_INFO_TABLE_SUFFIX = 'sample_info'
SAMPLE_INFO_TABLE_SCHEMA_FILE_PATH = (
    'gcp_variant_transforms/data/schema/sample_info.json')

def generate_schema():
  # type: () -> bigquery.TableSchema
  schema = bigquery.TableSchema()
  schema.fields.append(bigquery.TableFieldSchema(
      name=SAMPLE_ID,
      type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='An Integer that uniquely identifies a sample.'))
  schema.fields.append(bigquery.TableFieldSchema(
      name=SAMPLE_NAME,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('Name of the sample as we read it from the VCF file.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=FILE_PATH,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('Full file path on GCS of the sample.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=INGESTION_DATETIME,
      type=bigquery_util.TableFieldConstants.TYPE_TIMESTAMP,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('Ingestion datetime (up to current minute) of samples.')))

  return schema


def create_sample_info_table(output_table):
  full_table_id = bigquery_util.compose_table_name(output_table,
                                                   SAMPLE_INFO_TABLE_SUFFIX)
  partitioning.create_bq_table(full_table_id,
                               SAMPLE_INFO_TABLE_SCHEMA_FILE_PATH)
  return full_table_id
