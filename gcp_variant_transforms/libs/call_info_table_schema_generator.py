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

"""Generates call_info table schema."""

from apache_beam.io.gcp.internal.clients import bigquery

from gcp_variant_transforms.libs import bigquery_util

CALL_ID = 'call_id'
CALL_NAME = 'call_name'
FILE_PATH = 'file_path'
FILE_ID = 'file_id'
TABLE_SUFFIX = '_call_info'


def generate_schema():
  # type: () -> bigquery.TableSchema
  schema = bigquery.TableSchema()
  schema.fields.append(bigquery.TableFieldSchema(
      name=CALL_ID,
      type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description='An Integer that uniquely identifies a call.'))
  schema.fields.append(bigquery.TableFieldSchema(
      name=CALL_NAME,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('Name of the call.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=FILE_ID,
      type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('File id of the call.')))
  schema.fields.append(bigquery.TableFieldSchema(
      name=FILE_PATH,
      type=bigquery_util.TableFieldConstants.TYPE_STRING,
      mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
      description=('File path of the call.')))

  return schema
