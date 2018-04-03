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

"""A mock for SchemaDescriptor class for testing."""

from __future__ import absolute_import

from apache_beam.io.gcp.internal.clients import bigquery
from gcp_variant_transforms.libs import bigquery_schema_descriptor


# TODO(nmousavi): consider using mock.patch once pickling issue is solved.
# https://bugs.python.org/issue14577
class MockSchemaDescriptor(bigquery_schema_descriptor.SchemaDescriptor):
  """Mock schema descriptor that returns default value to an API call."""

  def __init__(self):
    empty_schema = bigquery.TableSchema()
    super(MockSchemaDescriptor, self).__init__(empty_schema)

  def has_simple_field(self, field_name):
    # pylint: disable=unused-argument
    return True

  def get_field_descriptor(self, field_name):
    # pylint: disable=unused-argument
    return None

  def get_record_schema_descriptor(self, record_name):
    # pylint: disable=unused-argument
    return self
