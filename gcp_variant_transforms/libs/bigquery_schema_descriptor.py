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
"""A dict based description for BigQuery schema."""


from typing import NamedTuple
from apache_beam.io.gcp.internal.clients import bigquery  # pylint: disable=unused-import


# Stores data about a simple field (not a record) in BigQuery Schema.
FieldDescriptor = NamedTuple('FieldDescriptor', [('type', str), ('mode', str)])


class SchemaDescriptor():
  """A dict based description for :class:`bigquery.TableSchema` object.

     This class provides APIs for checking if and how (e.g. type, mode) a field
     is defined in the BigQuery schema. This is useful when checking if
     data matches its field definition in the schema for example.
  """

  def __init__(self, table_schema):
    # type: (bigquery.TableSchema) -> None

    # Dict of (field_name, :class:`FieldDescriptor`).
    self._field_descriptor_dict = {}
    # Dict of (record_name, :class:`SchemaDescriptor`).
    self._schema_descriptor_dict = {}

    self._extract_all_descriptors(table_schema)

  def _extract_all_descriptors(self, table_schema):
    # type: (bigquery.TableSchema) -> None
    """Extracts descriptor for fields and records in `table_schema`."""
    for field in table_schema.fields:
      if field.fields:
        # Record field.
        self._schema_descriptor_dict[field.name] = SchemaDescriptor(field)
      else:
        # Simple field.
        self._field_descriptor_dict[field.name] = FieldDescriptor(
            type=field.type, mode=field.mode)

  def has_simple_field(self, field_name):
    # type: (str) -> bool
    """Returns true iff the schema has a non-record field named `field_name`.

    Note: Records and their fields are ignored.

    Args:
      field_name: name of a field in the schema.
    """
    return field_name in self._field_descriptor_dict

  def get_field_descriptor(self, field_name):
    # type: (str) -> FieldDescriptor
    """Returns :class:`FieldDescriptor obj for the given field.

    Args:
      field_name: name of a simple (not a record) field in BigQuery table.
    """
    if field_name in self._field_descriptor_dict:
      return self._field_descriptor_dict[field_name]
    else:
      raise ValueError('Field descriptor not found. Not such field in Bigquery '
                       'schema: {}'.format(field_name))

  def get_record_schema_descriptor(self, record_name):
    # type: (str) -> SchemaDescriptor
    if record_name in self._schema_descriptor_dict:
      return self._schema_descriptor_dict[record_name]
    else:
      raise ValueError('Schema descriptor not found. No such record '
                       'in Bigquery schema: {}'.format(record_name))
