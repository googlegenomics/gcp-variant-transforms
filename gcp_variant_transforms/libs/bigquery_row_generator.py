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

from typing import Any, Dict, List, Union  # pylint: disable=unused-import

from gcp_variant_transforms.libs import bigquery_sanitizer
from gcp_variant_transforms.libs import bigquery_schema_descriptor  # pylint: disable=unused-import
from gcp_variant_transforms.libs import vcf_field_conflict_resolver  # pylint: disable=unused-import


class BigQueryRowGenerator(object):
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
      null_numeric_value_replacement=None  # type: int
  ):
    # type: (...) -> None
    self._schema_descriptor = schema_descriptor
    self._conflict_resolver = conflict_resolver
    self._bigquery_field_sanitizer = bigquery_sanitizer.FieldSanitizer(
        null_numeric_value_replacement)

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

  def get_rows(self, data, **kwargs):
    # type:(Any, ** str) -> Dict
    """Yields BigQuery rows."""
    raise NotImplementedError
