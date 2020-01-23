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

"""Class for resolving conflicts in VCF field definitions."""

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcf_parser
from gcp_variant_transforms.libs import bigquery_schema_descriptor  # pylint: disable=unused-import
from gcp_variant_transforms.libs import bigquery_util


class FieldConflictResolver(object):
  """A class for resolving all VCF field related mismatches.

  Example mismatch: conflict in definition of a VCF field (INFO, FORMAT, etc),
    or conflict between schema definition and parsed value for a VCF field.
  """

  def __init__(self,
               split_alternate_allele_info_fields=True,
               resolve_always=False):
    # type: (bool, bool) -> None
    """Initialize the class.

    Args:
      split_alternate_allele_info_fields: Whether INFO fields with `Number=A`
        are stored under the alternate_bases record.
      resolve_always: Always find a solution for the conflicts. When the
        conflicts are incompatible, convert all type conflicts to `String` and
        number conflicts to `.`.
    """
    self._split_alternate_allele_info_fields = (
        split_alternate_allele_info_fields)
    self._resolve_always = resolve_always

  def resolve_schema_conflict(self,
                              schema_field_descriptor,
                              vcf_field_value):
    # type: (bigquery_schema_descriptor.FieldDescriptor, Any) -> Any
    """Resolves conflict between schema and field value.

    Args:
      schema_field_descriptor: BigQuery field schema.
      vcf_field_value: field value parsed from VCF file.
    Returns:
      A copy of the given field value that matches the schema if there is
      a mismatch, otherwise the same given field value is returned.
    """
    if not schema_field_descriptor:
      # Nothing to resolve.
      return vcf_field_value

    # Resolve size conflict.
    is_schema_repeated = (schema_field_descriptor.mode ==
                          bigquery_util.TableFieldConstants.MODE_REPEATED)
    if isinstance(vcf_field_value, list) and not is_schema_repeated:
      if (schema_field_descriptor.type ==
          bigquery_util.TableFieldConstants.TYPE_BOOLEAN):
        vcf_field_value = bool(vcf_field_value)
      else:
        vcf_field_value = vcf_field_value[0] if vcf_field_value else None
    elif not isinstance(vcf_field_value, list) and is_schema_repeated:
      vcf_field_value = [vcf_field_value]

    # Return if there is no type conflict.
    # For a list, we only check the first element. All elements of the list
    # have the same type (unless there is a bug in parser).
    schema_field_type = bigquery_util.get_python_type_from_bigquery_type(
        schema_field_descriptor.type)
    if (vcf_field_value is None or
        isinstance(vcf_field_value, schema_field_type) or
        (isinstance(vcf_field_value, list) and
         (not vcf_field_value or
          isinstance(vcf_field_value[0], schema_field_type)))):
      return vcf_field_value

    # There is a type conflict. Resolve it.
    if not isinstance(vcf_field_value, list):
      return schema_field_type(vcf_field_value)
    return [schema_field_type(v) for v in vcf_field_value]

  def resolve_attribute_conflict(self, attribute_name, first_attribute_value,
                                 second_attribute_value):
    # type: (str, Union[str, int], Union[str, int]) -> Union[str, int]
    """Returns resolution for the conflicting field attributes.

    Args:
      attribute_name: A field definition in VCF header consists of attributes
        e.g. Type, Number, each has a value. E.g, Type='String'.
      first_attribute_value: first attribute value.
      second_attribute_value: second attribute value.
    Raises:
      ValueError: if the conflict cannot be resolved.
    """
    if attribute_name == vcf_header_io.VcfParserHeaderKeyConstants.TYPE:
      return self._resolve_type(first_attribute_value, second_attribute_value)
    elif attribute_name == vcf_header_io.VcfParserHeaderKeyConstants.NUM:
      return self._resolve_number(first_attribute_value, second_attribute_value)
    else:
      # We only care about conflicts in 'num' and 'type' attributes.
      # TODO(bashir2): add check for he desc for annotation_fields
      # (the desc must be equal).
      return first_attribute_value

  def _resolve_type(self, first, second):
    type_constants = vcf_header_io.VcfHeaderFieldTypeConstants
    numeric_types = (type_constants.INTEGER, type_constants.FLOAT)
    if first == second:
      return first
    elif first == '.' or second == '.':
      return first if second == '.' else second
    elif first in numeric_types and second in numeric_types:
      return type_constants.FLOAT
    elif self._resolve_always:
      return type_constants.STRING
    else:
      raise ValueError('Incompatible values cannot be resolved: '
                       '{}, {}'.format(first, second))

  def _resolve_number(self, first, second):
    if first == second:
      return first
    elif (self._is_bigquery_field_repeated(first) and
          self._is_bigquery_field_repeated(second)):
      # None implies arbitrary number of values.
      return '.'
    elif self._resolve_always:
      return '.'
    else:
      raise ValueError('Incompatible numbers cannot be resolved: '
                       '{}, {}'.format(first, second))

  def _is_bigquery_field_repeated(self, vcf_num):
    # type: (int) -> bool
    """Returns true if the corresponding field in bigquery schema is repeated.

    Args:
      vcf_num (int): value of field `Number` in VCF header.
    """
    if vcf_num in (0, 1):
      return False
    elif (vcf_num == vcf_parser.FIELD_COUNT_ALTERNATE_ALLELE and
          self._split_alternate_allele_info_fields):
      # info field with `Number=A` does not become a repeated field if flag
      # `split_alternate_allele_info_fields` is on.
      # See `variant_transform_options.py` for more details.
      return False
    else:
      return True
