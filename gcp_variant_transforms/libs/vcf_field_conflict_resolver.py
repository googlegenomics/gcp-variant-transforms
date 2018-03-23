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

import vcf


class VcfParserConstants(object):
  """Constants for type and number from VCF parser."""
  FLOAT = 'Float'
  INTEGER = 'Integer'
  STRING = 'String'
  FLAG = 'Flag'
  CHARACTER = 'Character'
  NUM = 'num'
  TYPE = 'type'


class FieldConflictResolver(object):
  """A class for resolving all VCF field related mismatches."""
  # TODO(nmousavi): explain this class more.

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

  def resolve(self, vcf_field_key, first_vcf_field_value,
              second_vcf_field_value):
    # type: (str, Union[str, int], Union[str, int]) -> Union[str, int]
    """Returns resolution for the conflicting field values.

    Args:
      vcf_field_key: field key in VCF header.
      first_vcf_field_value: first field value.
      second_vcf_field_value: second field value.
    Raises:
      ValueError: if the conflict cannot be resolved.
    """
    if vcf_field_key == VcfParserConstants.TYPE:
      return self._resolve_type(first_vcf_field_value, second_vcf_field_value)
    elif vcf_field_key == VcfParserConstants.NUM:
      return self._resolve_number(first_vcf_field_value, second_vcf_field_value)
    else:
      # We only care about conflicts in 'num' and 'type' fields.
      return first_vcf_field_value

  def _resolve_type(self, first, second):
    if first == second:
      return first
    elif (first in (VcfParserConstants.INTEGER, VcfParserConstants.FLOAT) and
          second in (VcfParserConstants.INTEGER, VcfParserConstants.FLOAT)):
      return VcfParserConstants.FLOAT
    elif self._resolve_always:
      return VcfParserConstants.STRING
    else:
      raise ValueError('Incompatible values cannot be resolved: '
                       '{}, {}'.format(first, second))

  def _resolve_number(self, first, second):
    if first == second:
      return first
    elif (self._is_bigquery_field_repeated(first) and
          self._is_bigquery_field_repeated(second)):
      # None implies arbitrary number of values.
      return None
    elif self._resolve_always:
      return None
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
    elif (vcf_num == vcf.parser.field_counts['A'] and
          self._split_alternate_allele_info_fields):
      # info field with `Number=A` does not become a repeated field if flag
      # `split_alternate_allele_info_fields` is on.
      # See `variant_transform_options.py` for more details.
      return False
    else:
      return True
