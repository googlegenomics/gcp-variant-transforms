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

"""beam combiner function for merging VCF file headers."""
from collections import OrderedDict
import apache_beam as beam
import vcf

from gcp_variant_transforms.beam_io import vcf_header_io

__all__ = ['MergeHeaders']

class _HeaderConflictResolver(object):
  """A class for resolving mistmatch between header fields (e.g. type, num).
  """

  def __init__(self, split_alternate_allele_info_fields=True):
    """Initialize the class.

    Args:
     split_alternate_allele_info_fields (bool): Whether INFO fields with
       `Number=A` are store under the alternate_bases record.
     """
    self._split_alternate_allele_info_fields = (
        split_alternate_allele_info_fields)

  def resolve(self, vcf_field_key, first_vcf_field_value,
              second_vcf_field_value):
    """Returns resolution for the conflicting field values.

    Args:
      vcf_field_key (str): field key in VCF header.
      first_vcf_field_value (int or str): first field value.
      second_vcf_field_value (int or str): second field value.
    Raises:
      ValueError: if the conflict cannot be resolved.
    """
    if vcf_field_key == 'type':
      return self._resolve_type(first_vcf_field_value, second_vcf_field_value)
    elif vcf_field_key == 'num':
      return self._resolve_number(first_vcf_field_value, second_vcf_field_value)
    else:
      # We only care about conflicts in 'num' and 'type' fields.
      return first_vcf_field_value

  def _resolve_type(self, first, second):
    if first == second:
      return first
    elif (first in ('Integer', 'Float') and second in ('Integer', 'Float')):
      return 'Float'
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
    else:
      raise ValueError('Incompatible numbers cannot be resolved: '
                       '{}, {}'.format(first, second))

  def _is_bigquery_field_repeated(self, vcf_num):
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


class _HeaderMerger(object):
  """Class for merging two :class:`VcfHeader`s."""

  def __init__(self, resolver):
    """Initialize :class:`VcfHeader` object.

    Args:
      resolver (:class:`_HeaderConflictResolver`): for resolving possible
        header value mistmatches.
    """
    self._resolver = resolver

  def merge(self, first, second):
    """Updates ``first``'s headers with values from ``second``.

    If a specific key does not already exist in a specific one of ``first``'s
    headers, that key and the associated value will be added. If the key does
    already exist in the specific header of ``first``, then the value of that
    key will be updated with the value from ``second``.

    Args:
      first (:class:`VcfHeader`): The VcfHeader object.
      second (:class:`VcfHeader`): The VcfHeader object that's headers will be
        merged into the headers of first.
    """
    if (not isinstance(first, vcf_header_io.VcfHeader) or
        not isinstance(first, vcf_header_io.VcfHeader)):
      raise NotImplementedError

    self._merge_header_fields(first.infos, second.infos)
    self._merge_header_fields(first.filters, second.filters)
    self._merge_header_fields(first.alts, second.alts)
    self._merge_header_fields(first.formats, second.formats)
    self._merge_header_fields(first.contigs, second.contigs)

  def _merge_header_fields(self, first, second):
    """Modifies ``first`` to add any keys from ``second`` not in ``first``.

    Args:
      first (dict): first header fields.
      second (dict): second header fields.
    Raises:
      ValueError: If the header fields are incompatible (e.g. same key with
        different types or numbers).
    """
    for second_key, second_value in second.iteritems():
      if second_key not in first:
        first[second_key] = second_value
        continue
      first_value = first[second_key]
      if first_value.keys() != second_value.keys():
        raise ValueError('Incompatible header fields: {}, {}'.format(
            first_value, second_value))
      merged_value = OrderedDict()
      for first_field_key, first_field_value in first_value.iteritems():
        second_field_value = second_value[first_field_key]
        try:
          resolution_field_value = self._resolver.resolve(
              first_field_key,
              first_field_value,
              second_field_value)
          merged_value.update({first_field_key: resolution_field_value})
        except ValueError as e:
          raise ValueError('Incompatible number or types in header fields:'
                           '{}, {} \n. Error: {}'.format(
                               first_value, second_value, str(e)))

      first[second_key] = merged_value

class _MergeHeadersFn(beam.CombineFn):
  """Combiner function for merging VCF file headers."""

  def __init__(self, header_merger):
    super(_MergeHeadersFn, self).__init__()
    self._header_merger = header_merger

  def create_accumulator(self):
    return vcf_header_io.VcfHeader()

  def add_input(self, source, to_merge):
    return self.merge_accumulators([source, to_merge])

  def merge_accumulators(self, accumulators):
    merged_headers = self.create_accumulator()
    for to_merge in accumulators:
      self._header_merger.merge(merged_headers, to_merge)
    return merged_headers

  def extract_output(self, merged_headers):
    return merged_headers


class MergeHeaders(beam.PTransform):
  """A PTransform to merge VCF file headers."""

  def __init__(self, split_alternate_allele_info_fields=True):
    """Initializes :class:`MergeHeaders` object.

    Args:
      split_alternate_allele_info_fields (bool): Whether INFO fields with
        `Number=A` are store under the alternate_bases record. This is relevant
        as it changes the header compatibility rules as it changes the schema.
    """
    super(MergeHeaders, self).__init__()
    self._header_merger = _HeaderMerger(
        _HeaderConflictResolver(split_alternate_allele_info_fields))

  def expand(self, pcoll):
    return pcoll | 'MergeHeaders' >> beam.CombineGlobally(_MergeHeadersFn(
        self._header_merger))
