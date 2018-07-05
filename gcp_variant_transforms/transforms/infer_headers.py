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

"""A PTransform to infer undefined/mismatched header fields."""

from __future__ import absolute_import

from typing import Any, Dict, Iterable, List, Optional, Union  # pylint: disable=unused-import

import apache_beam as beam

from vcf.parser import _Format as Format
from vcf.parser import _Info as Info
from vcf.parser import field_counts

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcfio  # pylint: disable=unused-import
from gcp_variant_transforms.transforms import merge_headers

_FIELD_COUNT_ALTERNATE_ALLELE = 'A'

# Alias for the header key/type constants to make referencing easier.
_HeaderKeyConstants = vcf_header_io.VcfParserHeaderKeyConstants
_HeaderTypeConstants = vcf_header_io.VcfHeaderFieldTypeConstants


class _InferHeaderFields(beam.DoFn):
  """Infers header fields from `Variant` records.

  Two types of fields are inferred:
  - The fields are undefined in the headers.
  - The field definitions provided by the headers does not match the field
    values.
  """

  def _get_field_count(self, field_value):
    # type: (Union[List, bool, int, str]) -> Optional[int]
    """
    Args:
      field_value: value for the field returned by PyVCF. E.g. [0.33, 0.66] is a
        field value for Allele frequency (AF) field.
    """
    if isinstance(field_value, list):
      return field_counts['.']
    elif isinstance(field_value, bool):
      return 0
    else:
      return 1

  def _get_field_type(self, field_value):
    """
    Args:
      field_value (list, bool, integer, or string): value for the field
        returned by PyVCF. E.g. [0.33, 0.66] is a field value for Allele
        frequency (AF) field.
    """
    if isinstance(field_value, list):
      return (self._get_field_type(field_value[0]) if field_value else
              vcf_header_io.VcfHeaderFieldTypeConstants.STRING)

    if isinstance(field_value, bool):
      return vcf_header_io.VcfHeaderFieldTypeConstants.FLAG
    elif isinstance(field_value, int):
      return vcf_header_io.VcfHeaderFieldTypeConstants.INTEGER
    elif isinstance(field_value, float):
      return vcf_header_io.VcfHeaderFieldTypeConstants.FLOAT
    elif self._can_cast_to(field_value, int):
      return vcf_header_io.VcfHeaderFieldTypeConstants.INTEGER
    elif self._can_cast_to(field_value, float):
      return vcf_header_io.VcfHeaderFieldTypeConstants.FLOAT
    else:
      return vcf_header_io.VcfHeaderFieldTypeConstants.STRING

  def _can_cast_to(self, value, cast_type):
    """Returns true if `value` can be casted to type `type`"""
    try:
      _ = cast_type(value)
      return True
    except ValueError:
      return False

  def _get_corrected_type(self, defined_type, value):
    # type: (str, Any) -> str
    """Returns the corrected type according to `defined_type` and `value`.

    It handles one special case PyVCF cannot handle, i.e., the defined type is
    `Integer`, but the provided value is float. In this case, correct the type
    to be `Float`.

    Note that if `value` is a float instance but with an integer value
    (e.g. 2.0), the type will stay the same as `defined_type`.
    """
    if defined_type == _HeaderTypeConstants.INTEGER:
      if isinstance(value, float) and not value.is_integer():
        return _HeaderTypeConstants.FLOAT
      if isinstance(value, list):
        for item in value:
          corrected_type = self._get_corrected_type(defined_type, item)
          if corrected_type != defined_type:
            return corrected_type
    return defined_type

  def _infer_mismatched_info_field(self,
                                   field_key,  # type: str
                                   field_value,  # type: Any
                                   defined_header,  # type: Dict
                                   num_alternate_bases  # type: int
                                  ):
    # type: (...) -> Optional[Info]
    """Returns corrected info if there are mismatches.

    Two mismatches are handled:
    - Defined num is `A`, but the provided values do not have the same
      cardinality as the alternate bases. Correct the num to be `None`.
    - Defined type is `Integer`, but the provided value is float. Correct the
      type to be `Float`.
    Args:
      field_key: the info field key.
      field_value: the value of the field key given in the variant.
      defined_header: The definition of `field_key` in the header.
      num_alternate_bases: number of the alternate bases.
    Returns:
      Corrected info definition if there are mismatches.
    """
    corrected_num = defined_header.get(_HeaderKeyConstants.NUM)
    if (corrected_num == field_counts[_FIELD_COUNT_ALTERNATE_ALLELE] and
        len(field_value) != num_alternate_bases):
      corrected_num = field_counts['.']

    corrected_type = self._get_corrected_type(
        defined_header.get(_HeaderKeyConstants.TYPE), field_value)

    if (corrected_type != defined_header.get(_HeaderKeyConstants.TYPE) or
        corrected_num != defined_header.get(_HeaderKeyConstants.NUM)):
      return Info(field_key,
                  corrected_num,
                  corrected_type,
                  defined_header.get(_HeaderKeyConstants.DESC),
                  defined_header.get(_HeaderKeyConstants.SOURCE),
                  defined_header.get(_HeaderKeyConstants.VERSION))
    return None

  def _infer_mismatched_format_field(self,
                                     field_key,  # type: str
                                     field_value,  # type: Any
                                     defined_header  # type: Dict
                                    ):
    # type: (...) -> Optional[Format]
    """Returns corrected format if there are mismatches.

    One type of mismatches is handled:
    - Defined type is `Integer`, but the provided value is float. Correct the
      type to be `Float`.
    Args:
      field_key: the format field key.
      field_value: the value of the field key given in the variant.
      defined_header: The definition of `field_key` in the header.
    Returns:
      Corrected format definition if there are mismatches.
    """
    corrected_type = self._get_corrected_type(
        defined_header.get(_HeaderKeyConstants.TYPE), field_value)
    if corrected_type != defined_header.get(_HeaderKeyConstants.TYPE):
      return Format(field_key,
                    defined_header.get(_HeaderKeyConstants.NUM),
                    corrected_type,
                    defined_header.get(_HeaderKeyConstants.DESC))
    return None

  def _infer_info_fields(self, variant, defined_headers):
    # type: (vcfio.Variant, vcf_header_io.VcfHeader) -> Dict[str, Info]
    """Returns inferred info fields.

    Two types of info fields are inferred:
    - The info fields are undefined in the headers.
    - The info fields' definitions provided by the header does not match the
      field value.
    Args:
      variant: variant obj.
      defined_headers: header fields defined in header section of VCF files.
    Returns:
      A dict of (info_key(str), :class:`Info`) for any info field in `variant`
      that is not defined in the header or the definition mismatches the field
      values.
    """
    infos = {}
    for info_field_key, info_field_value in variant.info.iteritems():
      if not defined_headers or info_field_key not in defined_headers.infos:
        if info_field_key in infos:
          raise ValueError(
              'Invalid VCF file. Duplicate INFO field in variant {}'.format(
                  variant))
        infos[info_field_key] = Info(info_field_key,
                                     self._get_field_count(info_field_value),
                                     self._get_field_type(info_field_value),
                                     '',  # NO_DESCRIPTION
                                     '',  # UNKNOWN_SOURCE
                                     '')  # UNKNOWN_VERSION
      else:
        corrected_info = self._infer_mismatched_info_field(
            info_field_key, info_field_value,
            defined_headers.infos.get(info_field_key),
            len(variant.alternate_bases))
        if corrected_info:
          infos[info_field_key] = corrected_info
    return infos

  def _infer_format_fields(self, variant, defined_headers):
    # type: (vcfio.Variant, vcf_header_io.VcfHeader) -> Dict[str, Format]
    """Returns inferred format fields.

    Two types of format fields are inferred:
    - The format fields are undefined in the headers.
    - The format definition provided by the headers does not match the field
      values.
    Args:
      variant: variant obj.
      defined_headers: header fields defined in header section of VCF files.
    Returns:
      A dict of (format_key(str), :class:`Format`) for any format key in
      `variant` that is not defined in the header or the definition mismatches
      the field values.
    """
    formats = {}
    for call in variant.calls:
      for format_key, format_value in call.info.iteritems():
        if not defined_headers or format_key not in defined_headers.formats:
          if format_key in formats:
            raise ValueError(
                'Invalid VCF file. Duplicate FORMAT field in variant {}'.format(
                    variant))
          formats[format_key] = Format(format_key,
                                       self._get_field_count(format_value),
                                       self._get_field_type(format_value),
                                       '')  # NO_DESCRIPTION
      # No point in proceeding. All other calls have the same FORMAT.
      break
    for call in variant.calls:
      for format_key, format_value in call.info.iteritems():
        if defined_headers and format_key in defined_headers.formats:
          corrected_format = self._infer_mismatched_format_field(
              format_key, format_value, defined_headers.formats.get(format_key))
          if corrected_format:
            formats[format_key] = corrected_format
    return formats

  def process(self,
              variant,  # type: vcfio.Variant
              defined_headers  # type: vcf_header_io.VcfHeader
             ):
    # type: (...) -> Iterable[vcf_header_io.VcfHeader]
    """
    Args:
      defined_headers: header fields defined in header section of VCF files.
    """
    infos = self._infer_info_fields(variant, defined_headers)
    formats = self._infer_format_fields(variant, defined_headers)
    yield vcf_header_io.VcfHeader(infos=infos, formats=formats)


class InferHeaderFields(beam.PTransform):
  """Extracts inferred header fields from `Variant` records."""

  def __init__(self, defined_headers, allow_incompatible_records=False):
    # type: (vcf_header_io.VcfHeader, bool) -> None
    """Initializes the transform.
    Args:
      defined_headers: Side input containing all the header fields (e.g., INFO,
        FORMAT) defined in the header section of the VCF files. This is used to
        skip already defined header fields when infer undefined header fields.
        Also, it is used to find and further infer the fields with mismatched
        definition and value.
      allow_incompatible_records: If true, header definition with type mismatch
        (e.g., string vs float) are always resolved.
    """
    self._defined_headers = defined_headers
    self._allow_incompatible_records = allow_incompatible_records

  def expand(self, pcoll):
    return (pcoll
            | 'InferHeaderFields' >> beam.ParDo(
                _InferHeaderFields(), self._defined_headers)
            # TODO(nmousavi): Modify the MergeHeaders to resolve 1 vs '.'
            # mistmatch for headers extracted from variants.
            #
            # Note: argument `split_alternate_allele_info_fileds` is not
            # relevant here since no fields with `Number=A` will be extracted
            # from variants, therefore we let the default value (True) for it
            # be used. Should this changes, we should modify the default value.
            | 'MergeHeaders' >> merge_headers.MergeHeaders(
                split_alternate_allele_info_fields=True,
                allow_incompatible_records=self._allow_incompatible_records))
