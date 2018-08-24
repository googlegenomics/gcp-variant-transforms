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

import logging
from typing import Any, Dict, Iterable, List, Optional, Union  # pylint: disable=unused-import

import apache_beam as beam

from vcf.parser import _Format as Format
from vcf.parser import _Info as Info
from vcf.parser import field_counts

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcfio  # pylint: disable=unused-import
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.libs.annotation import annotation_parser
from gcp_variant_transforms.libs import vcf_field_conflict_resolver

_FIELD_COUNT_ALTERNATE_ALLELE = 'A'

#Filled with annotation field and name data, then used as a header ID
_BASE_ANNOTATION_TYPE_KEY = '{}_{}_TYPE'

# Alias for the header key/type constants to make referencing easier.
_HeaderKeyConstants = vcf_header_io.VcfParserHeaderKeyConstants
_HeaderTypeConstants = vcf_header_io.VcfHeaderFieldTypeConstants


def get_inferred_annotation_type_header_key(annot_field, name):
  return _BASE_ANNOTATION_TYPE_KEY.format(annot_field, name)


class _InferHeaderFields(beam.DoFn):
  """Infers header fields from `Variant` records.

  Up to three types of fields are inferred:

  if `infer_headers` is True:
  - The fields are undefined in the headers.
  - The field definitions provided by the headers does not match the field
    values.
  if `infer_annotation_types` is True:
  - Fields containing type information of corresponding annotation Info fields.
  """

  def __init__(
      self,
      infer_headers=False,  # type: bool
      annotation_fields_to_infer=None,  # type: Optional[List[str]]
      ):
    """
    Args:
      infer_headers: If true, header fields are inferred from variant data.
      annotation_fields_to_infer: list of info fields treated as annotation
        fields (e.g. ['CSQ', 'CSQ_VT']).
    """
    # type: (...) -> None
    if annotation_fields_to_infer is None:
      annotation_fields_to_infer = []
    self._annotation_fields_to_infer = annotation_fields_to_infer
    self._infer_headers = infer_headers

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

  def _infer_standard_info_fields(self, variant, infos, defined_headers):
    # type: (vcfio.Variant, Dict[str, Info], vcf_header_io.VcfHeader) -> None
    """Updates `infos` with inferred info fields.

    Three types of info fields are inferred:
    - The info fields are undefined in the headers.
    - The info fields' definitions provided by the header does not match the
      field value.
    Args:
      variant: variant obj.
      infos: dict of (info_key, `Info`) for any info field in
        `variant` that is not defined in the header or the definition mismatches
        the field values.
      defined_headers: header fields defined in header section of VCF files.
    """
    for info_field_key, info_field_value in variant.info.iteritems():
      if not defined_headers or info_field_key not in defined_headers.infos:
        if info_field_key in infos:
          raise ValueError(
              'Duplicate INFO field "{}" in variant "{}"'.format(
                  info_field_key, variant))
        logging.warning('Undefined INFO field "%s" in variant "%s"',
                        info_field_key, str(variant))
        infos[info_field_key] = Info(info_field_key,
                                     self._get_field_count(info_field_value),
                                     self._get_field_type(info_field_value),
                                     '',  # NO_DESCRIPTION
                                     '',  # UNKNOWN_SOURCE
                                     '')  # UNKNOWN_VERSION
      else:
        defined_header = defined_headers.infos.get(info_field_key)
        corrected_info = self._infer_mismatched_info_field(
            info_field_key, info_field_value,
            defined_header, len(variant.alternate_bases))
        if corrected_info:
          logging.warning(
              'Incorrect INFO field "%s". Defined as "type=%s,num=%s", '
              'got "%s", in variant "%s"',
              info_field_key, defined_header.get(_HeaderKeyConstants.TYPE),
              str(defined_header.get(_HeaderKeyConstants.NUM)),
              str(info_field_value), str(variant))
          infos[info_field_key] = corrected_info

  def _infer_annotation_type_info_fields(self, variant, infos, defined_headers):
    # type: (vcfio.Variant, Dict[str, Info], vcf_header_io.VcfHeader) -> None
    """Updates `infos` with inferred annotation type info fields.

    All annotation headers in each annotation field are converted to Info header
    lines where the new ID corresponds to the given annotation field and header,
    and the new TYPE corresponds to inferred type of the original header. Since
    each variant potentially contains multiple values for each annotation
    header, a small 'merge' of value types is performed before VcfHeader
    creation for each variant.
    Args:
      variant: variant obj.
      infos: dict of (info_key, `Info`) for any info field in
        `variant` that is not defined in the header or the definition mismatches
        the field values.
      defined_headers: header fields defined in header section of VCF files.
    """

    def _check_annotation_lists_lengths(names, values):
      lengths = set(len(v) for v in values)
      lengths.add(len(names))
      if len(lengths) != 1:
        error = ('Annotation lists have inconsistent lengths: {}.\nnames={}\n'
                 'values={}').format(lengths, names, values)
        raise ValueError(error)

    resolver = vcf_field_conflict_resolver.FieldConflictResolver(
        resolve_always=True)
    for field in self._annotation_fields_to_infer:
      if field not in variant.info:
        continue
      annotation_names = annotation_parser.extract_annotation_names(
          defined_headers.infos[field][_HeaderKeyConstants.DESC])
      # First element (ALT) is ignored, since its type is hard-coded as string
      annotation_values = [annotation_parser.extract_annotation_list_with_alt(
          annotation)[1:] for annotation in variant.info[field]]
      _check_annotation_lists_lengths(annotation_names, annotation_values)
      annotation_values = zip(*annotation_values)
      for name, values in zip(annotation_names, annotation_values):
        variant_merged_type = _HeaderTypeConstants.INTEGER
        for v in values:
          if not v:
            continue
          variant_merged_type = resolver.resolve_attribute_conflict(
              _HeaderKeyConstants.TYPE,
              variant_merged_type,
              self._get_field_type(v))
          if variant_merged_type == _HeaderTypeConstants.STRING:
            break
        key_id = get_inferred_annotation_type_header_key(field, name)
        infos[key_id] = Info(key_id,
                             1,  # field count
                             variant_merged_type,
                             ('Inferred type field for annotation {}.'.format(
                                 name)),
                             '',  # UNKNOWN_SOURCE
                             '')  # UNKNOWN_VERSION

  def _infer_info_fields(self, variant, defined_headers):
    """Returns inferred info fields.

    Up to three types of info fields are inferred:

    if `infer_headers` is True:
    - The info fields are undefined in the headers.
    - The info fields' definitions provided by the header does not match the
      field value.
    if `infer_annotation_types` is True:
    - Fields containing type information of corresponding annotation Info
      fields.

    Args:
      variant: variant obj.
      defined_headers: header fields defined in header section of VCF files.
    Returns:
      infos: dict of (info_key, `Info`) for any info field in
        `variant` that is not defined in the header or the definition mismatches
        the field values.
    """
    infos = {}
    if self._infer_headers:
      self._infer_standard_info_fields(variant, infos, defined_headers)
    if self._annotation_fields_to_infer:
      self._infer_annotation_type_info_fields(variant, infos, defined_headers)
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
      A dict of (format_key, `Format`) for any format key in
      `variant` that is not defined in the header or the definition mismatches
      the field values.
    """
    formats = {}
    for call in variant.calls:
      for format_key, format_value in call.info.iteritems():
        if not defined_headers or format_key not in defined_headers.formats:
          if format_key in formats:
            raise ValueError(
                'Duplicate FORMAT field "{}" in variant "{}"'.format(
                    format_key, variant))
          logging.warning('Undefined FORMAT field "%s" in variant "%s"',
                          format_key, str(variant))
          formats[format_key] = Format(format_key,
                                       self._get_field_count(format_value),
                                       self._get_field_type(format_value),
                                       '')  # NO_DESCRIPTION
      # No point in proceeding. All other calls have the same FORMAT.
      break
    for call in variant.calls:
      for format_key, format_value in call.info.iteritems():
        if defined_headers and format_key in defined_headers.formats:
          defined_header = defined_headers.formats.get(format_key)
          corrected_format = self._infer_mismatched_format_field(
              format_key, format_value, defined_header)
          if corrected_format:
            logging.warning(
                'Incorrect FORMAT field "%s". Defined as "type=%s,num=%s", '
                'got "%s" in variant "%s"',
                format_key, defined_header.get(_HeaderKeyConstants.TYPE),
                str(defined_header.get(_HeaderKeyConstants.NUM)),
                str(format_value), str(variant))
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
    formats = {}
    if self._infer_headers:
      formats = self._infer_format_fields(variant, defined_headers)
    yield vcf_header_io.VcfHeader(infos=infos, formats=formats)


class InferHeaderFields(beam.PTransform):
  """Extracts inferred header fields from `Variant` records."""

  def __init__(
      self,
      defined_headers,  # type: Optional[vcf_header_io.VcfHeader]
      allow_incompatible_records=False,  # type: bool
      infer_headers=False,  # type: bool
      annotation_fields_to_infer=None  # type: Optional[List[str]]
      ):
    """Initializes the transform.
    Args:
      defined_headers: Side input containing all the header fields (e.g., INFO,
        FORMAT) defined in the header section of the VCF files. This is used to
        skip already defined header fields when infer undefined header fields.
        Also, it is used to find and further infer the fields with mismatched
        definition and value.
      annotation_fields_to_infer: list of info fields treated as annotation
        fields (e.g. ['CSQ', 'CSQ_VT'])
      allow_incompatible_records: If true, header definition with type mismatch
        (e.g., string vs float) are always resolved.
      infer_headers: If true, header fields are inferred from variant data.
    """
    self._defined_headers = defined_headers
    self._annotation_fields_to_infer = annotation_fields_to_infer
    self._allow_incompatible_records = allow_incompatible_records
    self._infer_headers = infer_headers

  def expand(self, pcoll):
    return (pcoll
            | 'InferHeaderFields' >> beam.ParDo(
                _InferHeaderFields(self._infer_headers,
                                   self._annotation_fields_to_infer),
                self._defined_headers)
            # TODO(nmousavi): Modify the MergeHeaders to resolve 1 vs '.'
            # mismatch for headers extracted from variants.
            #
            # Note: argument `split_alternate_allele_info_fields` is not
            # relevant here since no fields with `Number=A` will be extracted
            # from variants, therefore we let the default value (True) for it
            # be used. Should this changes, we should modify the default value.
            | 'MergeHeaders' >> merge_headers.MergeHeaders(
                split_alternate_allele_info_fields=True,
                allow_incompatible_records=(
                    self._allow_incompatible_records or
                    bool(self._annotation_fields_to_infer))))
