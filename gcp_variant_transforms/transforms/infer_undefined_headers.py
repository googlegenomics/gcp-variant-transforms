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

"""A PTransform to infer undefined header fields."""

from __future__ import absolute_import

import apache_beam as beam

from vcf.parser import _Format as Format
from vcf.parser import _Info as Info
from vcf.parser import field_counts

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.transforms import merge_headers



class _InferUndefinedHeaderFields(beam.DoFn):
  """Extracts undefined header fields from ``Variant`` record."""

  def _get_field_count(self, field_value):
    """
    Args:
      field_value (list, bool, integer, or string): value for the field
        returned by PyVCF. E.g. [0.33, 0.66] is a field value for Allele
        frequency (AF) field.
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

  def _infer_undefined_info_fields(self, variant, defined_headers):
    """Returns info fields not defined in the headers.

    Args:
      variant (:class:`vcfio.Variant`): variant obj.
      defined_headers (:class:`vcf_header_io.VcfHeader`): header fields defined
        in header section of VCF files.
    Returns:
      A dict of (info_key(str), :class:`Info`) for any info field in `variant`
      that is not defined in the header.
    """
    infos = {}
    for info_field_key, variant_info in variant.info.iteritems():
      info_field_value = variant_info.data
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
    return infos

  def _infer_undefined_format_fields(self, variant, defined_headers):
    """Returns format fields not defined in the headers.

    Args:
      variant (:class:`vcfio.Variant`): variant obj.
      defined_headers (:class:`vcf_header_io.VcfHeader`): header fields defined
        in header section of VCF files.
    Returns:
      A dict of (format_key(str), :class:`Format`) for any format key in
      `variant` that is not defined in the header.
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
                                       '') # NO_DESCRIPTION
      # No point in proceeding. All other calls have the same FORMAT.
      break
    return formats

  def process(self, variant, defined_headers):
    """
    Args:
      defined_headers (:class:`vcf_header_io.VcfHeader`): header fields defined
        in header section of VCF files.
    """
    infos = self._infer_undefined_info_fields(variant, defined_headers)
    formats = self._infer_undefined_format_fields(variant, defined_headers)
    if infos or formats:
      yield vcf_header_io.VcfHeader(infos=infos, formats=formats)


class InferUndefinedHeaderFields(beam.PTransform):
  """Extracts undefined header fields from ``Variant`` records."""

  def __init__(self, defined_headers):
    """Initializes the transform.
    Args:
      defined_headers (:class:`vcf_header_io.VCFHeader`): Side input
        containing all the header fields (e.g., INFO, FORMAT) defined
        in the header section of the VCF files. This is used to skip already
        defined header fields.
    """
    self._defined_headers = defined_headers

  def expand(self, pcoll):
    return (pcoll
            | 'InferUndefinedHeaderFields' >> beam.ParDo(
                _InferUndefinedHeaderFields(), self._defined_headers)
            # TODO(nmousavi): Modify the MergeHeaders to resolve 1 vs '.'
            # mistmatch for headers extracted from variants.
            #
            # Note: argument `split_alternate_allele_info_fileds` is not
            # relevant here since no fields with `Number=A` will be extracted
            # from variants, therefore we let the default value (True) for it
            # be used. Should this changes, we should modify the default value.
            | 'MergeHeaders' >> merge_headers.MergeHeaders(
                split_alternate_allele_info_fields=True))
