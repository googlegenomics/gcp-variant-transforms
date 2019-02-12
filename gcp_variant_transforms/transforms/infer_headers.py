# Copyright 2019 Google Inc.  All Rights Reserved.
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

from typing import Iterable, List, Optional  # pylint: disable=unused-import

import apache_beam as beam


from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcfio  # pylint: disable=unused-import
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.libs import infer_headers_util

_FIELD_COUNT_ALTERNATE_ALLELE = 'A'

# Alias for the header key/type constants to make referencing easier.
_HeaderKeyConstants = vcf_header_io.VcfParserHeaderKeyConstants
_HeaderTypeConstants = vcf_header_io.VcfHeaderFieldTypeConstants


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

  def process(self,
              variant,  # type: vcfio.Variant
              defined_headers  # type: vcf_header_io.VcfHeader
             ):
    # type: (...) -> Iterable[vcf_header_io.VcfHeader]
    """
    Args:
      defined_headers: header fields defined in header section of VCF files.
    """
    infos = infer_headers_util.infer_info_fields(
        variant,
        defined_headers,
        self._infer_headers,
        self._annotation_fields_to_infer)
    formats = {}
    if self._infer_headers:
      formats = infer_headers_util.infer_format_fields(variant, defined_headers)
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
