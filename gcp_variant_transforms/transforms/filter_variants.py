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

"""A PTransform for filtering variants."""

from __future__ import absolute_import

from typing import Iterable  # pylint: disable=unused-import
import logging

import apache_beam as beam

from gcp_variant_transforms.beam_io import vcfio


class FilterVariants(beam.PTransform):
  """Filters variants according to input parameters."""

  def __init__(self, reference_names=None):
    # type: (List[str]) -> None
    """Initializes the transform.

    Args:
      reference_names : A list of reference_names that will be kept. If this
        parameter is None or empty all references will be kept.
    """
    self._reference_names = reference_names

  def _is_valid_record(self, variant):
    if isinstance(variant, vcfio.Variant):
      return True
    elif isinstance(variant, vcfio.MalformedVcfRecord):
      logging.warning('VCF record read failed in %s for line %s with error %s',
                      variant.file_name, variant.line, variant.error)
      return False
    else:
      raise ValueError(
          'Unexpected {} encountered while filtering variants.'.format(
              type(variant)))

  def _should_keep_reference_name(self, variant):
    return (not self._reference_names or
            variant.reference_name in self._reference_names)

  def _apply_filters(self, variant):
    if (self._is_valid_record(variant) and
        self._should_keep_reference_name(variant)):
      yield variant

  def expand(self, pcoll):
    return pcoll | 'ApplyFilters' >> beam.ParDo(self._apply_filters)


class ExtractMalformedVariants(beam.PTransform):
  """Extracts malformed variants from all variants."""

  def _apply_filters(self, variant):
    # type: (vcfio.Variant) -> Iterable[vcfio.Variant]
    if isinstance(variant, vcfio.MalformedVcfRecord):
      yield variant

  def expand(self, pcoll):
    return pcoll | 'ExtractMalformedVariants' >> beam.ParDo(self._apply_filters)
