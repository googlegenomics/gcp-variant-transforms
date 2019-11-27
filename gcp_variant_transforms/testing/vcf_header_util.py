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

"""Utility functions for creating VcfHeader objects used by unit tests."""

from __future__ import absolute_import

from gcp_variant_transforms.beam_io import vcf_header_io


def make_header(header_num_dict):
  # type: (Dict[str, str]) -> VcfHeader
  """Builds a VcfHeader based on the header_num_dict.

  Args:
    header_num_dict: a dictionary mapping info keys to string num values.
  """
  infos = {}
  for k, v in header_num_dict.iteritems():
    infos[k] = vcf_header_io.CreateInfoField(k, v, '.', '')
  return vcf_header_io.VcfHeader(infos=infos)
