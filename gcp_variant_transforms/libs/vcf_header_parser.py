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

"""Helper library for reading VCF headers from multiple files."""

from __future__ import absolute_import

from collections import namedtuple
from apache_beam.io.filesystems import FileSystems

import vcf

__all__ = ['HeaderFields', 'get_merged_vcf_headers']


# Stores parsed header information.
#   - infos: Stores all `##INFO` header lines. Type is a ``dict`` with values
#     equal to ``namedtuple`` defined in ``vcf.parser._Info``.
#   - formats: Stores all `##FORMAT` header lines. Type is a ``dict`` with
#     values equal to ``namedtuple`` defined in ``vcf.parser._Formats``.
HeaderFields = namedtuple('HeaderFields', ['infos', 'formats'])


def get_merged_vcf_headers(input_pattern):
  """Returns merged VCF headers (FORMAT and INFO) from ``input_pattern``.

  If multiple files are specified by ``input_pattern`` then header fields will
  be merged by key and only one will be chosen as representative (in no
  particular order). If the same key is used in multiple files, then all types
  and numbers must be the same.

  Args:
    input_pattern (str): A string specifying the path to VCF input file(s). They
      can be local or remote (e.g. on GCS).
  Returns:
    `HeaderFields`` specifying merged header info from all files in
    ``input_pattern``.
  Raises:
    ValueError: If the VCF headers are invalid or incompatible (e.g. the same
      key if defined with different types in multiple files).
  """
  merged_info_fields = {}
  merged_format_fields = {}
  # The match implementation supports one pattern, but the API technically
  # supports multiple patterns, hence the need for a list (in both request
  # and response).
  match_results = FileSystems.match([input_pattern])
  if not match_results:
    return HeaderFields(merged_info_fields, merged_format_fields)
  for file_metadata in match_results[0].metadata_list:
    file_name = file_metadata.path
    if not file_metadata.size_in_bytes:
      continue  # Ignore empty files.
    try:
      vcf_reader = vcf.Reader(fsock=_line_generator(file_name))
    except SyntaxError as e:
      raise ValueError('Invalid VCF header: %s' % str(e))
    _merge_header_fields(merged_info_fields, vcf_reader.infos)
    _merge_header_fields(merged_format_fields, vcf_reader.formats)
  return HeaderFields(merged_info_fields, merged_format_fields)


def _merge_header_fields(source, to_merge):
  """Modifies ``source`` to add any keys from ``to_merge`` not in ``source``.

  Args:
    source (dict): Source header fields.
    to_merge (dict): Header fields to merge with ``source``.
  Raises:
    ValueError: If the header fields are incompatible (e.g. same key with
      different types or numbers).
  """
  if not to_merge:
    return source
  for key, to_merge_value in to_merge.iteritems():
    if key not in source:
      source[key] = to_merge_value
    else:
      source_value = source[key]
      if (source_value.num != to_merge_value.num or
          source_value.type != to_merge_value.type):
        raise ValueError(
            'Incompatible number of types in header fields: %s, %s' % (
                source_value, to_merge_value))


def _line_generator(file_name):
  """Generator to return lines delimited by newline chars from ``file_name``."""
  with FileSystems.open(file_name) as f:
    while True:
      line = f.readline()
      if line:
        yield line
      else:
        break
