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

__all__ = ['HeaderFields', 'get_vcf_headers']


# Stores parsed header information.
#   - infos: Stores all `##INFO` header lines. Type is a ``dict`` with values
#     equal to ``namedtuple`` defined in ``vcf.parser._Info``.
#   - formats: Stores all `##FORMAT` header lines. Type is a ``dict`` with
#     values equal to ``namedtuple`` defined in ``vcf.parser._Formats``.
HeaderFields = namedtuple('HeaderFields', ['infos', 'formats'])


def get_vcf_headers(input_file):
  """Returns VCF headers (FORMAT and INFO) from ``input_file``.

  Args:
    input_file (str): A string specifying the path to the representative VCF
    file, i.e., the VCF file that contains a header representative of all VCF
    files matching the input_pattern of the job. It can be local or remote (e.g.
    on GCS).
  Returns:
    ``HeaderFields`` specifying header info.
  Raises:
    ValueError: If ``input_file`` is not a valid VCF file (e.g. bad format,
    empty, non-existent).
  """
  if not FileSystems.exists(input_file):
    raise ValueError('VCF header does not exist')
  try:
    vcf_reader = vcf.Reader(fsock=_line_generator(input_file))
  except (SyntaxError, StopIteration) as e:
    raise ValueError('Invalid VCF header: %s' % str(e))
  return HeaderFields(vcf_reader.infos, vcf_reader.formats)


def _line_generator(file_name):
  """Generator to return lines delimited by newline chars from ``file_name``."""
  with FileSystems.open(file_name) as f:
    while True:
      line = f.readline()
      if line:
        yield line
      else:
        break
