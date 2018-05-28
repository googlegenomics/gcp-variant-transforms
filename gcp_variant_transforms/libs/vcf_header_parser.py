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

import vcf

from apache_beam.io.filesystems import FileSystems
from gcp_variant_transforms.beam_io import vcf_header_io


def get_vcf_headers(input_file):
  # type: (str) -> vcf_header_io.VcfHeader
  """Returns VCF headers from ``input_file``.

  Args:
    input_file (str): A string specifying the path to the representative VCF
      file, i.e., the VCF file that contains a header representative of all VCF
      files matching the input_pattern of the job. It can be local or
      remote (e.g. on GCS).
  Returns:
    VCF header info.
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
  return vcf_header_io.VcfHeader(infos=vcf_reader.infos,
                                 filters=vcf_reader.filters,
                                 alts=vcf_reader.alts,
                                 formats=vcf_reader.formats,
                                 contigs=vcf_reader.contigs,
                                 file_name=input_file)


def get_metadata_header_lines(input_file):
  # type: (str) -> List[str]
  """Returns header lines from the given VCF file ``input_file``.

  Only returns lines starting with ## and not #.

  Args:
    input_file: A string specifying the path to a VCF file.
      It can be local or remote (e.g. on GCS).
  Returns:
    A list containing header lines of ``input_file``.
  Raises:
    ValueError: If ``input_file`` does not exist.
  """
  if not FileSystems.exists(input_file):
    raise ValueError('{} does not exist'.format(input_file))
  return [line for line in _line_generator(input_file) if line.startswith('##')]


def _line_generator(file_name):
  """Generator to return lines delimited by newline chars from ``file_name``."""
  with FileSystems.open(file_name) as f:
    while True:
      line = f.readline()
      if line:
        yield line
      else:
        break
