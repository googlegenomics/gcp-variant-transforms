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

from pysam import libcbcf

from apache_beam.io.filesystems import FileSystems
from gcp_variant_transforms.beam_io import vcf_header_io

def get_vcf_headers(input_file):

  if not FileSystems.exists(input_file):
    raise ValueError('VCF header does not exist')
  header = libcbcf.VariantHeader()
  lines = _line_generator(input_file)
  sample_line = None
  header.add_line('##fileformat=VCFv4.0\n')
  file_empty = True
  for line in lines:
    if line.startswith('##'):
      header.add_line(line.strip())
      file_empty = False
    elif line.startswith('#'):
      sample_line = line.strip()
      file_empty = False
    elif line:
      if not sample_line:
        raise ValueError('Header line is missing')
    else:
      if file_empty:
        raise ValueError('File is empty')
      if not sample_line:
        sample_line = vcf_header_io.LAST_HEADER_LINE_PREFIX

  return vcf_header_io.VcfHeader(infos=header.info,
                                 filters=header.filters,
                                 alts=header.alts,
                                 formats=header.formats,
                                 contigs=header.contigs,
                                 samples=sample_line,
                                 file_path=input_file)


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
    record = None
    while True:
      record = f.readline()
      while record and not record.strip():  # Skip empty lines.
        record = f.readline()
      if record and record.startswith('#'):
        yield record
      else:
        break
    yield record
