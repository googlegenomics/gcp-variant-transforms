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

"""A source for reading VCF file headers."""

from __future__ import absolute_import

from collections import OrderedDict
from functools import partial
from typing import Dict  # pylint: disable=unused-import
import vcf

import apache_beam as beam
from apache_beam.io import filebasedsource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform

from gcp_variant_transforms.beam_io import vcfio


class VcfHeaderFieldTypeConstants(object):
  """Constants for types from VCF header."""
  FLOAT = 'Float'
  INTEGER = 'Integer'
  STRING = 'String'
  FLAG = 'Flag'
  CHARACTER = 'Character'
  STRING = 'String'


class VcfParserHeaderKeyConstants(object):
  """Constants for header fields from the parser (currently PyVCF)."""
  ID = 'id'
  NUM = 'num'
  TYPE = 'type'
  DESC = 'desc'
  SOURCE = 'source'
  VERSION = 'version'
  LENGTH = 'length'


class VcfHeader(object):
  """Container for header data."""

  def __init__(self,
               infos=None,  # type: Dict[str, OrderedDict[vcf.parser._Info]]
               filters=None,  # type: Dict[str, OrderedDict[vcf.parser._Filter]]
               alts=None,  # type: Dict[str, OrderedDict[vcf.parser._Alt]]
               formats=None,  # type: Dict[str, OrderedDict[vcf.parser._Format]]
               contigs=None,  # type: Dict[str, OrderedDict[vcf.parser._Contig]]
               file_name=None  # type: str
              ):
    """Initializes a VcfHeader object.

    It keeps the order of values in the input dictionaries. Order is important
    in some fields like `contigs` and for ensuring order is unchanged in
    VCF->VCF pipelines.

    Args:
      infos: A dictionary mapping info keys to vcf info metadata values.
      filters: A dictionary mapping filter keys to vcf filter metadata values.
      alts: A dictionary mapping alt keys to vcf alt metadata values.
      formats: A dictionary mapping format keys to vcf format metadata values.
      contigs: A dictionary mapping contig keys to vcf contig metadata values.
      file_name: The file name of the vcf file.
    """
    self.infos = self._values_asdict(infos or {})
    self.filters = self._values_asdict(filters or {})
    self.alts = self._values_asdict(alts or {})
    self.formats = self._values_asdict(formats or {})
    self.contigs = self._values_asdict(contigs or {})
    self.file_name = file_name

  def __eq__(self, other):
    return (self.infos == other.infos and
            self.filters == other.filters and
            self.alts == other.alts and
            self.formats == other.formats and
            self.contigs == other.contigs)

  def __repr__(self):
    return ', '.join([str(header) for header in [self.infos,
                                                 self.filters,
                                                 self.alts,
                                                 self.formats,
                                                 self.contigs]])

  def _values_asdict(self, header):
    """Converts PyVCF header values to ordered dictionaries."""
    ordered_dict = OrderedDict()
    for key in header:
      # These methods were not designed to be protected. They start with an
      # underscore to avoid conflicts with field names. For more info, see
      # https://docs.python.org/2/library/collections.html#collections.namedtuple
      ordered_dict[key] = header[key]._asdict()  # pylint: disable=W0212
    return ordered_dict


class VcfHeaderSource(filebasedsource.FileBasedSource):
  """A source for reading VCF file headers.

  Parses VCF files (version 4) using PyVCF library.
  """

  def __init__(self,
               file_pattern,
               compression_type=CompressionTypes.AUTO,
               validate=True):
    super(VcfHeaderSource, self).__init__(file_pattern,
                                          compression_type=compression_type,
                                          validate=validate,
                                          splittable=False)
    self._compression_type = compression_type

  def read_records(self, file_name, unused_range_tracker):
    try:
      vcf_reader = vcf.Reader(fsock=self._read_headers(file_name))
    except StopIteration:
      raise ValueError('{} has no header.'.format(file_name))

    yield VcfHeader(infos=vcf_reader.infos,
                    filters=vcf_reader.filters,
                    alts=vcf_reader.alts,
                    formats=vcf_reader.formats,
                    contigs=vcf_reader.contigs,
                    file_name=file_name)

  def _read_headers(self, file_name):
    with FileSystems.open(
        file_name, compression_type=self._compression_type) as file_to_read:
      while True:
        record = file_to_read.readline()
        if record and record.startswith('#'):
          yield record
        else:
          break


class ReadVcfHeaders(PTransform):
  """A PTransform for reading the header lines of VCF files.

  Parses VCF files (version 4) using PyVCF library.
  """

  def __init__(
      self,
      file_pattern,
      compression_type=CompressionTypes.AUTO,
      validate=True,
      **kwargs):
    """Initialize the :class:`ReadVcfHeaders` transform.

    Args:
      file_pattern (str): The file path to read from either as a single file or
        a glob pattern.
      compression_type (str): Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      validate (bool): flag to verify that the files exist during the pipeline
        creation time.
    """
    super(ReadVcfHeaders, self).__init__(**kwargs)
    self._source = VcfHeaderSource(
        file_pattern, compression_type, validate=validate)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)


def _create_vcf_header_source(file_pattern=None, compression_type=None):
  return VcfHeaderSource(file_pattern=file_pattern,
                         compression_type=compression_type)


class ReadAllVcfHeaders(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading the
  header lines of :class:`~apache_beam.pvalue.PCollection` of VCF files.

  Reads a :class:`~apache_beam.pvalue.PCollection` of VCF files or file patterns
  and produces a PCollection :class:`VcfHeader` objects.

  This transform should be used when reading from massive (>70,000) number of
  files.
  """

  DEFAULT_DESIRED_BUNDLE_SIZE = 64 * 1024 * 1024  # 64MB

  def __init__(
      self,
      desired_bundle_size=DEFAULT_DESIRED_BUNDLE_SIZE,
      compression_type=CompressionTypes.AUTO,
      **kwargs):
    """Initialize the :class:`ReadAllVcfHeaders` transform.

    Args:
      desired_bundle_size (int): Desired size of bundles that should be
        generated when splitting this source into bundles. See
        :class:`~apache_beam.io.filebasedsource.FileBasedSource` for more
        details.
      compression_type (str): Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
    """
    super(ReadAllVcfHeaders, self).__init__(**kwargs)
    source_from_file = partial(
        _create_vcf_header_source, compression_type=compression_type)
    self._read_all_files = filebasedsource.ReadAllFiles(
        False,  # splittable (we are just reading the headers)
        CompressionTypes.AUTO, desired_bundle_size,
        0,  # min_bundle_size
        source_from_file)

  def expand(self, pvalue):
    return pvalue | 'ReadAllFiles' >> self._read_all_files


class HeaderTypeConstants(object):
  INFO = 'INFO'
  FILTER = 'FILTER'
  ALT = 'ALT'
  FORMAT = 'FORMAT'
  CONTIG = 'contig'


class _HeaderFieldKeyConstants(object):
  ID = 'ID'
  NUMBER = 'Number'
  TYPE = 'Type'
  DESCRIPTION = 'Description'
  SOURCE = 'Source'
  VERSION = 'Version'
  LENGTH = 'length'


class _WriteVcfHeaderFn(beam.DoFn):
  """A DoFn for writing VCF headers to a file."""

  HEADER_TEMPLATE = '##{}=<{}>\n'
  FINAL_HEADER_LINE = '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT\n'

  def __init__(self, file_path):
    self._file_path = file_path
    self._file_to_write = None

  def process(self, header):
    with FileSystems.create(self._file_path) as self._file_to_write:
      self._write_headers_by_type(HeaderTypeConstants.INFO, header.infos)
      self._write_headers_by_type(HeaderTypeConstants.FILTER, header.filters)
      self._write_headers_by_type(HeaderTypeConstants.ALT, header.alts)
      self._write_headers_by_type(HeaderTypeConstants.FORMAT, header.formats)
      self._write_headers_by_type(HeaderTypeConstants.CONTIG, header.contigs)
      self._file_to_write.write(self.FINAL_HEADER_LINE)

  def _write_headers_by_type(self, header_type, headers):
    """Writes all VCF headers of a specific type.

    Args:
      header_type (str): The type of `headers` (e.g. INFO, FORMAT, etc.).
      headers (dict): Each value of headers is a dictionary that describes a
        single VCF header line.
    """
    for header in headers.values():
      self._file_to_write.write(
          self._to_vcf_header_line(header_type, header))

  def _to_vcf_header_line(self, header_type, header):
    """Formats a single VCF header line.

    Args:
      header_type (str): The VCF type of `header` (e.g. INFO, FORMAT, etc.).
      header (dict): A dictionary mapping header field keys (e.g. id, desc,
        etc.) to their corresponding values for the header line.

    Returns:
      A formatted VCF header line.
    """
    formatted_header_values = self._format_header(header)
    return self.HEADER_TEMPLATE.format(header_type, formatted_header_values)

  def _format_header(self, header):
    """Formats all key, value pairs that describe the header line.

    Args:
      header (dict): A dictionary mapping header field keys (e.g. id, desc,
        etc.) to their corresponding values for the header line.

    Returns:
      A formatted string composed of header keys and values.
    """
    formatted_values = []
    for key, value in header.iteritems():
      if self._should_include_key_value(key, value):
        formatted_values.append(self._format_header_key_value(key, value))
    return ','.join(formatted_values)

  def _should_include_key_value(self, key, value):
    return value is not None or (key != 'source' and key != 'version')

  def _format_header_key_value(self, key, value):
    """Formats a single key, value pair in a header line.

    Args:
      key (str): The key of the header field (e.g. num, desc, etc.).
      value: The header value corresponding to the key in a specific
        header line.

    Returns:
      A formatted key, value pair for a VCF header line.
    """
    key = self._format_header_key(key)
    if value is None:
      value = vcfio.MISSING_FIELD_VALUE
    elif key == _HeaderFieldKeyConstants.NUMBER:
      value = self._format_number(value)
    elif (key == _HeaderFieldKeyConstants.DESCRIPTION
          or key == _HeaderFieldKeyConstants.SOURCE
          or key == _HeaderFieldKeyConstants.VERSION):
      value = self._format_string_value(value)
    return '{}={}'.format(key, value)

  def _format_header_key(self, key):
    if key == VcfParserHeaderKeyConstants.ID:
      return _HeaderFieldKeyConstants.ID
    elif key == VcfParserHeaderKeyConstants.NUM:
      return _HeaderFieldKeyConstants.NUMBER
    elif key == VcfParserHeaderKeyConstants.DESC:
      return _HeaderFieldKeyConstants.DESCRIPTION
    elif key == VcfParserHeaderKeyConstants.TYPE:
      return _HeaderFieldKeyConstants.TYPE
    elif key == VcfParserHeaderKeyConstants.SOURCE:
      return _HeaderFieldKeyConstants.SOURCE
    elif key == VcfParserHeaderKeyConstants.VERSION:
      return _HeaderFieldKeyConstants.VERSION
    elif key == VcfParserHeaderKeyConstants.LENGTH:
      return _HeaderFieldKeyConstants.LENGTH
    else:
      raise ValueError('Invalid VCF header key {}.'.format(key))

  def _format_number(self, number):
    """Returns the string representation of field_count from PyVCF.

    PyVCF converts field counts to an integer with some predefined constants
    as specified in the vcf.parser.field_counts dict (e.g. 'A' is -1). This
    method converts them back to their string representation to avoid having
    direct dependency on the arbitrary PyVCF constants.

    Args:
      number (int): An integer representing the number of fields in INFO
        as specified by PyVCF.

    Returns:
      A string representation of field_count (e.g. '-1' becomes 'A').

    Raises:
      ValueError: if the number is not valid.
    """
    if number is None:
      return None
    elif number >= 0:
      return str(number)
    number_to_string = {v: k for k, v in vcf.parser.field_counts.items()}
    if number in number_to_string:
      return number_to_string[number]
    else:
      raise ValueError('Invalid value for number: {}'.format(number))

  def _format_string_value(self, value):
    return '"{}"'.format(value)


class WriteVcfHeaders(PTransform):
  """A PTransform for writing VCF header lines."""

  def __init__(self, file_path):
    self._file_path = file_path

  def expand(self, pcoll):
    return pcoll | beam.ParDo(_WriteVcfHeaderFn(self._file_path))
