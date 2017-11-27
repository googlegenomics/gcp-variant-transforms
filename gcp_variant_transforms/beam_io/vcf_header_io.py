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

from functools import partial
import vcf

import apache_beam as beam
from apache_beam.io import filebasedsource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform

from gcp_variant_transforms.beam_io import vcfio

__all__ = ['VcfHeader', 'VcfHeaderSource', 'ReadAllVcfHeaders',
           'ReadVcfHeaders', 'WriteVcfHeaders']


class VcfHeader(object):
  """Container for header data."""

  def __init__(self,
               infos=None,
               filters=None,
               alts=None,
               formats=None,
               contigs=None):
    """Initializes a VcfHeader object.

    Args:
      infos (dict): A dictionary mapping info keys (str) to vcf info metadata
        values (:class:`~vcf.parser._Info`).
      filters (dict): A dictionary mapping filter keys (str) to vcf filter
        metadata values (:class:`~vcf.parser._Filter`).
      alts (dict): A dictionary mapping alt keys (str) to vcf alt metadata
        values (:class:`~vcf.parser._Alt`).
      formats (dict): A dictionary mapping format keys (str) to vcf format
        metadata values (:class:`~vcf.parser._Format`).
      contigs (dict): A dictionary mapping contig keys (str) to vcf contig
        metadata values (:class:`~vcf.parser._Contig`).
    """
    self.infos = self._values_asdict(infos or {})
    self.filters = self._values_asdict(filters or {})
    self.alts = self._values_asdict(alts or {})
    self.formats = self._values_asdict(formats or {})
    self.contigs = self._values_asdict(contigs or {})

  def update(self, to_merge):
    """Updates ``self``'s headers with values from ``to_merge``.

    If a specific key does not already exists in a specific one of ``self``'s
    headers, that key and the associated value will be added. If the key does
    already exist in the specific header of ``self``, then the value of that key
    will be updated with the value from ``to_merge``.

    Args:
      to_merge (:class:`VcfHeader`): The VcfHeader object that's headers will be
        merged into the headers of self.
    """
    if not isinstance(to_merge, VcfHeader):
      raise NotImplementedError

    self._merge_header_fields(self.infos, to_merge.infos)
    self._merge_header_fields(self.filters, to_merge.filters)
    self._merge_header_fields(self.alts, to_merge.alts)
    self._merge_header_fields(self.formats, to_merge.formats)
    self._merge_header_fields(self.contigs, to_merge.contigs)

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
    """Converts PyVCF header values to dictionaries."""

    # These methods were not designed to be protected. They start with an
    # underscore to avoid confilcts with field names. For more info, see below:
    # https://docs.python.org/2/library/collections.html#collections.namedtuple
    return {key: header[key]._asdict() for key in header}  # pylint: disable=W0212

  def _merge_header_fields(self, source, to_merge):
    """Modifies ``source`` to add any keys from ``to_merge`` not in ``source``.

    Args:
      source (dict): Source header fields.
      to_merge (dict): Header fields to merge with ``source``.
    Raises:
      ValueError: If the header fields are incompatible (e.g. same key with
        different types or numbers).
    """
    for key, to_merge_value in to_merge.iteritems():
      if key not in source:
        source[key] = to_merge_value
      else:
        source_value = source[key]
        # Verify num and type fields are the same for infos and formats
        if (source_value.keys() != to_merge_value.keys()
            or ('num' in source_value and 'type' in source_value
                and (source_value['num'] != to_merge_value['num']
                     or source_value['type'] != to_merge_value['type']))):
          raise ValueError(
              'Incompatible number or types in header fields: {}, {}'.format(
                  source_value, to_merge_value))


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

  def read_records(self, file_name, range_tracker):
    try:
      vcf_reader = vcf.Reader(fsock=self._read_headers(file_name))
    except StopIteration:
      raise ValueError('{} has no header.'.format(file_name))

    yield VcfHeader(infos=vcf_reader.infos,
                    filters=vcf_reader.filters,
                    alts=vcf_reader.alts,
                    formats=vcf_reader.formats,
                    contigs=vcf_reader.contigs)

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


class _HeaderTypeConstants(object):
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
      self._write_headers_by_type(_HeaderTypeConstants.INFO, header.infos)
      self._write_headers_by_type(_HeaderTypeConstants.FILTER, header.filters)
      self._write_headers_by_type(_HeaderTypeConstants.ALT, header.alts)
      self._write_headers_by_type(_HeaderTypeConstants.FORMAT, header.formats)
      self._write_headers_by_type(_HeaderTypeConstants.CONTIG, header.contigs)
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
    if key == 'id':
      return _HeaderFieldKeyConstants.ID
    elif key == 'num':
      return _HeaderFieldKeyConstants.NUMBER
    elif key == 'desc':
      return _HeaderFieldKeyConstants.DESCRIPTION
    elif key == 'type':
      return _HeaderFieldKeyConstants.TYPE
    elif key == 'source':
      return _HeaderFieldKeyConstants.SOURCE
    elif key == 'version':
      return _HeaderFieldKeyConstants.VERSION
    elif key == 'length':
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
