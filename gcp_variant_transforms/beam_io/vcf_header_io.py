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

import vcf

from apache_beam.io import filebasedsource
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform

__all__ = ['VcfHeader', 'ReadVcfHeaders']


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
              'Incompatible number or types in header fields: %s, %s' % (
                  source_value, to_merge_value))


class _VcfHeaderSource(filebasedsource.FileBasedSource):
  """A source for reading VCF file headers.

  Parses VCF files (version 4) using PyVCF library.
  """

  def __init__(self,
               file_pattern,
               compression_type=CompressionTypes.AUTO,
               validate=True):
    super(_VcfHeaderSource, self).__init__(file_pattern,
                                           compression_type=compression_type,
                                           validate=validate,
                                           splittable=False)
    self._compression_type = compression_type

  def read_records(self, file_name, range_tracker):
    try:
      vcf_reader = vcf.Reader(fsock=self._read_headers(file_name))
    except StopIteration:
      raise ValueError('%s has no header.', file_name)

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
      file_pattern=None,
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
    self._source = _VcfHeaderSource(
        file_pattern, compression_type, validate=validate)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)
