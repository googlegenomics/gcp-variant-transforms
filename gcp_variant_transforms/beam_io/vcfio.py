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

"""A source for reading from VCF files (version 4.x).

The 4.2 spec is available at https://samtools.github.io/hts-specs/VCFv4.2.pdf.
"""

from __future__ import absolute_import

import logging
from collections import namedtuple

import vcf

from apache_beam.coders import coders
from apache_beam.io import filebasedsource
from apache_beam.io import iobase
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem

__all__ = ['ReadFromVcf', 'Variant', 'VariantCall', 'VariantInfo']


# Stores data about variant INFO fields. The type of 'data' is specified in the
# VCF headers. 'field_count' is a string that specifies the number of fields
# that the data type contains. Its value can either be a number representing a
# constant number of fields, `None` indicating that the value is not set
# (equivalent to '.' in the VCF file) or one of:
#   - 'A': one value per alternate allele.
#   - 'G': one value for each possible genotype.
#   - 'R': one value for each possible allele (including the reference).
VariantInfo = namedtuple('VariantInfo', ['data', 'field_count'])
MISSING_FIELD_VALUE = '.'  # Indicates field is missing in VCF record.
PASS_FILTER = 'PASS'  # Indicates that all filters have been passed.
END_INFO_KEY = 'END'  # The info key that explicitly specifies end of a record.
GENOTYPE_FORMAT_KEY = 'GT'  # The genotype format key in a call.
PHASESET_FORMAT_KEY = 'PS'  # The phaseset format key.
DEFAULT_PHASESET_VALUE = '*'  # Default phaseset value if call is phased, but
                              # no 'PS' is present.
MISSING_GENOTYPE_VALUE = -1  # Genotype to use when '.' is used in GT field.


class Variant(object):
  """A class to store info about a genomic variant.

  Each object corresponds to a single record in a VCF file.
  """

  def __init__(self,
               reference_name=None,
               start=None,
               end=None,
               reference_bases=None,
               alternate_bases=None,
               names=None,
               quality=None,
               filters=None,
               info=None,
               calls=None):
    """Initialize the :class:`Variant` object.

    Args:
      reference_name (str): The reference on which this variant occurs
        (such as `chr20` or `X`). .
      start (int): The position at which this variant occurs (0-based).
        Corresponds to the first base of the string of reference bases.
      end (int): The end position (0-based) of this variant. Corresponds to the
        first base after the last base in the reference allele.
      reference_bases (str): The reference bases for this variant.
      alternate_bases (List[str]): The bases that appear instead of the
        reference bases.
      names (List[str]): Names for the variant, for example a RefSNP ID.
      quality (float): Phred-scaled quality score (-10log10 prob(call is wrong))
        Higher values imply better quality.
      filters (List[str]): A list of filters (normally quality filters) this
        variant has failed. `PASS` indicates this variant has passed all
        filters.
      info (dict): A map of additional variant information. The key is specified
        in the VCF record and the value is of type ``VariantInfo``.
      calls (list of :class:`VariantCall`): The variant calls for this variant.
        Each one represents the determination of genotype with respect to this
        variant.
    """
    self.reference_name = reference_name
    self.start = start
    self.end = end
    self.reference_bases = reference_bases
    self.alternate_bases = alternate_bases or []
    self.names = names or []
    self.quality = quality
    self.filters = filters or []
    self.info = info or {}
    self.calls = calls or []

  def __eq__(self, other):
    return (isinstance(other, Variant) and
            vars(self) == vars(other))

  def __repr__(self):
    return ', '.join(
        [str(s) for s in [self.reference_name,
                          self.start,
                          self.end,
                          self.reference_bases,
                          self.alternate_bases,
                          self.names,
                          self.quality,
                          self.filters,
                          self.info,
                          self.calls]])

  def __lt__(self, other):
    if not isinstance(other, Variant):
      return NotImplemented

    # Elements should first be sorted by reference_name, start, end.
    # Ordering of other members is not important, but must be
    # deterministic.
    if self.reference_name != other.reference_name:
      return self.reference_name < other.reference_name
    elif self.start != other.start:
      return self.start < other.start
    elif self.end != other.end:
      return self.end < other.end

    self_vars = vars(self)
    other_vars = vars(other)
    for key in sorted(self_vars):
      if self_vars[key] != other_vars[key]:
        return self_vars[key] < other_vars[key]

    return False

  def __le__(self, other):
    if not isinstance(other, Variant):
      return NotImplemented

    return self < other or self == other

  def __ne__(self, other):
    return not self == other

  def __gt__(self, other):
    if not isinstance(other, Variant):
      return NotImplemented

    return other < self

  def __ge__(self, other):
    if not isinstance(other, Variant):
      return NotImplemented

    return other <= self


class VariantCall(object):
  """A class to store info about a variant call.

  A call represents the determination of genotype with respect to a particular
  variant. It may include associated information such as quality and phasing.
  """

  def __init__(self, name=None, genotype=None, phaseset=None, info=None):
    """Initialize the :class:`VariantCall` object.

    Args:
      name (str): The name of the call.
      genotype (List[int]): The genotype of this variant call as specified by
        the VCF schema. The values are either `0` representing the reference,
        or a 1-based index into alternate bases. Ordering is only important if
        `phaseset` is present. If a genotype is not called (that is, a `.` is
        present in the GT string), -1 is used
      phaseset (str): If this field is present, this variant call's genotype
        ordering implies the phase of the bases and is consistent with any other
        variant calls in the same reference sequence which have the same
        phaseset value. If the genotype data was phased but no phase set was
        specified, this field will be set to `*`.
      info (dict): A map of additional variant call information. The key is
        specified in the VCF record and the type of the value is specified by
        the VCF header FORMAT.
    """
    self.name = name
    self.genotype = genotype or []
    self.phaseset = phaseset
    self.info = info or {}

  def __eq__(self, other):
    return ((self.name, self.genotype, self.phaseset, self.info) ==
            (other.name, other.genotype, other.phaseset, other.info))

  def __repr__(self):
    return ', '.join(
        [str(s) for s in [self.name, self.genotype, self.phaseset, self.info]])


class _TextSource(filebasedsource.FileBasedSource):
  r"""A source for reading text files.

  Parses a text file as newline-delimited elements. Supports newline delimiters
  '\n' and '\r\n.

  This implementation only supports reading text encoded using UTF-8 or
  ASCII.
  """

  DEFAULT_READ_BUFFER_SIZE = 8192

  class ReadBuffer(object):
    # A buffer that gives the buffered data and next position in the
    # buffer that should be read.

    def __init__(self, data, position):
      self._data = data
      self._position = position

    @property
    def data(self):
      return self._data

    @data.setter
    def data(self, value):
      assert isinstance(value, bytes)
      self._data = value

    @property
    def position(self):
      return self._position

    @position.setter
    def position(self, value):
      assert isinstance(value, (int, long))
      if value > len(self._data):
        raise ValueError('Cannot set position to %d since it\'s larger than '
                         'size of data %d.', value, len(self._data))
      self._position = value

    def reset(self):
      self.data = ''
      self.position = 0

  def __init__(self,
               file_pattern,
               min_bundle_size,
               compression_type,
               strip_trailing_newlines,
               coder,
               buffer_size=DEFAULT_READ_BUFFER_SIZE,
               validate=True,
               skip_header_lines=0,
               header_processor_fns=(None, None)):
    """Initialize a _TextSource

    Args:
      header_processor_fns (tuple): a tuple of a `header_matcher` function
        and a `header_processor` function. The `header_matcher` should
        return `True` for all lines at the start of the file that are part
        of the file header and `False` otherwise. These header lines will
        not be yielded when reading records and instead passed into
        `header_processor` to be handled. If `skip_header_lines` and a
        `header_matcher` are both provided, the value of `skip_header_lines`
        lines will be skipped and the header will be processed from
        there.

    Raises:
      ValueError: if skip_lines is negative.

    Please refer to documentation in class `ReadFromText` for the rest
    of the arguments.
    """
    super(_TextSource, self).__init__(file_pattern, min_bundle_size,
                                      compression_type=compression_type,
                                      validate=validate)

    self._strip_trailing_newlines = strip_trailing_newlines
    self._compression_type = compression_type
    self._coder = coder
    self._buffer_size = buffer_size
    if skip_header_lines < 0:
      raise ValueError('Cannot skip negative number of header lines: %d',
                       skip_header_lines)
    elif skip_header_lines > 10:
      logging.warning(
          'Skipping %d header lines. Skipping large number of header '
          'lines might significantly slow down processing.')
    self._skip_header_lines = skip_header_lines
    self._header_matcher, self._header_processor = header_processor_fns

  def display_data(self):
    parent_dd = super(_TextSource, self).display_data()
    parent_dd['strip_newline'] = DisplayDataItem(
        self._strip_trailing_newlines,
        label='Strip Trailing New Lines')
    parent_dd['buffer_size'] = DisplayDataItem(
        self._buffer_size,
        label='Buffer Size')
    parent_dd['coder'] = DisplayDataItem(
        self._coder.__class__,
        label='Coder')
    return parent_dd

  def read_records(self, file_name, range_tracker):
    start_offset = range_tracker.start_position()
    read_buffer = _TextSource.ReadBuffer('', 0)

    next_record_start_position = -1

    def split_points_unclaimed(stop_position):
      return (0 if stop_position <= next_record_start_position
              else iobase.RangeTracker.SPLIT_POINTS_UNKNOWN)

    range_tracker.set_split_points_unclaimed_callback(split_points_unclaimed)

    with self.open_file(file_name) as file_to_read:
      position_after_processing_header_lines = (
          self._process_header(file_to_read, read_buffer))
      start_offset = max(start_offset, position_after_processing_header_lines)
      if start_offset > position_after_processing_header_lines:
        # Seeking to one position before the start index and ignoring the
        # current line. If start_position is at beginning if the line, that line
        # belongs to the current bundle, hence ignoring that is incorrect.
        # Seeking to one byte before prevents that.

        file_to_read.seek(start_offset - 1)
        read_buffer.reset()
        sep_bounds = self._find_separator_bounds(file_to_read, read_buffer)
        if not sep_bounds:
          # Could not find a separator after (start_offset - 1). This means that
          # none of the records within the file belongs to the current source.
          return

        _, sep_end = sep_bounds
        read_buffer.data = read_buffer.data[sep_end:]
        next_record_start_position = start_offset - 1 + sep_end
      else:
        next_record_start_position = position_after_processing_header_lines

      while range_tracker.try_claim(next_record_start_position):
        record, num_bytes_to_next_record = self._read_record(file_to_read,
                                                             read_buffer)
        # For compressed text files that use an unsplittable OffsetRangeTracker
        # with infinity as the end position, above 'try_claim()' invocation
        # would pass for an empty record at the end of file that is not
        # followed by a new line character. Since such a record is at the last
        # position of a file, it should not be a part of the considered range.
        # We do this check to ignore such records.
        if len(record) == 0 and num_bytes_to_next_record < 0:  # pylint: disable=len-as-condition
          break

        # Record separator must be larger than zero bytes.
        assert num_bytes_to_next_record != 0
        if num_bytes_to_next_record > 0:
          next_record_start_position += num_bytes_to_next_record

        yield self._coder.decode(record)
        if num_bytes_to_next_record < 0:
          break

  def _process_header(self, file_to_read, read_buffer):
    # Returns a tuple containing the position in file after processing header
    # records and a list of decoded header lines that match
    # 'header_matcher'.
    header_lines = []
    position = self._skip_lines(
        file_to_read, read_buffer,
        self._skip_header_lines) if self._skip_header_lines else 0
    if self._header_matcher:
      while True:
        record, num_bytes_to_next_record = self._read_record(file_to_read,
                                                             read_buffer)
        decoded_line = self._coder.decode(record)
        if not self._header_matcher(decoded_line):
          # We've read past the header section at this point, so go back a line.
          file_to_read.seek(position)
          read_buffer.reset()
          break
        header_lines.append(decoded_line)
        if num_bytes_to_next_record < 0:
          break
        position += num_bytes_to_next_record

      if self._header_processor:
        self._header_processor(header_lines)

    return position

  def _find_separator_bounds(self, file_to_read, read_buffer):
    # Determines the start and end positions within 'read_buffer.data' of the
    # next separator starting from position 'read_buffer.position'.
    # Currently supports following separators.
    # * '\n'
    # * '\r\n'
    # This method may increase the size of buffer but it will not decrease the
    # size of it.

    current_pos = read_buffer.position

    while True:
      if current_pos >= len(read_buffer.data):
        # Ensuring that there are enough bytes to determine if there is a '\n'
        # at current_pos.
        if not self._try_to_ensure_num_bytes_in_buffer(
            file_to_read, read_buffer, current_pos + 1):
          return

      # Using find() here is more efficient than a linear scan of the byte
      # array.
      next_lf = read_buffer.data.find('\n', current_pos)
      if next_lf >= 0:
        if next_lf > 0 and read_buffer.data[next_lf - 1] == '\r':
          # Found a '\r\n'. Accepting that as the next separator.
          return (next_lf - 1, next_lf + 1)
        else:
          # Found a '\n'. Accepting that as the next separator.
          return (next_lf, next_lf + 1)

      current_pos = len(read_buffer.data)

  def _try_to_ensure_num_bytes_in_buffer(
      self, file_to_read, read_buffer, num_bytes):
    # Tries to ensure that there are at least num_bytes bytes in the buffer.
    # Returns True if this can be fulfilled, returned False if this cannot be
    # fulfilled due to reaching EOF.
    while len(read_buffer.data) < num_bytes:
      read_data = file_to_read.read(self._buffer_size)
      if not read_data:
        return False

      read_buffer.data += read_data

    return True

  def _skip_lines(self, file_to_read, read_buffer, num_lines):
    """Skip num_lines from file_to_read, return num_lines+1 start position."""
    if file_to_read.tell() > 0:
      file_to_read.seek(0)
    position = 0
    for _ in range(num_lines):
      _, num_bytes_to_next_record = self._read_record(file_to_read, read_buffer)
      if num_bytes_to_next_record < 0:
        # We reached end of file. It is OK to just break here
        # because subsequent _read_record will return same result.
        break
      position += num_bytes_to_next_record
    return position

  def _read_record(self, file_to_read, read_buffer):
    # Returns a tuple containing the current_record and number of bytes to the
    # next record starting from 'read_buffer.position'. If EOF is
    # reached, returns a tuple containing the current record and -1.

    if read_buffer.position > self._buffer_size:
      # read_buffer is too large. Truncating and adjusting it.
      read_buffer.data = read_buffer.data[read_buffer.position:]
      read_buffer.position = 0

    record_start_position_in_buffer = read_buffer.position
    sep_bounds = self._find_separator_bounds(file_to_read, read_buffer)
    read_buffer.position = sep_bounds[1] if sep_bounds else len(
        read_buffer.data)

    if not sep_bounds:
      # Reached EOF. Bytes up to the EOF is the next record. Returning '-1' for
      # the starting position of the next record.
      return (read_buffer.data[record_start_position_in_buffer:], -1)

    if self._strip_trailing_newlines:
      # Current record should not contain the separator.
      return (read_buffer.data[record_start_position_in_buffer:sep_bounds[0]],
              sep_bounds[1] - record_start_position_in_buffer)
    else:
      # Current record should contain the separator.
      return (read_buffer.data[record_start_position_in_buffer:sep_bounds[1]],
              sep_bounds[1] - record_start_position_in_buffer)


class _VcfSource(filebasedsource.FileBasedSource):
  """A source for reading VCF files.

  Parses VCF files (version 4) using PyVCF library. If file_pattern specifies
  multiple files, then the header from each file is used separately to parse
  the content. However, the output will be a uniform PCollection of
  :class:`Variant` objects.
  """

  DEFAULT_VCF_READ_BUFFER_SIZE = 65536  # 64kB

  def __init__(self,
               file_pattern,
               compression_type=CompressionTypes.AUTO,
               buffer_size=DEFAULT_VCF_READ_BUFFER_SIZE,
               validate=True):
    super(_VcfSource, self).__init__(file_pattern,
                                     compression_type=compression_type,
                                     validate=validate)

    self._compression_type = compression_type
    self._buffer_size = buffer_size

  def read_records(self, file_name, range_tracker):
    record_iterator = _VcfSource._VcfRecordIterator(
        file_name,
        range_tracker,
        self._pattern,
        self._compression_type,
        buffer_size=self._buffer_size,
        skip_header_lines=0)

    # Convert iterator to generator to abstract behavior
    for line in record_iterator:
      yield line

  class _VcfRecordIterator(object):
    """An Iterator for processing a single VCF file."""

    def __init__(self,
                 file_name,
                 range_tracker,
                 file_pattern,
                 compression_type,
                 **kwargs):
      self._header_lines = []
      self._last_record = None
      self._file_name = file_name

      text_source = _TextSource(
          file_pattern,
          0,  # min_bundle_size
          compression_type,
          True,  # strip_trailing_newlines
          coders.StrUtf8Coder(),  # coder
          validate=False,
          header_processor_fns=(lambda x: x.startswith('#'),
                                self._store_header_lines),
          **kwargs)

      self._text_lines = text_source.read_records(self._file_name,
                                                  range_tracker)
      try:
        self._vcf_reader = vcf.Reader(fsock=self._create_generator())
      except SyntaxError as e:
        raise ValueError('Invalid VCF header %s' % str(e))

    def _store_header_lines(self, header_lines):
      self._header_lines = header_lines

    def _create_generator(self):
      header_processed = False
      for text_line in self._text_lines:
        if not header_processed and self._header_lines:
          for header in self._header_lines:
            self._last_record = header
            yield self._last_record
          header_processed = True
        # PyVCF has explicit str() calls when parsing INFO fields, which fails
        # with UTF-8 decoded strings. Encode the line back to UTF-8.
        self._last_record = text_line.encode('utf-8')
        yield self._last_record

    def __iter__(self):
      return self

    def next(self):
      try:
        record = next(self._vcf_reader)
        return self._convert_to_variant_record(record, self._vcf_reader.infos,
                                               self._vcf_reader.formats)
      except (LookupError, ValueError) as e:
        raise ValueError('Invalid record in VCF file. Error: %s' % str(e))

    def _convert_to_variant_record(self, record, infos, formats):
      """Converts the PyVCF record to a :class:`Variant` object.

      Args:
        record (:class:`~vcf.model._Record`): An object containing info about a
          variant.
        infos (dict): The PyVCF dict storing INFO extracted from the VCF header.
          The key is the info key and the value is :class:`~vcf.parser._Info`.
        formats (dict): The PyVCF dict storing FORMAT extracted from the VCF
          header. The key is the FORMAT key and the value is
          :class:`~vcf.parser._Format`.

      Returns:
        A :class:`Variant` object from the given record.
      """
      variant = Variant()
      variant.reference_name = record.CHROM
      variant.start = record.start
      variant.end = record.end
      variant.reference_bases = (
          record.REF if record.REF != MISSING_FIELD_VALUE else None)
      # ALT fields are classes in PyVCF (e.g. Substitution), so need convert
      # them to their string representations.
      variant.alternate_bases.extend(
          [str(r) for r in record.ALT if r] if record.ALT else [])
      variant.names.extend(record.ID.split(';') if record.ID else [])
      variant.quality = record.QUAL
      # PyVCF uses None for '.' and an empty list for 'PASS'.
      if record.FILTER is not None:
        variant.filters.extend(
            record.FILTER if record.FILTER else [PASS_FILTER])
      for k, v in record.INFO.iteritems():
        # Special case: END info value specifies end of the record, so adjust
        # variant.end and do not include it as part of variant.info.
        if k == END_INFO_KEY:
          variant.end = v
          continue
        field_count = None
        if k in infos:
          field_count = self._get_field_count_as_string(infos[k].num)
        variant.info[k] = VariantInfo(data=v, field_count=field_count)
      for sample in record.samples:
        call = VariantCall()
        call.name = sample.sample
        for allele in sample.gt_alleles or [MISSING_GENOTYPE_VALUE]:
          if allele is None:
            allele = MISSING_GENOTYPE_VALUE
          call.genotype.append(int(allele))
        phaseset_from_format = (getattr(sample.data, PHASESET_FORMAT_KEY)
                                if PHASESET_FORMAT_KEY in sample.data._fields
                                else None)
        # Note: Call is considered phased if it contains the 'PS' key regardless
        # of whether it uses '|'.
        if phaseset_from_format or sample.phased:
          call.phaseset = (str(phaseset_from_format) if phaseset_from_format
                           else DEFAULT_PHASESET_VALUE)
        for field in sample.data._fields:
          # Genotype and phaseset (if present) are already included.
          if field in (GENOTYPE_FORMAT_KEY, PHASESET_FORMAT_KEY):
            continue
          data = getattr(sample.data, field)
          # Convert single values to a list for cases where the number of fields
          # is unknown. This is to ensure consistent types across all records.
          # Note: this is already done for INFO fields in PyVCF.
          if (field in formats and
              formats[field].num is None and
              isinstance(data, (int, float, long, basestring, bool))):
            data = [data]
          call.info[field] = data
        variant.calls.append(call)
      return variant

    def _get_field_count_as_string(self, field_count):
      """Returns the string representation of field_count from PyVCF.
      PyVCF converts field counts to an integer with some predefined constants
      as specified in the vcf.parser.field_counts dict (e.g. 'A' is -1). This
      method converts them back to their string representation to avoid having
      direct dependency on the arbitrary PyVCF constants.

      Args:
        field_count (int): An integer representing the number of fields in INFO
          as specified by PyVCF.

      Returns:
        A string representation of field_count (e.g. '-1' becomes 'A').

      Raises:
        ValueError: if the field_count is not valid.
      """
      if field_count is None:
        return None
      elif field_count >= 0:
        return str(field_count)
      field_count_to_string = {v: k for k, v in vcf.parser.field_counts.items()}
      if field_count in field_count_to_string:
        return field_count_to_string[field_count]
      else:
        raise ValueError('Invalid value for field_count: %d' % field_count)


class ReadFromVcf(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading VCF
  files.

  Parses VCF files (version 4) using PyVCF library. If file_pattern specifies
  multiple files, then the header from each file is used separately to parse
  the content. However, the output will be a PCollection of
  :class:`Variant` objects.
  """

  def __init__(
      self,
      file_pattern=None,
      compression_type=CompressionTypes.AUTO,
      validate=True,
      **kwargs):
    """Initialize the :class:`ReadFromVcf` transform.

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
    super(ReadFromVcf, self).__init__(**kwargs)
    self._source = _VcfSource(
        file_pattern, compression_type, validate=validate)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)
