"""A source for reading from VCF files."""


from __future__ import absolute_import
import logging

from apache_beam.coders import coders
from apache_beam.io import filebasedsource
from apache_beam.io import iobase
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem

import vcf

__all__ = ['ReadFromVcf', 'Variant']


class Variant(object):
  """A class to store info about a genomic variant.

  Each object corresponds to a single record in a VCF file.
  """

  def __init__(self,
               reference_name=None,
               start=None,
               end=None,
               reference_bases=None,
               alternate_bases=None):
    """Initialize the :class:`Variant` object.

    Args:
      reference_name (str): The reference on which this variant occurs
        (such as `chr20` or `X`). .
      start (int): The position at which this variant occurs (0-based).
      end (int): The end position (0-based) of this variant.
      reference_bases (str): The reference bases for this variant.
      alternate_bases (list of str): The bases that appear instead of the
        reference bases.
    """
    # TODO(arostami): Add more fields.
    self.reference_name = reference_name
    self.start = start
    self.end = end
    self.reference_bases = reference_bases
    self.alternate_bases = alternate_bases or []

  def __eq__(self, other):
    return (self.reference_name == other.reference_name and
            self.start == other.start and
            self.end == other.end and
            self.reference_bases == other.reference_bases and
            self.alternate_bases == other.alternate_bases)

  def __repr__(self):
    return ', '.join(
        [str(s) for s in [self.reference_name,
                          self.start,
                          self.end,
                          self.reference_bases,
                          self.alternate_bases]])

# TODO(arostami): Remove once header processing changes are released in the
# Beam SDK.
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
               header_matcher_predicate=None):
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
    self._header_matcher_predicate = header_matcher_predicate

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
      position_after_processing_header_lines, header_lines = (
          self._process_header(file_to_read, read_buffer))
      self.process_header_lines_matching_predicate(file_name, header_lines)
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

  def process_header_lines_matching_predicate(self, file_name, header_lines):
    # Performs any special processing based on header lines that match the
    # 'header_matcher_predicate'. Implementation is currently intended for
    # subclasses that handle special header-processing logic.
    # TODO(BEAM-2776): Implement generic header processing functionality here.
    pass

  def _process_header(self, file_to_read, read_buffer):
    # Returns a tuple containing the position in file after processing header
    # records and a list of decoded header lines that match
    # 'header_matcher_predicate'.
    header_lines_to_return = []
    position = self._skip_lines(
        file_to_read, read_buffer,
        self._skip_header_lines) if self._skip_header_lines else 0
    while self._header_matcher_predicate:
      record, num_bytes_to_next_record = self._read_record(file_to_read,
                                                           read_buffer)
      decoded_line = self._coder.decode(record)
      if not self._header_matcher_predicate(decoded_line):
        # We've read past the header section at this point, so go back a line.
        file_to_read.seek(position)
        read_buffer.reset()
        break
      header_lines_to_return.append(decoded_line)
      if num_bytes_to_next_record < 0:
        break
      position += num_bytes_to_next_record
    return position, header_lines_to_return

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


class _VcfSource(_TextSource):
  r"""A source for reading VCF files.

  Parses VCF files (version 4) using PyVCF library. If file_pattern specifies
  multiple files, then the header from each file is used separately to parse
  the content. However, the output will be a uniform PCollection of
  :class:`Variant` objects.
  """

  DEFAULT_VCF_READ_BUFFER_SIZE = 65536  # 64kB

  def __init__(self,
               file_pattern,
               min_bundle_size=0,
               compression_type=CompressionTypes.AUTO,
               buffer_size=DEFAULT_VCF_READ_BUFFER_SIZE,
               validate=True):
    super(_VcfSource, self).__init__(
        file_pattern,
        min_bundle_size,
        compression_type,
        strip_trailing_newlines=True,
        coder=coders.StrUtf8Coder(),  # VCF files are UTF-8 per schema.
        buffer_size=buffer_size,
        validate=validate,
        skip_header_lines=0,
        header_matcher_predicate=lambda x: x.startswith('#'))
    self._header_lines_per_file = {}

  def process_header_lines_matching_predicate(self, file_name, header_lines):
    self._header_lines_per_file[file_name] = header_lines

  def read_records(self, file_name, range_tracker):
    def line_generator():
      header_processed = False
      for line in super(_VcfSource, self).read_records(file_name,
                                                       range_tracker):
        if not header_processed and file_name in self._header_lines_per_file:
          for header in self._header_lines_per_file[file_name]:
            yield header
          header_processed = True
        yield line

    try:
      vcf_reader = vcf.Reader(fsock=line_generator())
    except SyntaxError as e:
      raise ValueError('Invalid VCF header: %s' % str(e))
    while True:
      try:
        record = next(vcf_reader)
        yield self.convert_to_variant_record(record)
      except StopIteration:
        break
      except (LookupError, ValueError) as e:
        # TODO(arostami): Add 'strict' and 'loose' modes to not throw an
        # exception in case of such failures.
        raise ValueError('Invalid record in VCF file. Error: %s' % str(e))

  def convert_to_variant_record(self, record):
    variant = Variant()
    variant.reference_name = record.CHROM
    variant.start = record.start
    variant.end = record.end
    variant.reference_bases = record.REF
    # ALT fields are classes in PyVCF (e.g. Substitution), so need convert them
    # to their string representations.
    variant.alternate_bases.extend(
        [str(r) for r in record.ALT if r] if record.ALT else [])
    # TODO(arostami): Add the rest of the fields from record.
    return variant


class ReadFromVcf(PTransform):
  r"""A :class:`~apache_beam.transforms.ptransform.PTransform` for reading VCF
  files.

  Parses VCF files (version 4) using PyVCF library. If file_pattern specifies
  multiple files, then the header from each file is used separately to parse
  the content. However, the output will be a uniform PCollection of
  :class:`Variant` objects.
  """

  def __init__(
      self,
      file_pattern=None,
      min_bundle_size=0,
      compression_type=CompressionTypes.AUTO,
      validate=True,
      **kwargs):
    """Initialize the :class:`ReadFromVcf` transform.

    Args:
      file_pattern (str): The file path to read from as a local file path or a
        GCS ``gs://`` path. The path can contain glob characters
        (``*``, ``?``, and ``[...]`` sets).
      min_bundle_size (int): Minimum size of bundles that should be generated
        when splitting this source into bundles. See
        :class:`~apache_beam.io.filebasedsource.FileBasedSource` for more
        details.
      compression_type (str): Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      validate (bool): flag to verify that the files exist during the pipeline
        creation time.
    """
    super(ReadFromVcf, self).__init__(**kwargs)
    self._source = _VcfSource(
        file_pattern, min_bundle_size, compression_type, validate=validate)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)
