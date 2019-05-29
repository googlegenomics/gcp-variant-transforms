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

"""Parses VCF files (version 4.x) and converts them to Variant objects.

The 4.2 spec is available at https://samtools.github.io/hts-specs/VCFv4.2.pdf.
"""

from __future__ import absolute_import

import logging
import os
import tempfile
import zlib
from collections import namedtuple

try:
  from nucleus.io.python import vcf_reader as nucleus_vcf_reader
  from nucleus.protos import variants_pb2
except ImportError:
  logging.warning('Nucleus is not installed. Cannot use the Nucleus parser.')

import vcf

from apache_beam.coders import coders
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io import textio
from apache_beam.io.gcp import gcsio

# Stores data about failed VCF record reads. `line` is the text line that
# caused the failed read and `file_name` is the name of the file that the read
# failed in.
MalformedVcfRecord = namedtuple('MalformedVcfRecord',
                                ['file_name', 'line', 'error'])
FIELD_COUNT_ALTERNATE_ALLELE = 'A'  # Indicates one value for each alternate
                                    # allele.
MISSING_FIELD_VALUE = '.'  # Indicates field is missing in VCF record.
PASS_FILTER = 'PASS'  # Indicates that all filters have been passed.
END_INFO_KEY = 'END'  # The info key that explicitly specifies end of a record.
GENOTYPE_FORMAT_KEY = 'GT'  # The genotype format key in a call.
PHASESET_FORMAT_KEY = 'PS'  # The phaseset format key.
DEFAULT_PHASESET_VALUE = '*'  # Default phaseset value if call is phased, but
                              # no 'PS' is present.
MISSING_GENOTYPE_VALUE = -1  # Genotype to use when '.' is used in GT field.
FILE_FORMAT_HEADER_TEMPLATE = '##fileformat=VCFv{VERSION}'

class Variant(object):
  """A class to store info about a genomic variant.

  Each object corresponds to a single record in a VCF file.
  """

  def __init__(self,
               reference_name=None,  # type: str
               start=None,  # type: int
               end=None,  # type: int
               reference_bases=None,  # type: str
               alternate_bases=None,  # type: List[str]
               names=None,  # type: List[str]
               quality=None,  # type: float
               filters=None,  # type: List[str]
               info=None,  # type: Dict[str, Any]
               calls=None  # type: List[VariantCall]
              ):
    # type: (...) -> None
    """Initialize the ``Variant`` object.

    Args:
      reference_name: The reference on which this variant occurs (such as
        `chr20` or `X`).
      start: The position at which this variant occurs (0-based). Corresponds to
        the first base of the string of reference bases.
      end: The end position (0-based) of this variant. Corresponds to the first
        base after the last base in the reference allele.
      reference_bases: The reference bases for this variant.
      alternate_bases: The bases that appear instead of the reference bases.
      names: Names for the variant, for example a RefSNP ID.
      quality: Phred-scaled quality score (-10log10 prob(call is wrong)).
        Higher values imply better quality.
      filters: A list of filters (normally quality filters) this variant has
        failed. `PASS` indicates this variant has passed all filters.
      info: A map of additional variant information. The key is specified
        in the VCF record and the value can be any type .
      calls: The variant calls for this variant. Each one represents the
        determination of genotype with respect to this variant.
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
    # type: (str, List[int], str, Dict[str, Any]) -> None
    """Initialize the :class:`VariantCall` object.

    Args:
      name: The name of the call.
      genotype: The genotype of this variant call as specified by the VCF
        schema. The values are either `0` representing the reference, or a
        1-based index into alternate bases. Ordering is only important if
        `phaseset` is present. If a genotype is not called (that is, a `.` is
        present in the GT string), -1 is used.
      phaseset: If this field is present, this variant call's genotype ordering
        implies the phase of the bases and is consistent with any other variant
        calls in the same reference sequence which have the same phaseset value.
        If the genotype data was phased but no phase set was specified, this
        field will be set to `*`.
      info: A map of additional variant call information. The key is specified
        in the VCF record and the type of the value is specified by the VCF
        header FORMAT.
    """
    self.name = name
    self.genotype = genotype or []
    self.phaseset = phaseset
    self.info = info or {}

  def __eq__(self, other):
    return ((self.name, self.genotype, self.phaseset, self.info) ==
            (other.name, other.genotype, other.phaseset, other.info))

  def __lt__(self, other):
    if self.name != other.name:
      return self.name < other.name
    elif self.genotype != other.genotype:
      return self.genotype < other.genotype
    elif self.phaseset != other.phaseset:
      return self.phaseset < other.phaseset
    else:
      return self.info < other.info

  def __le__(self, other):
    return self < other or self == other

  def __gt__(self, other):
    return other < self

  def __ge__(self, other):
    return other <= self

  def __ne__(self, other):
    return not self == other

  def __repr__(self):
    return ', '.join(
        [str(s) for s in [self.name, self.genotype, self.phaseset, self.info]])


class VcfParser(object):
  """Base abstract class for defining a VCF file parser.

  Derived classes must implement two methods:
    _init_with_header: must initialize parser with given header lines.
    _get_variant: given a line of VCF file, returns a Variant object.
  Objects of the derived classed will be an iterator of records:
  ```
  record_iterator = DerivedVcfParser(...)
  for record in record_iterator:
    yield record
  ```
  """

  def __init__(self,
               file_name,  # type: str
               range_tracker,  # type: range_trackers.OffsetRangeTracker
               file_pattern,  # type: str
               compression_type,  # type: str
               allow_malformed_records,  # type: bool
               representative_header_lines=None,  # type:  List[str]
               splittable_bgzf=False,  # type: bool
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    # If `representative_header_lines` is given, header lines in `file_name`
    # are ignored; refer to _process_header_lines() logic.
    self._representative_header_lines = representative_header_lines
    self._file_name = file_name
    self._allow_malformed_records = allow_malformed_records

    if splittable_bgzf:
      text_source = BGZFBlockSource(
          file_name,
          range_tracker,
          representative_header_lines,
          compression_type,
          header_processor_fns=(
              lambda x: not x.strip() or x.startswith('#'),
              self._process_header_lines),
          **kwargs)
    elif compression_type == filesystems.CompressionTypes.GZIP:
      text_source = BGZFSource(
          file_pattern,
          0,  # min_bundle_size
          compression_type,
          True,  # strip_trailing_newlines
          coders.StrUtf8Coder(),  # coder
          validate=False,
          header_processor_fns=(
              lambda x: not x.strip() or x.startswith('#'),
              self._process_header_lines),
          **kwargs)
    else:
      text_source = textio._TextSource(
          file_pattern,
          0,  # min_bundle_size
          compression_type,
          True,  # strip_trailing_newlines
          coders.StrUtf8Coder(),  # coder
          validate=False,
          header_processor_fns=(
              lambda x: not x.strip() or x.startswith('#'),
              self._process_header_lines),
          **kwargs)

    self._text_lines = text_source.read_records(self._file_name,
                                                range_tracker)

  def _process_header_lines(self, header_lines):
    """Processes header lines from text source and initializes the parser.

    Note: this method will be automatically called by textio._TextSource().
    """
    if self._representative_header_lines:
      # Replace header lines with given representative header lines.
      # We need to keep the last line of the header from the file because it
      # contains the sample IDs, which is unique per file.
      header_lines = self._representative_header_lines + header_lines[-1:]
    self._init_with_header(header_lines)

  def next(self):
    text_line = next(self._text_lines).strip()
    while not text_line:  # skip empty lines.
      # This natively raises StopIteration if end of file is reached.
      text_line = next(self._text_lines).strip()
    record = self._get_variant(text_line)
    if isinstance(record, Variant):
      return record
    elif isinstance(record, MalformedVcfRecord):
      if self._allow_malformed_records:
        return record
      else:
        raise ValueError('VCF record read failed in %s for line %s: %s' %
                         (self._file_name, text_line, str(record.error)))
    else:
      raise ValueError('Unrecognized record type: %s.' % str(type(record)))

  def __iter__(self):
    return self

  def _init_with_header(self, header_lines):
    # type: (List[str]) -> None
    """Initializes the parser specific settings with the given header_lines.

    Note: this method will be called by _process_header_lines().
    """
    raise NotImplementedError

  def _get_variant(self, data_line):
    # type: (str) -> Variant
    """Converts a single data_line of a VCF file into a Variant object.

    In case something goes wrong it must return a MalformedVcfRecord object.
    Note: this method will be called by next(), one line at a time.
    """
    raise NotImplementedError


class PyVcfParser(VcfParser):
  """An Iterator for processing a single VCF file using PyVcf."""

  def __init__(self,
               file_name,  # type: str
               range_tracker,  # type: range_trackers.OffsetRangeTracker
               compression_type,  # type: str
               allow_malformed_records,  # type: bool
               file_pattern=None,  # type: str
               representative_header_lines=None,  # type:  List[str]
               splittable_bgzf=False,  # type: bool
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    super(PyVcfParser, self).__init__(file_name,
                                      range_tracker,
                                      file_pattern,
                                      compression_type,
                                      allow_malformed_records,
                                      representative_header_lines,
                                      splittable_bgzf,
                                      **kwargs)
    self._header_lines = []
    self._next_line_to_process = None
    self._current_line = None
    # This member will be properly initiated in _init_with_header().
    self._vcf_reader = None

  def _init_with_header(self, header_lines):
    self._header_lines = header_lines
    try:
      self._vcf_reader = vcf.Reader(fsock=self._line_generator())
    except SyntaxError as e:
      raise ValueError(
          'Invalid VCF header in %s: %s' % (self._file_name, str(e)))

  def _get_variant(self, data_line):
    # _line_generator will consume this line.
    self._next_line_to_process = data_line
    try:
      record = next(self._vcf_reader)
      return self._convert_to_variant(record, self._vcf_reader.formats)
    except (LookupError, ValueError) as e:
      logging.warning('VCF record read failed in %s for line %s: %s',
                      self._file_name, data_line, str(e))
      return MalformedVcfRecord(self._file_name, data_line, str(e))

  def _line_generator(self):
    for header in self._header_lines:
      yield header
    # Continue to process the next line indefinitely. The next line is set
    # inside _get_variant() and this method is indirectly called in get_variant.
    while self._next_line_to_process:
      self._current_line = self._next_line_to_process
      self._next_line_to_process = None
      # PyVCF has explicit str() calls when parsing INFO fields, which fails
      # with UTF-8 decoded strings. Encode the line back to UTF-8.
      yield self._current_line.encode('utf-8')
      # Making sure _get_variant() assigned a new value before consuming it.
      assert self._next_line_to_process is not None, (
          'Internal error: A data line is requested to be processed more than '
          'once. Please file a bug if you see this!')

  def _convert_to_variant(
      self,
      record,  # type: vcf.model._Record
      formats  # type: Dict[str, vcf.parser._Format]
      ):
    # type: (...) -> Variant
    """Converts the PyVCF record to a :class:`Variant` object.

    Args:
      record: An object containing info about a variant.
      formats: The PyVCF dict storing FORMAT extracted from the VCF header.
        The key is the FORMAT key and the value is
        :class:`~vcf.parser._Format`.
    Returns:
      A :class:`Variant` object from the given record.
    Raises:
      ValueError: if ``record`` is semantically invalid.
    """
    return Variant(
        reference_name=record.CHROM,
        start=record.start,
        end=self._get_variant_end(record),
        reference_bases=(
            record.REF if record.REF != MISSING_FIELD_VALUE else None),
        alternate_bases=self._get_variant_alternate_bases(record),
        names=record.ID.split(';') if record.ID else [],
        quality=record.QUAL,
        filters=[PASS_FILTER] if record.FILTER == [] else record.FILTER,
        info=self._get_variant_info(record),
        calls=self._get_variant_calls(record, formats))

  def _get_variant_end(self, record):
    if END_INFO_KEY not in record.INFO:
      return record.end
    end_info_value = record.INFO[END_INFO_KEY]
    if isinstance(end_info_value, (int, long)):
      return end_info_value
    if (isinstance(end_info_value, list) and len(end_info_value) == 1 and
        isinstance(end_info_value[0], (int, long))):
      return end_info_value[0]
    else:
      raise ValueError('Invalid END INFO field in record: {}'.format(
          self._current_line))

  def _get_variant_alternate_bases(self, record):
    # ALT fields are classes in PyVCF (e.g. Substitution), so need convert
    # them to their string representations.
    return [str(r) for r in record.ALT if r] if record.ALT else []

  def _get_variant_info(self, record):
    info = {}
    for k, v in record.INFO.iteritems():
      if k != END_INFO_KEY:
        info[k] = v

    return info

  def _get_variant_calls(self, record, formats):
    calls = []
    for sample in record.samples:
      call = VariantCall()
      call.name = sample.sample
      for allele in sample.gt_alleles or [MISSING_GENOTYPE_VALUE]:
        if allele is None:
          allele = MISSING_GENOTYPE_VALUE
        call.genotype.append(int(allele))
      phaseset_from_format = (
          getattr(sample.data, PHASESET_FORMAT_KEY)
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
            formats[field].num not in (0, 1) and
            isinstance(data, (int, float, long, basestring, bool))):
          data = [data]
        call.info[field] = data
      calls.append(call)
    return calls


class NucleusParser(VcfParser):
  """An Iterator for processing a single VCF file using Nucleus."""

  def __init__(self,
               file_name,  # type: str
               range_tracker,  # type: range_trackers.OffsetRangeTracker
               compression_type,  # type: str
               allow_malformed_records,  # type: bool
               file_pattern=None,  # type: str
               representative_header_lines=None,  # type:  List[str]
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    super(NucleusParser, self).__init__(file_name,
                                        range_tracker,
                                        file_pattern,
                                        compression_type,
                                        allow_malformed_records,
                                        representative_header_lines,
                                        **kwargs)
    try:
      nucleus_vcf_reader
    except NameError:
      raise RuntimeError(
          'Nucleus is not installed. Cannot use the Nucleus parser.')

    # This member will be properly initiated in _init_with_header().
    self._vcf_reader = None
    # These members will be properly initiated in _extract_header_fields().
    self._header_infos = {}
    self._header_formats = {}

  def _store_to_temp_local_file(self, header_lines):
    temp_file, temp_file_name = tempfile.mkstemp(text=True)
    for line in header_lines:
      if not line.endswith('\n'):
        line += '\n'
      os.write(temp_file, line)
    os.close(temp_file)
    return temp_file_name

  def _init_with_header(self, header_lines):
    # The first header line must be similar to '##fileformat=VCFv.*'.
    if header_lines and not header_lines[0].startswith(
        FILE_FORMAT_HEADER_TEMPLATE.format(VERSION='')):
      header_lines.insert(0, FILE_FORMAT_HEADER_TEMPLATE.format(VERSION='4.0'))

    try:
      self._vcf_reader = nucleus_vcf_reader.VcfReader.from_file(
          self._store_to_temp_local_file(header_lines),
          variants_pb2.VcfReaderOptions())
    except ValueError as e:
      raise ValueError(
          'Invalid VCF header in %s: %s' % (self._file_name, str(e)))
    self._extract_header_fields()

  def _extract_header_fields(self):
    header = self._vcf_reader.header
    for info in header.infos:
      self._header_infos[info.id] = info

    for format_info in header.formats:
      self._header_formats[format_info.id] = format_info

  def _is_info_repeated(self, info_id):
    info = self._header_infos.get(info_id, None)
    if not info or not info.number:
      return False
    else:
      return self._is_repeated(info.number)

  def _is_format_repeated(self, format_id):
    format_info = self._header_formats.get(format_id, None)
    if not format_info or not format_info.number:
      return False
    else:
      return self._is_repeated(format_info.number)

  def _is_repeated(self, number):
    if number in ('0', '1'):
      return False
    else:
      return True

  def _get_variant(self, data_line):
    try:
      variant_proto = self._vcf_reader.from_string(data_line)
      return self._convert_to_variant(variant_proto)
    except ValueError as e:
      logging.warning('VCF variant_proto read failed in %s for line %s: %s',
                      self._file_name, data_line, str(e))
      return MalformedVcfRecord(self._file_name, data_line, str(e))

  def _convert_to_variant(self, variant_proto):
    # type: (variants_pb2.Variant) -> Variant
    return Variant(
        reference_name=variant_proto.reference_name,
        start=variant_proto.start,
        end=variant_proto.end,
        reference_bases=(variant_proto.reference_bases
                         if variant_proto.reference_bases != MISSING_FIELD_VALUE
                         else None),
        alternate_bases=list(variant_proto.alternate_bases),
        names=variant_proto.names[0].split(';') if variant_proto.names else [],
        # TODO(samanvp): ensure the default value (when missing) is set to -1.
        quality=variant_proto.quality,
        filters=map(str, variant_proto.filter),
        info=self._get_variant_info(variant_proto),
        calls=self._get_variant_calls(variant_proto))

  def _get_variant_info(self, variant_proto):
    info = {}
    for k in variant_proto.info:
      data = self._convert_list_value(variant_proto.info[k],
                                      self._is_info_repeated(k))
      # Avoid including missing flags as `false` or other fields valued as `[]`.
      if not data:
        continue
      info[k] = data
    return info

  def _convert_list_value(self, list_values, is_repeated):
    """Converts an object of ListValue to python native types.

    if is_repeated is set the output will be a list otherwise a single value.
    """
    output_list = []
    for value in list_values.values:
      if value.HasField('null_value'):
        output_list.append(value.null_value)
      elif value.HasField('number_value'):
        output_list.append(value.number_value)
      elif value.HasField('int_value'):
        output_list.append(value.int_value)
      elif value.HasField('string_value'):
        output_list.append(value.string_value)
      elif value.HasField('bool_value'):
        output_list.append(value.bool_value)
      elif value.HasField('struct_value'):
        output_list.append(value.struct_value)
      elif value.HasField('list_value'):
        output_list.append(self._convert_list_value(value.list_value, True))
      else:
        raise ValueError('ListValue object has an unexpected value: %s' % value)

    if is_repeated:
      return output_list
    else:
      if len(output_list) > 1 and not self._allow_malformed_records:
        raise ValueError('a not repeated field has more than 1 value')
      if not output_list:
        return None
      else:
        # TODO(samanvp): Verify whether we reach here with len(output_list) > 1.
        return output_list[0]

  def _get_variant_calls(self, variant_proto):
    calls = []
    for call_proto in variant_proto.calls:
      call = VariantCall()
      call.name = call_proto.call_set_name
      if not call_proto.genotype:
        call.genotype.append(MISSING_GENOTYPE_VALUE)
      else:
        call.genotype = list(call_proto.genotype)

      phaseset_from_format = (
          self._convert_list_value(call_proto.info[PHASESET_FORMAT_KEY], False)
          if PHASESET_FORMAT_KEY in call_proto.info else None)
      # Note: Call is considered phased if it contains the 'PS' key regardless
      # of whether it uses '|'.
      if phaseset_from_format or call_proto.is_phased:
        call.phaseset = (str(phaseset_from_format) if phaseset_from_format
                         else DEFAULT_PHASESET_VALUE)
      for k in call_proto.info:
        # Genotype and phaseset (if present) are already included.
        if k in (GENOTYPE_FORMAT_KEY, PHASESET_FORMAT_KEY):
          continue
        data = self._convert_list_value(call_proto.info[k],
                                        self._is_format_repeated(k))
        call.info[k] = data
      calls.append(call)
    return calls


def open_bgzf(file_name):
  compression_type = filesystems.CompressionTypes.GZIP
  mime_type = filesystems.CompressionTypes.mime_type(compression_type)
  raw_file = gcsio.GcsIO().open(file_name, 'rb', mime_type=mime_type)
  return BGZF(raw_file)


class BGZFSource(textio._TextSource):

  def open_file(self, file_name):
    return open_bgzf(file_name)


class BGZF(filesystem.CompressedFile):
  """File wrapper for easier handling of BGZF compressed files.

  It supports reading concatenated GZIP files.
  """

  def _fetch_to_internal_buffer(self, num_bytes):
    """Fetch up to num_bytes into the internal buffer."""
    if (not self._read_eof and self._read_position > 0 and
        (self._read_buffer.tell() - self._read_position) < num_bytes):
      # There aren't enough number of bytes to accommodate a read, so we
      # prepare for a possibly large read by clearing up all internal buffers
      # but without dropping any previous held data.
      self._read_buffer.seek(self._read_position)
      data = self._read_buffer.read()
      self._clear_read_buffer()
      self._read_buffer.write(data)

    while not self._read_eof and (self._read_buffer.tell() - self._read_position
                                 ) < num_bytes:
      self._decompress_decompressor_unused_data(num_bytes)
      if (self._read_buffer.tell() - self._read_position) >= num_bytes:
        return
      buf = self._file.read(self._read_size)
      if buf:
        decompressed = self._decompressor.decompress(buf)
        del buf
        self._read_buffer.write(decompressed)
        self._decompress_decompressor_unused_data(num_bytes)
      else:
        self._read_eof = True

  def _decompress_decompressor_unused_data(self, num_bytes):
    while (self._decompressor.unused_data != b'' and
           self._read_buffer.tell() - self._read_position < num_bytes):
      buf = self._decompressor.unused_data
      self._decompressor = zlib.decompressobj(self._gzip_mask)
      decompressed = self._decompressor.decompress(buf)
      del buf
      self._read_buffer.write(decompressed)


class BGZFBlockSource(textio._TextSource):

  def __init__(self,
               file_name,
               block,
               header_lines,
               compression_type,
               header_processor_fns,
               strip_trailing_newlines=True,
               min_bundle_size=0,
               coder=coders.StrUtf8Coder(),
               validate=True
              ):
    """A source for reading BGZF Block."""
    super(BGZFBlockSource, self).__init__(
        file_name,
        min_bundle_size,
        compression_type,
        strip_trailing_newlines,
        coder,
        validate=validate,
        header_processor_fns=header_processor_fns)
    self._block = block
    self._header_lines = header_lines

  def open_file(self, file_name):
    compression_type = filesystem.CompressionTypes.GZIP
    mime_type = filesystem.CompressionTypes.mime_type(compression_type)
    raw_file = gcsio.GcsIO().open(file_name, 'rb', mime_type=mime_type)
    return BGZFBlock(raw_file, self._block)

  def read_records(self, file_name, _):
    read_buffer = textio._TextSource.ReadBuffer(b'', 0)
    # Processes the header. It appends the header line `#CHROM...` (which
    # contains sample info that is unique for `file_name`) to `header_lines`.
    with open_bgzf(file_name) as file_to_read:
      self._process_header(file_to_read, read_buffer)
    with self.open_file(file_name) as file_to_read:
      while True:
        record = file_to_read.readline()
        if not record or not record.strip():
          break
        if record and not record.startswith('#'):
          yield self._coder.decode(record)


class BGZFBlock(filesystem.CompressedFile):
  """File wrapper to handle one BGZF Block."""

  # Each block in BGZF is no larger than `_MAX_GZIP_SIZE`.
  _MAX_GZIP_SIZE = 64 * 1024

  def __init__(self,
               fileobj,
               block,
               compression_type=filesystem.CompressionTypes.GZIP):
    super(BGZFBlock, self).__init__(fileobj,
                                    compression_type)
    self._block = block
    self._start_offset = self._block.start
    self._last_char = ''

  def _fetch_to_internal_buffer(self, num_bytes):
    """Fetch up to num_bytes into the internal buffer.

    It reads contents from `self._block`.
    - The string before first `\n` is discarded.
    - If the last row does not end with `\n`, it further decompress the next
      64KB and reads the first line. It assumes one row can at most spans in two
      Blocks.
    """
    if self._read_eof:
      return
    # First time enter this method, read the first block data
    if self._start_offset == self._block.start:
      buf = self._read_data_from_block()
      decompressed = self._decompressor.decompress(buf)
      self._last_char = decompressed[-1]
      del buf
      lines = decompressed.split('\n')
      self._read_buffer.write('\n'.join(lines[1:]))
    # There aren't enough number of bytes to accommodate a read, so we
    # prepare for a possibly large read by clearing up all internal buffers
    # but without dropping any previous held data.
    if (self._read_position > 0 and
        self._read_buffer.tell() - self._read_position < num_bytes):
      self._read_buffer.seek(self._read_position)
      data = self._read_buffer.read()
      self._clear_read_buffer()
      self._read_buffer.write(data)
    # Decompress the data until there are at least `num_bytes` in the buffer.
    while self._decompressor.unused_data != b'':
      buf = self._decompressor.unused_data
      if (len(buf) < self._MAX_GZIP_SIZE and
          self._start_offset < self._block.end):
        buf = ''.join([buf, self._read_data_from_block()])

      self._decompressor = zlib.decompressobj(self._gzip_mask)
      decompressed = self._decompressor.decompress(buf)
      self._last_char = decompressed[-1]
      del buf
      self._read_buffer.write(decompressed)
      if (self._read_buffer.tell() - self._read_position) >= num_bytes:
        return

    # Fetch the first line in the next `self._MAX_GZIP_SIZE` bytes.
    if self._last_char != '\n':
      buf2 = self._file.raw._downloader.get_range(
          self._block.end, self._block.end + self._MAX_GZIP_SIZE)
      self._decompressor = zlib.decompressobj(self._gzip_mask)
      decompressed2 = self._decompressor.decompress(buf2)
      del buf2
      self._read_buffer.write(decompressed2.split('\n')[0] + '\n')

    self._read_eof = True

  def _read_data_from_block(self):
    buf = self._file.raw._downloader.get_range(self._start_offset,
                                               self._block.end)
    self._start_offset += len(buf)
    return buf
