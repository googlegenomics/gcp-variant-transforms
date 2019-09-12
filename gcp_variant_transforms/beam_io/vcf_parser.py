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

from collections import namedtuple
from copy import copy
import logging
import os
import tempfile

from apache_beam.coders import coders
from apache_beam.io import filesystems
from apache_beam.io import textio
from apache_beam.io import range_trackers
try:
  from nucleus.io.python import vcf_reader as nucleus_vcf_reader
  from nucleus.protos import variants_pb2
except ImportError:
  logging.warning('Nucleus is not installed. Cannot use the Nucleus parser.')
from pysam import libcbcf
import vcf

from gcp_variant_transforms.beam_io import bgzf

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
LAST_HEADER_LINE_PREFIX = '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO'
HEADER_INFO_TYPES = ['Integer', 'Float', 'Flag', 'Character', 'String', '.']
HEADER_INFO_NUMBERS = ['A', 'R', 'G', '.']

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
      text_source = bgzf.BGZFBlockSource(
          file_name,
          range_tracker,
          representative_header_lines,
          compression_type,
          header_processor_fns=(
              lambda x: not x.strip() or x.startswith('#'),
              self._process_header_lines),
          **kwargs)
    elif compression_type == filesystems.CompressionTypes.GZIP:
      text_source = bgzf.BGZFSource(
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

    if isinstance(self, PySamParser):
      # In an absence of "#CHROM POS..." header line, PySam process would lock.
      # However, we need to support cases when no records in files were provided
      # even if the above header line is missing. To circumvent the error, read
      # the file until the first line is found, and verify validity.
      self._header_lines = None
      if isinstance(range_tracker, range_trackers.UnsplittableRangeTracker):
        secondary_tracker = range_trackers.UnsplittableRangeTracker(
            copy(range_tracker._range_tracker))
      else:
        secondary_tracker = copy(range_tracker)
      self._verify_header(copy(text_source), secondary_tracker)

    self._text_lines = text_source.read_records(self._file_name,
                                                range_tracker)

  def after_read(self):
    return

  def _verify_header(self, text_source, range_tracker):
    try:
      text_lines = text_source.read_records(self._file_name,
                                            range_tracker)

      text_line = next(text_lines).strip()
      while not text_line.strip():  # skip empty lines.
        # This natively raises StopIteration if end of file is reached.
        text_line = next(text_lines).strip()
      if not self._header_lines[-1].startswith(LAST_HEADER_LINE_PREFIX):
        raise ValueError('Header missing CHROM line.')
      header = libcbcf.VariantHeader()
      for header_line in self._header_lines[:-1]:
        header.add_line(header_line)

      for k, v in header.info.iteritems():
        if not k:
          raise ValueError('Corrupt ID at header line {}.'.format(v.id))
        if not v.description:
          raise ValueError(
              'Corrupt Description at header line {}.'.format(v.id))
        if not v.type or v.record['Type'] not in HEADER_INFO_TYPES:
          raise ValueError('Corrupt Type at header line {}'.format(v.id))
        if not v.record['Number'] or (
            v.record['Number'] not in HEADER_INFO_NUMBERS and
            not v.record['Number'].isdigit()):
          raise ValueError('Unknown Number at header line {}.'.format(v.id))
    except StopIteration:
      if not self._header_lines[-1].startswith(LAST_HEADER_LINE_PREFIX):
        self._header_lines.append(LAST_HEADER_LINE_PREFIX)

  def _process_header_lines(self, header_lines):
    """Processes header lines from text source and initializes the parser.

    Note: this method will be automatically called by textio._TextSource().
    """
    if self._representative_header_lines:
      # Replace header lines with given representative header lines.
      # We need to keep the last line of the header from the file because it
      # contains the sample IDs, which is unique per file.
      header_lines = self._representative_header_lines + header_lines[-1:]
    if isinstance(self, PySamParser):
      if header_lines and not header_lines[0].startswith(
          FILE_FORMAT_HEADER_TEMPLATE.format(VERSION='')):
        header_lines.insert(
            0, FILE_FORMAT_HEADER_TEMPLATE.format(VERSION='4.0'))
      header_lines = [line.strip() for line in header_lines if line.strip()]
      self._header_lines = filter(None, [line.strip().replace(
          'Number=G', 'Number=.').encode('utf-8') for line in header_lines])
    else:
      self._init_with_header(header_lines)

  def next(self):
    try:
      text_line = next(self._text_lines).strip()
      while not text_line:  # skip empty lines.
        # This natively raises StopIteration if end of file is reached.
        text_line = next(self._text_lines).strip()
    except StopIteration as e:
      self.after_read()
      raise e
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


class PySamParser(VcfParser):
  """An Iterator for processing a single VCF file using PySam.

  PySam allows reading a file through either stdin stream, or through as actual
  VCF files, for which it requires legitimate file descriptor. Since we want to
  perform our own parallelization, we will fork our process, to use 2 pipelines
  that will feed into PySam object - 1 will feed the data from main process
  throughout into the child one, while second will get that data in child
  process and feed it to PySam library.

  The requirement of using two pipelines comes from the design of VcfParser base
  class - we could only use a single pipe, but it will divert the parsers.
  """

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
    super(PySamParser, self).__init__(file_name,
                                      range_tracker,
                                      file_pattern,
                                      compression_type,
                                      allow_malformed_records,
                                      representative_header_lines,
                                      **kwargs)
    # These members will be properly initiated in _init_parent_process().
    self._vcf_reader = None
    self._to_child = None
    self._original_info_list = None
    self._process_pid = None
    self._init_with_header(self._header_lines)

  def after_read(self):
    self._to_child.write('\n')
    self._to_child.flush()
    self._to_child.close()
    os.waitpid(self._process_pid, 0)
    return

  def _init_parent_process(self, pysam_read, variants_write):
    into_pysam = os.fdopen(pysam_read)
    self._to_child = os.fdopen(variants_write, 'w')
    self._vcf_reader = libcbcf.VariantFile(into_pysam, 'r')
    self._original_info_list = self._vcf_reader.header.info.keys()

  def _init_child_process(self, variants_read, pysam_write, header_lines):
    # Child process' task is to populate data into the pipe that feeds
    # VariantFile class - first by populating all of the header lines, and then
    # by redirecting the data received from the second pipe.

    # Write Header Lines into PySam.
    to_pysam_pipe = os.fdopen(pysam_write, 'w')
    to_pysam_pipe.write('\n'.join(header_lines) + '\n')
    to_pysam_pipe.flush()

    # Forward variants from _get_variant to PySam.
    from_variants_pipe = os.fdopen(variants_read)
    while True:
      text_line = from_variants_pipe.readline()
      if not text_line or text_line == '\n':
        break
      to_pysam_pipe.write(text_line)
      to_pysam_pipe.flush()

    from_variants_pipe.close()
    to_pysam_pipe.close()
    os._exit(0)

  def _init_with_header(self, header_lines):
    # PySam requires a version header to be present, so add one if absent.
    if header_lines and not header_lines[0].startswith(
        FILE_FORMAT_HEADER_TEMPLATE.format(VERSION='')):
      header_lines.insert(0, FILE_FORMAT_HEADER_TEMPLATE.format(VERSION='4.0'))

    # Pysam pipe is responsible for supplying lines from child process into the
    # VariantFile, since it can only take an actual file descriptor as an input.
    pysam_read, pysam_write = os.pipe()
    # Since child process doesn't have access to the lines that need to be
    # parsed, variants pipe is needed to supply them from _get_variant() method
    # into the child process, to be propagated afterwards into the pysam pipe.
    variants_read, variants_write = os.pipe()
    pid = os.fork()
    if pid:
      self._process_pid = pid
      self._init_parent_process(pysam_read, variants_write)
    else:
      self._init_child_process(variants_read, pysam_write, header_lines)

  def _get_variant(self, data_line):
    try:
      self._to_child.write(data_line.encode('utf-8') + '\n')
      self._to_child.flush()
      return self._convert_to_variant(next(self._vcf_reader))
    except (MemoryError, IOError, ValueError) as e:
      logging.warning('VCF record read failed in %s for line %s: %s',
                      self._file_name, data_line, str(e))
      return MalformedVcfRecord(self._file_name, data_line, str(e))

  def _verify_record(self, record):
    if record.start < 0:
      raise ValueError('Start position is incorrect.')
    if record.stop < 0:
      raise ValueError('End position is incorrect.')

  def _convert_to_variant(self, record):
    # type: (libcbcf.VariantRecord) -> Variant
    """Converts the PySAM record to a :class:`Variant` object.

    Args:
      record: An object containing info about a variant.

    Returns:
      A :class:`Variant` object from the given record.

    Raises:
      ValueError: if `record` is semantically invalid.
    """
    self._verify_record(record)
    return Variant(
        reference_name=record.chrom.encode('utf-8'),
        start=record.start,
        end=record.stop,
        reference_bases=self._convert_field(record.ref),
        alternate_bases=list(record.alts) if record.alts else [],
        names=record.id.split(';') if record.id else [],
        quality=self._parse_to_numeric(record.qual) if record.qual else None,
        filters=(None if not list(record.filter.keys()) else
                 list(record.filter.keys())),
        info=self._get_variant_info(record),
        calls=self._get_variant_calls(record.samples))

  def _get_variant_info(self, record):
    # type: (libcbcf.VariantRecord) -> Dict[str, Any]
    info = {}
    for k, v in list(record.info.items()):
      if k != END_INFO_KEY:
        if isinstance(v, tuple):
          info[k] = list(map(self._convert_field, v))
        else:
          # If a field was not provided in the header, make it a list to
          # follow the PyVCF standards.
          info[k] = (
              self._convert_field(v) if k in self._original_info_list else
              [self._convert_field(v)])

    return info

  def _parse_to_numeric(self, value):
    # type: (Any) -> Any
    if isinstance(value, str) and value.isdigit():
      return int(value)
    else:
      try:
        return self._parse_float(float(value))
      except ValueError:
        # non float
        if isinstance(value, unicode):
          return value.encode('utf-8')
        return value

  def _parse_float(self, value):
    # type: (Any) -> Any
    precise_value = float("{:0g}".format(value))
    if precise_value.is_integer():
      return int(precise_value)
    return precise_value

  def _convert_field(self, value, numeric=True):
    # type: (Any) -> Any
    # PySam currently doesn't recognize '.' value for String fields as missing.
    if value == '.' or value == b'.' or not value:
      return None
    elif isinstance(value, unicode):
      value = value.encode('utf-8')
    return self._parse_to_numeric(value) if numeric else str(value)

  def _get_variant_calls(self, samples):
    # type: (libcvcf.VariantRecordSamples) -> List[VariantCall]
    calls = []

    for (name, sample) in list(samples.items()):
      phaseset = None
      genotype = None
      info = {}
      for (key, value) in list(sample.items()):
        if key == GENOTYPE_FORMAT_KEY:
          if isinstance(value, tuple):
            genotype = []
            for elem in value:
              genotype.append(MISSING_GENOTYPE_VALUE if elem is None else elem)
          else:
            genotype = MISSING_GENOTYPE_VALUE if value is None else value

        elif key == PHASESET_FORMAT_KEY:
          phaseset = (
              list(map(self._convert_field, value, [False] * (len(value)))) if
              isinstance(value, tuple) else
              self._convert_field(value, numeric=False))
        else:
          info[key] = (list(map(self._convert_field, value)) if
                       isinstance(value, tuple) else self._convert_field(value))

      # PySam samples are "phased" for haploids, so check for for the type
      # before settings default phaseset value.
      if phaseset is None and sample.phased and len(genotype) > 1:
        phaseset = DEFAULT_PHASESET_VALUE
      calls.append(VariantCall(name, genotype, phaseset, info))

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
