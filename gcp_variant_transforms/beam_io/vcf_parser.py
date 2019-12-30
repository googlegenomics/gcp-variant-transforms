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
from typing import Iterable  # pylint: disable=unused-import
import logging
import os

from apache_beam.coders import coders
from apache_beam.io import filesystems
from apache_beam.io import textio
from pysam import libcbcf

from gcp_variant_transforms.beam_io import bgzf

# Stores data about failed VCF record reads. `line` is the text line that
# caused the failed read and `file_name` is the name of the file that the read
# failed in.
MalformedVcfRecord = namedtuple('MalformedVcfRecord',
                                ['file_name', 'line', 'error'])
# Indicates one value for each alternate allele.
FIELD_COUNT_ALTERNATE_ALLELE = 'A'
FIELD_COUNT_ALL_ALLELE = 'R'
FIELD_COUNT_GENOTYPE = 'G'

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
               pre_infer_headers=False,  # type: bool
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    # If `representative_header_lines` is given, header lines in `file_name`
    # are ignored; refer to _process_header_lines() logic.
    self._representative_header_lines = representative_header_lines
    self._file_name = file_name
    self._allow_malformed_records = allow_malformed_records
    self._pre_infer_headers = pre_infer_headers

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

    self._text_lines = text_source.read_records(self._file_name,
                                                range_tracker)

  def send_kill_signal_to_child(self):
    return

  def _next_non_empty_line(self, iterator):
    # type: (Iterable[str]) -> str
    # Get next non-empty stripped record from iterator.
    text_line = next(iterator).strip()
    while not text_line:  # skip empty lines.
      text_line = next(iterator).strip()
    return text_line

  def _process_header_lines(self, header_lines):
    """Processes header lines from text source and initializes the parser.

    Note: this method will be automatically called by textio._TextSource().
    """
    if self._representative_header_lines:
      # Replace header lines with given representative header lines.
      # We need to keep the last line of the header from the file because it
      # contains the sample IDs, which is unique per file.
      header_lines = self._representative_header_lines + header_lines[-1:]

    # PySam requires 'fileformat=VCFvX' field to be supplied, default to 4.0.
    if header_lines and not header_lines[0].startswith(
        FILE_FORMAT_HEADER_TEMPLATE.format(VERSION='')):
      header_lines.insert(
          0, FILE_FORMAT_HEADER_TEMPLATE.format(VERSION='4.0'))

    parsed_header_lines = []
    for line in header_lines:
      # Number='G' is to be deprecated and is unsupported by PySam - replace
      # with 'unknown' number identifier if found.
      parsed_line = line.strip().replace('Number=G', 'Number=.')
      # Tests provide lines in unicode.
      if isinstance(parsed_line, str):
        parsed_line = parsed_line.decode('utf-8')
      if parsed_line:
        # For str cases, decode then re-encode lines in utf-8, to not use ascii
        # encoding.
        parsed_header_lines.append(parsed_line.encode('utf-8'))

    self._init_with_header(parsed_header_lines)

  def next(self):
    try:
      text_line = self._next_non_empty_line(self._text_lines)
    except StopIteration as e:
      # clean up, once iterator is depleted.
      self.send_kill_signal_to_child()
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
               pre_infer_headers=False,  # type: bool
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    super(PySamParser, self).__init__(file_name,
                                      range_tracker,
                                      file_pattern,
                                      compression_type,
                                      allow_malformed_records,
                                      representative_header_lines,
                                      splittable_bgzf,
                                      pre_infer_headers,
                                      **kwargs)
    # These members will be properly initiated in _init_parent_process().
    self._vcf_reader = None
    self._to_child = None
    self._original_info_list = None
    self._process_pid = None

  def send_kill_signal_to_child(self):
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

  def _init_child_process(
      self, variants_read, pysam_write, header_lines, pre_infer_headers):
    # Child process' task is to populate data into the pipe that feeds
    # VariantFile class - first by populating all of the header lines, and then
    # by redirecting the data received from the second pipe.

    # Write Header Lines into PySam.
    to_pysam_pipe = os.fdopen(pysam_write, 'w')
    if pre_infer_headers:
      to_pysam_pipe.write(header_lines[0] + '\n') # fileformat line
      to_pysam_pipe.write(header_lines[-1] + '\n') # `#CHROM...` line
    else:
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
      self._init_child_process(
          variants_read, pysam_write, header_lines, self._pre_infer_headers)

  def _get_variant(self, data_line):
    try:
      self._to_child.write(data_line.encode('utf-8') + '\n')
      self._to_child.flush()
      return self._convert_to_variant(next(self._vcf_reader))
    except (ValueError, StopIteration, TypeError) as e:
      logging.warning('VCF record read failed in %s for line %s: %s',
                      self._file_name, data_line, str(e))
      return MalformedVcfRecord(self._file_name, data_line, str(e))

  def _verify_record(self, record):
    # For incorrectly supplied POS or END info fields (eg. String given
    # instead of int), PySam returns "-MAX_INT" value instead of raising an
    # error, which we need to catch ourselves.
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
        reference_bases=self._convert_field(record.ref, True),
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
      if isinstance(v, bool):
        info[k] = v
      elif v is None and k not in self._original_info_list:
        info[k] = True
      elif k != END_INFO_KEY:
        if isinstance(v, tuple):
          info[k] = self._homogenize_list_dtype(
              map(self._convert_field, v, [True] * (len(v))))
        else:
          # If a field was not provided in the header, make it a list.
          info[k] = (
              self._convert_field(v, True) if k in self._original_info_list else
              [self._convert_field(v, True)])

    return info

  def _parse_to_numeric(self, value):
    # type: (Any) -> Any
    # Attempt to parse a value to int, then float or return as string.
    try:
      return int(str(value))
    except ValueError:
      try:
        # Resolve the floating point discrepancy.
        return self._parse_float(float(value))
      except ValueError:
        # non float
        return value

  def _parse_float(self, value):
    # type: (Any) -> Any

    precise_value = float("{:0g}".format(value))
    if precise_value.is_integer():
      return int(precise_value)
    return precise_value

  def _convert_field(self, value, is_numeric):
    # type: (Any) -> Any
    # PySam currently doesn't recognize '.' value for String fields as missing.
    if value == '.' or value == b'.' or value is None:
      return None
    # some times PySam returns unicode strings, encode them as strings instead.
    elif isinstance(value, unicode):
      value = value.encode('utf-8')
    return self._parse_to_numeric(value) if is_numeric else str(value)

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
              self._convert_field(value, False))
        else:
          info[key] = (self._homogenize_list_dtype(
              map(self._convert_field, value, [True] * (len(value)))) if
                       isinstance(value, tuple) else
                       self._convert_field(value, True))

      # PySam samples are "phased" for haploids, so check for for the type
      # before settings default phaseset value.
      if phaseset is None and sample.phased and len(genotype) > 1:
        phaseset = DEFAULT_PHASESET_VALUE
      calls.append(VariantCall(name, genotype, phaseset, info))

    return calls

  def _homogenize_list_dtype(self, data):
    # Due to BQ requirements, make sure that elements in repeated fields are of
    # the same type.
    none_type = type(None)
    if all(isinstance(elem, (int, none_type)) for elem in data):
      cast_type = int
    elif all(isinstance(elem, (int, float, none_type)) for elem in data):
      cast_type = float
    else:
      cast_type = str

    return [None if elem is None else cast_type(elem) for elem in data]
