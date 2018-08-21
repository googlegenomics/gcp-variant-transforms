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
from collections import namedtuple

from nucleus.io.python import vcf_reader as nucleus
from nucleus.protos import variants_pb2 as nucleus_proto
import os
import tempfile
import vcf

from apache_beam.coders import coders
from apache_beam.io import textio


# Stores data about failed VCF record reads. `line` is the text line that
# caused the failed read and `file_name` is the name of the file that the read
# failed in.
MalformedVcfRecord = namedtuple('MalformedVcfRecord',
                                ['file_name', 'line', 'error'])
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
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    # If `representative_header_lines` is given, header lines in `file_name`
    # are ignored; refer to _process_header_lines() logic.
    self._representative_header_lines = representative_header_lines
    self._file_name = file_name
    self._allow_malformed_records = allow_malformed_records

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
               file_pattern,  # type: str
               compression_type,  # type: str
               allow_malformed_records,  # type: bool
               representative_header_lines=None,  # type:  List[str]
               **kwargs  # type: **str
              ):
    # type: (...) -> None
    super(PyVcfParser, self).__init__(file_name,
                                      range_tracker,
                                      file_pattern,
                                      compression_type,
                                      allow_malformed_records,
                                      representative_header_lines,
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
	       file_pattern,  # type: str
	       compression_type,  # type: str
	       allow_malformed_records,  # type: bool
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
    # This member will be properly initiated in _init_with_header().
    self._vcf_reader = None

  def _store_to_temp_local_file(self, header_lines):
    (temp_file, temp_file_name) = tempfile.mkstemp(text=True)
    for line in header_lines:
      os.write(temp_file, line)
    os.close(temp_file)
    return temp_file_name

  def _init_with_header(self, header_lines):
    # This optional header line is needed by Nucleus.
    header_lines = ['##fileformat=VCFv4.2'] + header_lines
    try:
      self._vcf_reader = nucleus.VcfReader.from_file(
          self._store_to_temp_local_file(header_lines),
          nucleus_proto.VcfReaderOptions())
    except SyntaxError as e:
      raise ValueError(
          'Invalid VCF header in %s: %s' % (self._file_name, str(e)))

  def _get_variant(self, data_line):
    try:
      record = self._vcf_reader.from_string(data_line)
      return self._convert_to_variant(record)
    except (LookupError, ValueError) as e:
      logging.warning('VCF record read failed in %s for line %s: %s',
                      self._file_name, data_line, str(e))
      return MalformedVcfRecord(self._file_name, data_line, str(e))

  def _convert_to_variant_record(
      self,
      record,  # type: nucleus_proto
      ):
    # type: (...) -> Variant
    return Variant(
        reference_name=record.reference_name,
        start=record.start,
        end=record.end,
        reference_bases=(
            record.reference_bases if record.reference_bases != MISSING_FIELD_VALUE else None),
        alternate_bases=map(str, record.alternate_bases) if record.alternate_bases else [],
        names=map(str, record.names) if record.names else [],
        quality=record.quality,
        filters=[PASS_FILTER] if record.filter == [] else map(str, record.filter),
        info=record.info)#,
        #calls=self._get_variant_calls(record))
