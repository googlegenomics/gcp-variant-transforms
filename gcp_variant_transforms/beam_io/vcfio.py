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

from typing import Any, Iterable, List, Tuple  # pylint: disable=unused-import
from functools import partial

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.io import filebasedsource
from apache_beam.io import filesystems
from apache_beam.io import range_trackers  # pylint: disable=unused-import
from apache_beam.io import textio
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform

from gcp_variant_transforms.beam_io import bgzf_io
from gcp_variant_transforms.beam_io import vcf_parser

# All other modules depend on vcfio for the following const values.
# In order to keep the current setting we re-declared them here.
MalformedVcfRecord = vcf_parser.MalformedVcfRecord
MISSING_FIELD_VALUE = vcf_parser.MISSING_FIELD_VALUE
PASS_FILTER = vcf_parser.PASS_FILTER
END_INFO_KEY = vcf_parser.END_INFO_KEY
GENOTYPE_FORMAT_KEY = vcf_parser.GENOTYPE_FORMAT_KEY
PHASESET_FORMAT_KEY = vcf_parser.PHASESET_FORMAT_KEY
DEFAULT_PHASESET_VALUE = vcf_parser.DEFAULT_PHASESET_VALUE
MISSING_GENOTYPE_VALUE = vcf_parser.MISSING_GENOTYPE_VALUE
Variant = vcf_parser.Variant
VariantCall = vcf_parser.VariantCall
SampleNameEncoding = vcf_parser.SampleNameEncoding


class _ToVcfRecordCoder(coders.Coder):
  """Coder for encoding :class:`Variant` objects as VCF text lines."""

  def __init__(self, bq_uses_1_based_coordinate):
    # type: (bool) -> None
    """Initialize _ToVcfRecordCoder PTransform.

    Args:
      bq_uses_1_based_coordinate: specify whether the coordinates used to in BQ
        1-based (default) or 0-based. To find out examine start_position column
        description.
    """
    self.bq_uses_1_based_coordinate = bq_uses_1_based_coordinate

  def encode(self, variant):
    # type: (Variant) -> str
    """Converts a :class:`Variant` object back to a VCF line."""
    encoded_info = self._encode_variant_info(variant)
    format_keys = self._get_variant_format_keys(variant)
    encoded_calls = self._encode_variant_calls(variant, format_keys)
    columns = [
        variant.reference_name,
        (None if variant.start is None
         else (variant.start if self.bq_uses_1_based_coordinate
               else variant.start + 1)),
        ';'.join(variant.names),
        variant.reference_bases,
        ','.join(variant.alternate_bases),
        variant.quality,
        ';'.join(variant.filters),
        encoded_info,
        ':'.join(format_keys),
    ]
    if encoded_calls:
      columns.append(encoded_calls)
    columns = [self._encode_value(c) for c in columns]

    return '\t'.join(columns) + '\n'

  def _encode_value(self, value):
    # type: (Any) -> str
    """Encodes a single `Variant` column value for a VCF file line."""
    if not value and value != 0:
      return MISSING_FIELD_VALUE
    elif isinstance(value, list):
      return ','.join([self._encode_value(x) for x in value])
    return value.encode('utf-8') if isinstance(value, unicode) else str(value)

  def _encode_variant_info(self, variant):
    """Encodes the info of a :class:`Variant` for a VCF file line."""
    encoded_infos = []
    start_0_based = (None if variant.start is None
                     else (variant.start - 1 if self.bq_uses_1_based_coordinate
                           else variant.start))
    # Set END in info if it doesn't match len(reference_bases)+start in 0-based
    # coordinate system. This is usually the case for non-variant regions.
    if (variant.start is not None
        and variant.reference_bases
        and variant.end
        and start_0_based + len(variant.reference_bases) != variant.end):
      encoded_infos.append('END=%d' % variant.end)
    # Set all other fields of info.
    for k, v in variant.info.iteritems():
      if v is True:
        encoded_infos.append(k)
      else:
        encoded_infos.append('%s=%s' % (str(k), self._encode_value(v)))
    return ';'.join(encoded_infos)

  def _get_variant_format_keys(self, variant):
    """Gets the format keys of a :class:`Variant`."""
    if not variant.calls:
      return []

    format_keys = [GENOTYPE_FORMAT_KEY]
    for call in variant.calls:
      # If any calls have a set phaseset that is not `DEFAULT_PHASESET_VALUE`,
      # the key will be added to the format field.
      if self._is_alternate_phaseset(call.phaseset):
        format_keys.append(PHASESET_FORMAT_KEY)
      format_keys.extend([k for k in call.info])

    # Sort all keys and remove duplicates after GENOTYPE_FORMAT_KEY
    format_keys[1:] = sorted(list(set(format_keys[1:])))

    return format_keys

  def _encode_variant_calls(self, variant, format_keys):
    # type: (Variant, List[str]) -> str
    """Encodes the calls of `Variant` in a VCF line."""
    # Ensure that genotype is always the first key in format_keys
    assert not format_keys or format_keys[0] == GENOTYPE_FORMAT_KEY
    encoded_calls = []
    for call in variant.calls:
      encoded_call_info = [self._encode_genotype(call.genotype, call.phaseset)]
      for key in format_keys[1:]:
        if key == PHASESET_FORMAT_KEY:
          encoded_call_info.append(
              self._encode_phaseset(call.phaseset))
        else:
          encoded_call_info.append(
              self._encode_call_info_value(call.info, key))

      encoded_calls.append(':'.join(encoded_call_info))

    return '\t'.join(encoded_calls)

  def _encode_genotype(self, genotype, phaseset):
    """Encodes the genotype of a :class:`VariantCall` for a VCF file line."""
    if genotype == MISSING_GENOTYPE_VALUE:
      return MISSING_FIELD_VALUE
    encoded_genotype = []
    for allele in genotype:
      if allele == MISSING_GENOTYPE_VALUE:
        encoded_genotype.append(MISSING_FIELD_VALUE)
      else:
        encoded_genotype.append(self._encode_value(allele))

    phase_char = '|' if phaseset else '/'
    return phase_char.join(encoded_genotype) or MISSING_FIELD_VALUE

  def _encode_phaseset(self, phaseset):
    """Encodes the phaseset of a :class:`VariantCall` for a VCF file line."""
    if self._is_alternate_phaseset(phaseset):
      return phaseset
    return MISSING_FIELD_VALUE

  def _is_alternate_phaseset(self, phaseset):
    return phaseset and phaseset != DEFAULT_PHASESET_VALUE

  def _encode_call_info_value(self, info, key):
    """Encodes the info of a :class:`VariantCall` for a VCF file line."""
    if key in info:
      return self._encode_value(info[key])
    return MISSING_FIELD_VALUE


class _VcfSource(filebasedsource.FileBasedSource):
  """A source for reading VCF files.

  Parses VCF files (version 4) using PySam library. If file_pattern specifies
  multiple files, then the header from each file is used separately to parse
  the content. However, the output will be a uniform PCollection of
  :class:`Variant` objects.
  """

  DEFAULT_VCF_READ_BUFFER_SIZE = 65536  # 64kB

  def __init__(
      self,
      file_pattern,  # type: str
      representative_header_lines=None,  # type: List[str]
      compression_type=CompressionTypes.AUTO,  # type: str
      buffer_size=DEFAULT_VCF_READ_BUFFER_SIZE,  # type: int
      validate=True,  # type: bool
      allow_malformed_records=False,  # type: bool
      pre_infer_headers=False,  # type: bool
      sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH,  # type: int
      use_1_based_coordinate=False  # type: bool
      ):
    # type: (...) -> None
    super(_VcfSource, self).__init__(file_pattern,
                                     compression_type=compression_type,
                                     validate=validate)
    self._representative_header_lines = representative_header_lines
    self._compression_type = compression_type
    self._buffer_size = buffer_size
    self._allow_malformed_records = allow_malformed_records
    self._pre_infer_headers = pre_infer_headers
    self._sample_name_encoding = sample_name_encoding
    self._use_1_based_coordinate = use_1_based_coordinate


  def read_records(self,
                   file_name,  # type: str
                   range_tracker  # type: range_trackers.OffsetRangeTracker
                  ):
    # type: (...) -> Iterable[MalformedVcfRecord]
    record_iterator = vcf_parser.PySamParser(
        file_name,
        range_tracker,
        self._compression_type,
        self._allow_malformed_records,
        file_pattern=self._pattern,
        representative_header_lines=self._representative_header_lines,
        pre_infer_headers=self._pre_infer_headers,
        sample_name_encoding=self._sample_name_encoding,
        use_1_based_coordinate=self._use_1_based_coordinate,
        buffer_size=self._buffer_size,
        skip_header_lines=0)

    # Convert iterator to generator to abstract behavior
    for record in record_iterator:
      yield record


class ReadFromBGZF(beam.PTransform):
  """Reads variants from BGZF."""

  def __init__(self,
               input_files,
               representative_header_lines,
               allow_malformed_records,
               pre_infer_headers,
               sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH,
               use_1_based_coordinate=False
              ):
    # type: (List[str], List[str], bool, bool, int, bool) -> None
    """Initializes the transform.

    Args:
      input_files: The BGZF file paths to read from.
      representative_header_lines: Header definitions to be used for parsing
        VCF files.
      allow_malformed_records: If true, malformed records from VCF files will be
        returned as `MalformedVcfRecord` instead of failing the pipeline.
      pre_infer_headers: If true, drop headers and make sure PySam return the
        exact data for variants and calls, without type matching.
      sample_name_encoding: specify how we want to encode sample_name mainly
        to deal with same sample_name used across multiple VCF files.
      use_1_based_coordinate: specify whether the coordinates should be stored
        in BQ using 0-based exclusive (default) or 1-based inclusive coordinate.
    """
    self._input_files = input_files
    self._representative_header_lines = representative_header_lines
    self._allow_malformed_records = allow_malformed_records
    self._pre_infer_headers = pre_infer_headers
    self._sample_name_encoding = sample_name_encoding
    self._use_1_based_coordinate = use_1_based_coordinate

  def _read_records(self, (file_path, block)):
    # type: (Tuple[str, Block]) -> Iterable(Variant)
    """Reads records from `file_path` in `block`."""
    record_iterator = vcf_parser.PySamParser(
        file_path,
        block,
        filesystems.CompressionTypes.GZIP,
        self._allow_malformed_records,
        representative_header_lines=self._representative_header_lines,
        splittable_bgzf=True,
        pre_infer_headers=self._pre_infer_headers,
        sample_name_encoding=self._sample_name_encoding,
        use_1_based_coordinate=self._use_1_based_coordinate)

    for record in record_iterator:
      yield record

  def expand(self, pcoll):
    return (pcoll
            | 'InputFiles' >> beam.Create(self._input_files)
            | 'SplitSource' >> beam.FlatMap(bgzf_io.split_bgzf)
            | 'Reshuffle' >> beam.Reshuffle()
            | 'ReadBlock' >> beam.ParDo(self._read_records))


class ReadFromVcf(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading VCF
  files.

  Parses VCF files (version 4) using PySam library. If file_pattern specifies
  multiple files, then the header from each file is used separately to parse
  the content. However, the output will be a PCollection of
  :class:`Variant` (or :class:`MalformedVcfRecord for failed reads) objects.
  """

  def __init__(
      self,
      file_pattern=None,  # type: str
      representative_header_lines=None,  # type: List[str]
      compression_type=CompressionTypes.AUTO,  # type: str
      validate=True,  # type: bool
      allow_malformed_records=False,  # type: bool
      pre_infer_headers=False,  # type: bool
      sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH,  # type: int
      use_1_based_coordinate=False,  # type: bool
      **kwargs  # type: **str
      ):
    # type: (...) -> None
    """Initialize the :class:`ReadFromVcf` transform.

    Args:
      file_pattern: The file path to read from either as a single file or a
        glob pattern.
      representative_header_lines: Header definitions to be used for parsing
        VCF files. If supplied, header definitions in VCF files are ignored.
      compression_type: Used to handle compressed input files. Typical value is
        :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      validate: flag to verify that the files exist during the pipeline creation
        time.
      pre_infer_headers: If true, drop headers and make sure PySam return the
        exact data for variants and calls, without type matching.
      sample_name_encoding: specify how we want to encode sample_name mainly
        to deal with same sample_name used across multiple VCF files.
      use_1_based_coordinate: specify whether the coordinates should be stored
        in BQ using 0-based exclusive (default) or 1-based inclusive coordinate.
    """
    super(ReadFromVcf, self).__init__(**kwargs)

    self._source = _VcfSource(
        file_pattern,
        representative_header_lines,
        compression_type,
        validate=validate,
        allow_malformed_records=allow_malformed_records,
        pre_infer_headers=pre_infer_headers,
        sample_name_encoding=sample_name_encoding,
        use_1_based_coordinate=use_1_based_coordinate)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)


def _create_vcf_source(
    file_pattern=None, representative_header_lines=None, compression_type=None,
    allow_malformed_records=None, pre_infer_headers=False,
    sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH,
    use_1_based_coordinate=False):
  return _VcfSource(file_pattern=file_pattern,
                    representative_header_lines=representative_header_lines,
                    compression_type=compression_type,
                    allow_malformed_records=allow_malformed_records,
                    pre_infer_headers=pre_infer_headers,
                    sample_name_encoding=sample_name_encoding,
                    use_1_based_coordinate=use_1_based_coordinate)


class ReadAllFromVcf(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading a
  :class:`~apache_beam.pvalue.PCollection` of VCF files.

  Reads a :class:`~apache_beam.pvalue.PCollection` of VCF files or file patterns
  and produces a PCollection :class:`Variant` (or
  :class:`MalformedVcfRecord for failed reads) objects.

  This transform should be used when reading from massive (>70,000) number of
  files.
  """

  DEFAULT_DESIRED_BUNDLE_SIZE = 64 * 1024 * 1024  # 64MB

  def __init__(
      self,
      representative_header_lines=None,  # type: List[str]
      desired_bundle_size=DEFAULT_DESIRED_BUNDLE_SIZE,  # type: int
      compression_type=CompressionTypes.AUTO,  # type: str
      allow_malformed_records=False,  # type: bool
      pre_infer_headers=False,  # type: bool
      sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH,  # type: int
      use_1_based_coordinate=False,  # type: bool
      **kwargs  # type: **str
      ):
    # type: (...) -> None
    """Initialize the :class:`ReadAllFromVcf` transform.

    Args:
      representative_header_lines: Header definitions to be used for parsing VCF
        files. If supplied, header definitions in VCF files are ignored.
      desired_bundle_size: Desired size of bundles that should be generated when
        splitting this source into bundles. See
        :class:`~apache_beam.io.filebasedsource.FileBasedSource` for more
        details.
      compression_type: Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      allow_malformed_records: If true, malformed records from VCF files will be
        returned as :class:`MalformedVcfRecord` instead of failing the pipeline.
      pre_infer_headers: If true, drop headers and make sure PySam return the
        exact data for variants and calls, without type matching.
      sample_name_encoding: specify how we want to encode sample_name mainly
        to deal with same sample_name used across multiple VCF files.
      use_1_based_coordinate: specify whether the coordinates should be stored
        in BQ using 0-based exclusive (default) or 1-based inclusive coordinate.
    """
    super(ReadAllFromVcf, self).__init__(**kwargs)
    source_from_file = partial(
        _create_vcf_source,
        representative_header_lines=representative_header_lines,
        compression_type=compression_type,
        allow_malformed_records=allow_malformed_records,
        pre_infer_headers=pre_infer_headers,
        sample_name_encoding=sample_name_encoding,
        use_1_based_coordinate=use_1_based_coordinate)
    self._read_all_files = filebasedsource.ReadAllFiles(
        True,  # splittable
        CompressionTypes.AUTO, desired_bundle_size,
        0,  # min_bundle_size
        source_from_file)

  def expand(self, pvalue):
    return pvalue | 'ReadAllFiles' >> self._read_all_files


class WriteToVcf(PTransform):
  """A PTransform for writing to VCF files."""

  def __init__(self,
               file_path,
               num_shards=1,
               compression_type=CompressionTypes.AUTO,
               headers=None,
               bq_uses_1_based_coordinate=True):
    # type: (str, int, str, List[str]) -> None
    """Initialize a WriteToVcf PTransform.

    Args:
      file_path: The file path to write to. The files written will begin
        with this prefix, followed by a shard identifier (see num_shards). The
        file path should include the file extension (i.e. ".vcf", ".vcf.gz",
        etc).
      num_shards: The number of files (shards) used for output. If not set, the
        service will decide on the optimal number of shards.
        Constraining the number of shards is likely to reduce
        the performance of a pipeline.  Setting this value is not recommended
        unless you require a specific number of output files.
      compression_type: Used to handle compressed output files. Typical value
        for VCF files is CompressionTypes.UNCOMPRESSED. If set to
        CompressionTypes.AUTO, file_path's extension will be used to detect
        compression.
      headers: A list of VCF meta-information lines describing the at least the
        INFO and FORMAT entries in each record and a header line describing the
        column names. These lines will be written at the beginning of the file.
      bq_uses_1_based_coordinate: specify whether the coordinates used to in BQ
        1-based (default) or 0-based. To find out examine start_position column
        description.
    """
    self._file_path = file_path
    self._num_shards = num_shards
    self._compression_type = compression_type
    self._header = headers and '\n'.join([h.strip() for h in headers]) + '\n'
    self.bq_uses_1_based_coordinate = bq_uses_1_based_coordinate

  def expand(self, pcoll):
    return pcoll | 'WriteToVCF' >> textio.WriteToText(
        self._file_path,
        append_trailing_newlines=False,
        num_shards=self._num_shards,
        coder=_ToVcfRecordCoder(self.bq_uses_1_based_coordinate),
        compression_type=self._compression_type,
        header=self._header)


class _WriteVcfDataLinesFn(beam.DoFn):
  """A function that writes variants to one VCF file."""

  def __init__(self, bq_uses_1_based_coordinate):
    # type: (bool) -> None
    """Initialize _WriteVcfDataLinesFn DoFn function.

    Args:
      bq_uses_1_based_coordinate: specify whether the coordinates used to in BQ
        1-based (default) or 0-based. To find out examine start_position column
        description.
    """
    self._coder = _ToVcfRecordCoder(bq_uses_1_based_coordinate)

  def process(self, (file_path, variants), *args, **kwargs):
    # type: (Tuple[str, List[Variant]]) -> None
    with filesystems.FileSystems.create(file_path) as file_to_write:
      for variant in variants:
        file_to_write.write(self._coder.encode(variant))


class WriteVcfDataLines(PTransform):
  """A PTransform for writing VCF data lines.

  This PTransform takes PCollection<`file_path`, `variants`> as input, and
  writes `variants` to `file_path`. The PTransform `WriteToVcf` takes
  PCollection<`Variant`> as input, and writes all variants to the same file.
  """
  def __init__(self, bq_uses_1_based_coordinate):
    # type: (bool) -> None
    """Initialize WriteVcfDataLines PTransform.

    Args:
      bq_uses_1_based_coordinate: specify whether the coordinates used to in BQ
        1-based (default) or 0-based. To find out examine start_position column
        description.
    """
    self.bq_uses_1_based_coordinate = bq_uses_1_based_coordinate

  def expand(self, pcoll):
    return pcoll | 'WriteToVCF' >> beam.ParDo(_WriteVcfDataLinesFn(
        self.bq_uses_1_based_coordinate))
