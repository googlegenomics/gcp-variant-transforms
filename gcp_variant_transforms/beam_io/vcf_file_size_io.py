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

"""A source for estimating the size of VCF files when processed by vcf_to_bq."""

from __future__ import absolute_import

from typing import Iterable, List, Tuple  # pylint: disable=unused-import
import logging
import itertools

import apache_beam as beam
from apache_beam import coders
from apache_beam import transforms
from apache_beam.io import filebasedsource
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io import iobase
from apache_beam.io import range_trackers  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcfio


# Number of lines from each VCF that should be read when estimating disk usage.
SNIPPET_READ_SIZE = 50

def _get_file_size(file_name):
  # type: (str) -> List[FileSizeInfo]
  matched_files = filesystems.FileSystems.match([file_name])[0].metadata_list
  if len(matched_files) != 1:
    raise IOError("File name {} did not correspond to exactly 1 result. "
                  "Instead, got {} matches.".format(file_name,
                                                    len(matched_files)))
  file_metadata = matched_files[0]

  compression_type = filesystem.CompressionTypes.detect_compression_type(
      file_metadata.path)
  if compression_type != filesystem.CompressionTypes.UNCOMPRESSED:
    logging.error("VCF file %s is compressed; disk requirement estimator "
                  "will not be accurate.", file_metadata.path)
  return file_metadata.size_in_bytes


class FileSizeInfo(object):
  def __init__(self, raw_size, encoded_size=None, name="[no filename]"):
    # type: (int, int, str) -> None
    self.raw_size = raw_size
    self.encoded_size = encoded_size # Allow direct initialization
    self.name = name

  def estimate_encoded_file_size(self, raw_sample_size, encoded_sample_size):
    # type: (int, int) -> None
    """Estimate encoded file size, given the sizes for the raw file, sample raw
    lines and sample encoded lines.
    """
    if raw_sample_size == 0:
      # Propagate in-band error state to avoid divide-by-zero.
      logging.warning("File %s appears to have no valid Variant lines. File "
                      "will be ignored for size estimation.", self.name)
      self.encoded_size = 0
      self.raw_size = 0
    else:
      self.encoded_size = (self.raw_size * encoded_sample_size /
                           raw_sample_size)


class FileSizeInfoSumFn(beam.CombineFn):
  """Combiner Function, used to sum up the size fields of FileSizeInfo objects.
  """
  def create_accumulator(self):
    # type: (None) -> Tuple[int, int]
    return (0, 0)  # (raw, encoded) sums

  def add_input(self, (raw, encoded), file_size_info):
    # type: (Tuple[int, int], FileSizeInfo) -> Tuple[int, int]
    return raw + file_size_info.raw_size, encoded + file_size_info.encoded_size

  def merge_accumulators(self, accumulators):
    # type: (Iterable[Tuple[int, int]]) -> Tuple[int, int]
    raw, encoded = zip(*accumulators)
    return sum(raw), sum(encoded)

  def extract_output(self, (raw, encoded)):
    # type: (Tuple[int, int]) -> FileSizeInfo
    return FileSizeInfo(raw, encoded)


class _EstimateVcfSizeSource(filebasedsource.FileBasedSource):
  """A source for estimating the encoded size of a VCF file in `vcf_to_bq`.

  This source first obtains the raw file sizes of a set of VCF files. Then,
  the source reads a limited number of variants from a set of VCF files,
  both as raw strings and encoded `Variant` objects. Finally, the reader
  returns a single `FileSizeInfo` object with an estimate of the input size
  if all sizes had been encoded as `Variant` objects.

  Lines that are malformed are skipped.
  """

  DEFAULT_VCF_READ_BUFFER_SIZE = 65536  # 64kB

  def __init__(self,
               file_pattern,
               sample_size,
               compression_type=filesystem.CompressionTypes.AUTO,
               validate=True,
               vcf_parser_type=vcfio.VcfParserType.PYVCF):
    # type: (str, int, str, bool, vcfio.VcfParserType) -> None
    super(_EstimateVcfSizeSource, self).__init__(
        file_pattern,
        compression_type=compression_type,
        validate=validate,
        splittable=False)
    self._compression_type = compression_type
    self._sample_size = sample_size
    self._vcf_parser_type = vcf_parser_type

  def read_records(
      self,
      file_name,  # type: str
      range_tracker  # type: range_trackers.UnsplittableRangeTracker
      ):
    # type: (...) -> Iterable[FileSizeInfo]
    """This "generator" only emits a single FileSizeInfo object per file."""
    vcf_parser_class = vcfio.get_vcf_parser(self._vcf_parser_type)
    record_iterator = vcf_parser_class(
        file_name,
        range_tracker,
        self._compression_type,
        allow_malformed_records=True,
        file_pattern=self._pattern,
        representative_header_lines=None,
        buffer_size=self.DEFAULT_VCF_READ_BUFFER_SIZE,
        skip_header_lines=0)

    raw_file_size = _get_file_size(file_name)

    # Open distinct channel to read lines as raw bytestrings.
    with filesystems.FileSystems.open(file_name,
                                      self._compression_type) as raw_iterator:
      count, raw_size, encoded_size = 0, 0, 0
      for encoded_record, raw_record in itertools.izip(record_iterator,
                                                       raw_iterator):
        while raw_record and raw_record.startswith('#'):
          # Skip headers. Assume that header size is negligible.
          raw_record = raw_iterator.next()
        logging.debug(
            "Reading record for disk usage estimation. Encoded variant: %s\n"
            "Raw variant: %s", encoded_record, raw_record)
        if count >= self._sample_size:
          break
        if not isinstance(encoded_record, vcfio.Variant):
          logging.error(
              "Skipping VCF line that could not be decoded as a "
              "`vcfio.Variant` in file %s: %s", file_name, raw_record)
          continue
        # Encoding in `utf-8` should represent the string as one byte per char,
        # even for non-ASCII chars. Python adds significant overhead to the
        # bytesize of the full str object.
        raw_size += len(raw_record.encode('utf-8'))
        encoded_size += coders.registry.get_coder(vcfio.Variant).estimate_size(
            encoded_record)
        count += 1

    file_size_info = FileSizeInfo(raw_file_size, name=file_name)
    file_size_info.estimate_encoded_file_size(raw_size, encoded_size)
    yield file_size_info


class EstimateVcfSize(transforms.PTransform):
  """PTransform estimating encoded size of VCFs without reading whole files.

  Output is a PCollection with a single FileSizeInfo object representing the
  aggregate encoded size estimate.
  """

  def __init__(
      self,
      file_pattern,  # type: str
      sample_size,  # type: int
      compression_type=filesystem.CompressionTypes.AUTO,  # type: str
      validate=True,  # type: bool
      **kwargs  # type: **str
  ):
    # type: (...) -> None
    """Initialize the :class:`ReadVcfHeaders` transform.

    Args:
      file_pattern: The file path to read from either as a single file or a glob
        pattern.
      sample_size: The number of lines that should be read from the file.
      compression_type: Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      validate: Flag to verify that the files exist during the pipeline creation
        time.
    """
    super(EstimateVcfSize, self).__init__(**kwargs)
    self._source = _EstimateVcfSizeSource(
        file_pattern, sample_size, compression_type, validate=validate)

  def expand(self, pvalue):
    return (pvalue.pipeline
            | iobase.Read(self._source)
            | beam.CombineGlobally(FileSizeInfoSumFn()))
