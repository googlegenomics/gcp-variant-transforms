# Copyright 2019 Google LLC.
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


"""A source for reading VCF files and extracting signals about input size."""

from __future__ import absolute_import

from functools import partial
from typing import Dict, Iterable  # pylint: disable=unused-import

from apache_beam.io import filebasedsource
from apache_beam.io import range_trackers  # pylint: disable=unused-import
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform


class VcfEstimate(object):
  """Container for estimation data about the VCF file."""

  def __init__(self,
               file_name=None,  # type: str
               estimated_line_count=None,  # type: float
               samples=None,  # type: List[str]
               size_in_bytes=None  # type: int
              ):
    # type: (...) -> None
    """Initializes a VcfEstimate object.

    Object records file name as well as the following metadata:
      estimated line count - extracted by reading the first data line after
          the last header cloumn, calculating its size in bytes and diving
          the total size of the file by the size of single line.
      samples - contains the names of samples in the file. Extracted by reading
          the last header line and dropping the first 8-9 common columns.
      size_in_bytes - actual size of the file. Extracted from file's metadata.

    Args:
      file_name: name of file
      estimated_line_count: estimated number of variants
      samples: sample names in the file
      size_in_bytes: size of the file
    """
    self.file_name = file_name
    self.estimated_line_count = estimated_line_count
    self.samples = samples
    self.size_in_bytes = size_in_bytes

  def __eq__(self, other):
    return self.file_name == other.file_name

  def __repr__(self):
    return 'File Name: {}, Line Count: {}, Samples: {}, Size: {}'.format(
        self.file_name,
        self.estimated_line_count,
        self.samples,
        self.size_in_bytes
    )


class VcfEstimateSource(filebasedsource.FileBasedSource):
  """A source for reading VCF file estimates."""

  def __init__(self,
               file_pattern,
               compression_type=CompressionTypes.AUTO,
               validate=True):
    # type: (str, str, bool) -> None
    super(VcfEstimateSource, self).__init__(file_pattern,
                                            compression_type=compression_type,
                                            validate=validate,
                                            splittable=False)
    self._compression_type = compression_type


  def read_records(
      self,
      file_name,  # type: str
      unused_range_tracker  # type: range_trackers.UnsplittableRangeTracker
      ):
    # type: (...) -> Iterable[VcfEstimate]
    header_size = 0
    with FileSystems.open(
        file_name, compression_type=self._compression_type) as file_to_read:
      header_line = file_to_read.readline()
      # Read and skip all header lines starting with ##. Make sure to calculate
      # their total size, to marginally better approximate the line count.
      while (not header_line or
             not header_line.strip() or header_line.startswith('##')):
        if header_line and header_line.strip():
          header_size += len(header_line.decode('UTF-8').encode("UTF-8"))
        header_line = file_to_read.readline()
      if not header_line.startswith('#'):
        raise ValueError('File {} has bad header section.'.format(file_name))

      header_size += len(header_line.decode('UTF-8').encode("UTF-8"))
      # Use the last header line to extract samples.
      calls = header_line.split()[8:] # Remove #CHROME..INFO mandatory fields.
      samples = (
          calls if (not calls or calls[0] != 'FORMAT') else calls[1:])

      first_record = file_to_read.readline()
      while not first_record or not first_record.strip():
        first_record = file_to_read.readline()

      size_in_bytes = FileSystems.match(
          [file_name])[0].metadata_list[0].size_in_bytes
      all_lines_size = size_in_bytes
      if not isinstance(file_to_read, CompressedFile):
        # TODO(#482): Find a better solution to handling compressed files.
        all_lines_size -= header_size
      line_size = len(first_record.decode('UTF-8').encode("UTF-8"))
      estimated_line_count = float(all_lines_size) / line_size

    yield VcfEstimate(file_name=file_name,
                      samples=samples,
                      estimated_line_count=estimated_line_count,
                      size_in_bytes=size_in_bytes)


class GetEstimates(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading the
  vcf files, finding the last header line and first data line and to extract
  estimates from them.
  """

  def __init__(
      self,
      file_pattern,  # type: str
      compression_type=CompressionTypes.AUTO,  # type: str
      validate=True,  # type: bool
      **kwargs  # type: **str
      ):
    # type: (...) -> None
    """Initialize the :class:`GetEstimates` transform.

    Args:
      file_pattern: The file path to read from either as a single file or a glob
        pattern.
      compression_type: Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
      validate: Flag to verify that the files exist during the pipeline creation
        time.
    """
    super(GetEstimates, self).__init__(**kwargs)
    self._source = VcfEstimateSource(
        file_pattern,
        compression_type,
        validate=validate)

  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)


def _create_vcf_estimate_source(file_pattern=None,
                                compression_type=None):
  return VcfEstimateSource(file_pattern=file_pattern,
                           compression_type=compression_type)


class GetAllEstimates(PTransform):
  """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading the
  vcf files, finding the last header line and first data line and to extract
  estimates from them.

  This transform should be used when reading from massive (>70,000) number of
  files.
  """

  DEFAULT_DESIRED_BUNDLE_SIZE = 64 * 1024 * 1024  # 64MB

  def __init__(
      self,
      desired_bundle_size=DEFAULT_DESIRED_BUNDLE_SIZE,
      compression_type=CompressionTypes.AUTO,
      **kwargs):
    # type: (int, str, **str) -> None
    """Initialize the :class:`GetAllEstimates` transform.

    Args:
      desired_bundle_size: Desired size of bundles that should be generated when
        splitting this source into bundles. See
        :class:`~apache_beam.io.filebasedsource.FileBasedSource` for more
        details.
      compression_type: Used to handle compressed input files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the
        underlying file_path's extension will be used to detect the compression.
    """
    super(GetAllEstimates, self).__init__(**kwargs)
    source_from_file = partial(
        _create_vcf_estimate_source,
        compression_type=compression_type)
    self._read_all_files = filebasedsource.ReadAllFiles(
        False,  # splittable (we are just reading the headers)
        CompressionTypes.AUTO, desired_bundle_size,
        0,  # min_bundle_size
        source_from_file)

  def expand(self, pvalue):
    return pvalue | 'ReadAllFiles' >> self._read_all_files
