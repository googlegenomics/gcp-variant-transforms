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

from __future__ import absolute_import

import logging

from typing import Dict, List  # pylint: disable=unused-import

from apache_beam.io import filesystem  # pylint: disable=unused-import
from apache_beam.io import filesystems


# The siffux that is added to output files (the output is always a VCF).
_VEP_OUTPUT_SUFFIX = '_vep_output.vcf'

# This is used for matching all files under the output directory structure.
# It is okay for it to be Google Cloud Storage (GCS) specific because the
# assumption of using GCS as the remote file systems is baked into VepRunner.
_GCS_RECURSIVE_WILDCARD = '**'

# This is to account for the output file size plus some increase due to
# annotation fields.
_SIZE_FACTOR = 3

# The part of a path separating the file system scheme from the actual path.
_FILE_SYSTEM_SCHEME_SEPARATOR = '://'

# The expected suffix for .gz and .bgz files.
_GZ_SUFFIX = 'gz'

# This is used as a heuristic to account for the size of the unzipped file
# based on some anecdotal samples.
# TODO(bashir2): Revisit the file size calculation logic.
_GZ_FACTOR = 10


class SingleWorkerActions(object):
  """Holds information about actions on a single virtual machine.

  This is a pure data object and attributes can be accessed directly but the
  intended pattern for mutating (creation) instances of this is only through
  disribute_jobs_on_workers. Other accesses should be read only.
  """

  def __init__(self):
    self.disk_size_bytes = 0
    # `io_map` is a map from an input file to its corresponding output file.
    self.io_map = {}  # type: Dict[str, str]

  def __repr__(self):
    return 'disk_size_bytes= {} , io_map= {}'.format(
        self.disk_size_bytes, str(self.io_map))


def disribute_files_on_workers(
    file_metadata_list,  # type: List[filesystem.FileMetadata]
    output_dir,  # type: str
    num_workers  # type: int
    ):
  # type: (...) -> List[SingleWorkerActions]
  """Distributes a set of files among VMs to run VEP them.

  It also calculates some other configuration data for virtual machines running
  these actions, like disk space.
  Args:
    file_metadata_list: Information about input files, e.g., path, size, etc.
    output_dir: The location of output files.
    num_workers: Maximum number of workers to use.
  """
  single_vm_actions_list = []  # type: List[SingleWorkerActions]
  file_chunks = [file_metadata_list[i::num_workers] for i in range(num_workers)]
  for chunk in file_chunks:
    if not chunk:
      continue  # This happens when `num_workers` > number of files.
    current_worker = SingleWorkerActions()
    for metadata in chunk:
      current_worker.io_map[metadata.path] = _map_to_output_dir(metadata.path,
                                                                output_dir)
      if metadata.path.endswith(_GZ_SUFFIX):
        current_worker.disk_size_bytes += (
            metadata.size_in_bytes * _GZ_FACTOR)
      else:
        current_worker.disk_size_bytes += (
            metadata.size_in_bytes * _SIZE_FACTOR)
      logging.info('Found input file %s with size %d',
                   metadata.path, metadata.size_in_bytes)
    single_vm_actions_list.append(current_worker)
  if len(single_vm_actions_list) > num_workers:
    raise AssertionError(
        'Number of VM action sets {} is greater than workers {}'.format(
            len(single_vm_actions_list), num_workers))
  return single_vm_actions_list


def _map_to_output_dir(input_path, output_dir):
  # type: (str, str) -> (str)
  """Maps an input path to its corresponding output path.

  For example, for `input_path` being 'gs://my_bucket/input.vcf', it returns
  'gs://output_bucket/out_dir/my_bucket/input.vcf_vep_output.vcf' where
  `self._output_dir` is equal to 'gs://output_bucket/out_dir'.
  """
  output_file = input_path
  scheme = filesystems.FileSystems.get_scheme(input_path)
  if scheme:
    if not input_path.startswith(scheme + _FILE_SYSTEM_SCHEME_SEPARATOR):
      raise ValueError('Expected {}{} at the beginning of input {}'.format(
          scheme, _FILE_SYSTEM_SCHEME_SEPARATOR, input_path))
    output_file = input_path[len(scheme) +
                             len(_FILE_SYSTEM_SCHEME_SEPARATOR):]
  output_file += _VEP_OUTPUT_SUFFIX
  return filesystems.FileSystems.join(output_dir, output_file)


def get_output_pattern(output_dir):
  # type: (str) -> str
  return (format_dir_path(output_dir) +
          _GCS_RECURSIVE_WILDCARD + _VEP_OUTPUT_SUFFIX)


def format_dir_path(dir_path):
  # type: (str) -> str
  """Returns `dir_name` possibly with an added '/' if not already included."""
  return filesystems.FileSystems.join(dir_path, '')
