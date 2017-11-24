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

"""Utility functions and classes for testing."""

from __future__ import absolute_import

import bz2
import gzip
import os
import shutil
import tempfile

from apache_beam.io import filesystem

__all__ = ['TempDir']


class TempDir(object):
  """Context Manager to create and clean-up a temporary directory."""

  def __init__(self):
    self._tempdir = tempfile.mkdtemp()

  def __enter__(self):
    return self

  def __exit__(self, *args):
    if os.path.exists(self._tempdir):
      shutil.rmtree(self._tempdir)

  def get_path(self):
    """Returns the path to the temporary directory."""
    return self._tempdir

  def create_temp_file(
      self, suffix='', lines=None,
      compression_type=filesystem.CompressionTypes.UNCOMPRESSED):
    """Creates a temporary file in the temporary directory.

    Args:
      suffix (str): The filename suffix of the temporary file (e.g. '.txt')
      lines (List[str]): A list of lines that will be written to the temporary
        file.
      compression_type (str): Specifies compression type of the file. Value
        should be one of ``CompressionTypes``.
    Returns:
      The name of the temporary file created.
    Raises:
      ValueError: If ``compression_type`` is unsupported.
    """
    f = tempfile.NamedTemporaryFile(delete=False,
                                    dir=self._tempdir,
                                    suffix=suffix)
    if not lines:
      return f.name
    if compression_type in (filesystem.CompressionTypes.UNCOMPRESSED,
                            filesystem.CompressionTypes.AUTO):
      f.write(''.join(lines))
    elif compression_type == filesystem.CompressionTypes.GZIP:
      with gzip.GzipFile(f.name, 'w') as gzip_file:
        gzip_file.write(''.join(lines))
    elif compression_type == filesystem.CompressionType.BZIP:
      with bz2.BZ2File(f.name, 'w') as bzip_file:
        bzip_file.write(''.join(lines))
    else:
      raise ValueError('Unsupported CompressionType.')

    return f.name
