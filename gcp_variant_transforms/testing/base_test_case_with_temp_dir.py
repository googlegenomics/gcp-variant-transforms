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

"""Base class for unittests requiring temporary directories/files."""

from __future__ import absolute_import

import os
import shutil
import tempfile
import unittest

__all__ = ['BaseTestCaseWithTempDir']


class BaseTestCaseWithTempDir(unittest.TestCase):
  """Base class for TestCases that deals with TempDir clean-up.

  Inherited test cases will call self._new_tempdir() to start a temporary dir
  which will be deleted at the end of the tests (when tearDown() is called).
  """

  def setUp(self):
    self._tempdirs = []

  def tearDown(self):
    for path in self._tempdirs:
      if os.path.exists(path):
        shutil.rmtree(path)
    self._tempdirs = []

  def _new_tempdir(self):
    result = tempfile.mkdtemp()
    self._tempdirs.append(result)
    return result

  def _create_temp_file(self, name='', suffix='', tmpdir=None):
    if not name:
      name = tempfile.template
    return tempfile.NamedTemporaryFile(
        delete=False, prefix=name,
        dir=tmpdir or self._new_tempdir(), suffix=suffix)

  def _create_temp_vcf_file(self, lines, tmpdir=None):
    with self._create_temp_file(suffix='.vcf', tmpdir=tmpdir) as f:
      for line in lines:
        f.write(line)
    return f.name
