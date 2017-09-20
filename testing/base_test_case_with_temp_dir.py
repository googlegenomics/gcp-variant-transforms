"""Base class for unittests requiring temporary directories/files."""

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
