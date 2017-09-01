"""Util functions for accessing testdata."""

import os.path

__all__ = ['get_full_file_path', 'get_full_dir']


def get_full_file_path(file_name):
  """Returns the full path of the specified ``file_name`` from ``testdata``."""
  return os.path.join(os.path.dirname(__file__), 'testdata', file_name)


def get_full_dir():
  """Returns the full path of the  ``testdata`` directory."""
  return os.path.join(os.path.dirname(__file__), 'testdata')
