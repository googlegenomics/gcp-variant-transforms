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

"""Util functions for accessing testdata."""

from __future__ import absolute_import

import os.path

__all__ = ['get_full_file_path', 'get_full_dir']


def get_full_file_path(file_name):
  """Returns the full path of the specified ``file_name`` from ``data``."""
  return os.path.join(
      os.path.dirname(__file__), 'data', 'vcf', file_name)


def get_full_dir():
  """Returns the full path of the  ``data`` directory."""
  return os.path.join(os.path.dirname(__file__), 'data', 'vcf')
