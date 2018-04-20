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


# TODO(bashir2): Create tests with non local inputs as well.
INPUT_PATTERN = 'some/input/pattern*'
INPUT_FILES_WITH_SIZE = [
    ('some/input/pattern/a', 100),
    ('some/input/pattern/b', 100),
    ('some/input/pattern/c', 100),
    ('some/input/pattern/dir1/a', 100),
    ('some/input/pattern/dir1/dir2/b', 100),
    ('some/input/pattern/dir2/b', 100),
    ('some/input/pattern/dir2/c', 100),
]


class FileMetadataStub(object):
  """This is an object to imitate apache_beam.io.filesystem.FileMetadata."""

  def __init__(self, path, size_in_bytes):
    self.path = path
    self.size_in_bytes = size_in_bytes

  def __repr__(self):
    return 'path= {} size_in_bytes={}'.format(self.path, self.size_in_bytes)
