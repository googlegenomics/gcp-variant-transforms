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

import unittest

from gcp_variant_transforms.libs.annotation.vep import file_metadata_stub
from gcp_variant_transforms.libs.annotation.vep import vep_runner_util


# TODO(bashir2): Create tests with non local inputs as well.
_INPUT_PATTERN = 'some/input/pattern*'
_INPUT_FILES_WITH_SIZE = [
    ('some/input/pattern/a', 100),
    ('some/input/pattern/b', 100),
    ('some/input/pattern/c', 100),
    ('some/input/pattern/dir1/a', 100),
    ('some/input/pattern/dir1/dir2/b', 100),
    ('some/input/pattern/dir2/b', 100),
    ('some/input/pattern/dir2/c', 100),
]


class VepRunnerUtilTest(unittest.TestCase):

  def test_disribute_files_on_workers_one(self):
    output_dir = 'test/out/dir'
    file_metadata_list = [file_metadata_stub.FileMetadataStub(path, size) for
                          (path, size) in _INPUT_FILES_WITH_SIZE]
    worker_actions_list = vep_runner_util.disribute_files_on_workers(
        file_metadata_list, output_dir, 1)
    self.assertEqual(1, len(worker_actions_list))
    single_worker_action_map = worker_actions_list[0].io_map
    self.assertDictEqual(
        single_worker_action_map,
        {f.path: '{}/{}{}'.format(output_dir, f.path,
                                  vep_runner_util._VEP_OUTPUT_SUFFIX)
         for f in file_metadata_list})

  def test_disribute_files_on_workers_multiple(self):
    output_dir = 'test/out/dir'
    file_metadata_list = [file_metadata_stub.FileMetadataStub(path, size) for
                          (path, size) in _INPUT_FILES_WITH_SIZE]
    worker_actions_list = vep_runner_util.disribute_files_on_workers(
        file_metadata_list, output_dir, 3)
    self.assertEqual(3, len(worker_actions_list))
    total_number_of_files = sum([len(l.io_map) for l in worker_actions_list])
    self.assertEqual(total_number_of_files,
                     len(_INPUT_FILES_WITH_SIZE))
    merged_dict = {}
    for actions_list in  worker_actions_list:
      for k, v in actions_list.io_map.iteritems():
        merged_dict[k] = v
    self.assertDictEqual(
        merged_dict,
        {f.path: '{}/{}{}'.format(output_dir, f.path,
                                  vep_runner_util._VEP_OUTPUT_SUFFIX)
         for f in file_metadata_list})
