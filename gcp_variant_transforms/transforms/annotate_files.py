# Copyright 2019 Google Inc.  All Rights Reserved.
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



import threading
import time
import uuid
from datetime import datetime
from typing import List, Optional  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam.io import filesystems

from gcp_variant_transforms.libs.annotation.vep import vep_runner

_WATCHDOG_FILE_UPDATE_INTERVAL_SECONDS = 30


class AnnotateFile(beam.DoFn):
  """A PTransform to annotate VCF files."""

  def __init__(self, known_args, pipeline_args):
    # type: (argparse.Namespace, List[str]) -> None
    """Initializes `AnnotateFile` object."""
    self._known_args = known_args
    self._pipeline_args = pipeline_args

  def process(self, input_pattern):
    # type: (str) -> None
    watchdog_file = None
    if not self._known_args.run_with_garbage_collection:
      self._annotate_files(input_pattern, watchdog_file)
      return

    unique_id = '-'.join(['watchdog_file',
                          str(uuid.uuid4()),
                          datetime.now().strftime('%Y%m%d-%H%M%S')])
    watchdog_file = filesystems.FileSystems.join(
        self._known_args.annotation_output_dir, unique_id)

    t = threading.Thread(target=self._annotate_files,
                         args=(input_pattern, watchdog_file,))
    t.start()
    while t.isAlive():
      with filesystems.FileSystems.create(watchdog_file) as file_to_write:
        file_to_write.write('Watchdog file.')
      time.sleep(_WATCHDOG_FILE_UPDATE_INTERVAL_SECONDS)

  def _annotate_files(self, input_pattern, watchdog_file):
    # type: (str, Optional[str]) -> None
    runner = vep_runner.create_runner(self._known_args,
                                      self._pipeline_args,
                                      input_pattern,
                                      watchdog_file,
                                      _WATCHDOG_FILE_UPDATE_INTERVAL_SECONDS)
    runner.run_on_all_files()
    runner.wait_until_done()
