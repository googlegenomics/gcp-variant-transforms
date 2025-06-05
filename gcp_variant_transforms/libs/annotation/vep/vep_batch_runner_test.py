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


import unittest

import logging

from typing import List  # pylint: disable=unused-import

import mock
from mock import patch

from apache_beam.io import filesystems
from gcp_variant_transforms.libs.annotation.vep import vep_batch_runner

_SPECIES = 'homo_sapiens'
_ASSEMBLY = 'GRCh38'
_OUTPUT_DIR = 'gs://variant-data/output/annotation/test-vep-batch'
_VEP_INFO_FIELD = 'VEP_INFO'
_IMAGE = 'us-east1-docker.pkg.dev/variant-transform-dxt/dxt-public-variant-transform/vt_vep@sha256:355f464a68676153bbf37b48ab78bac5d6a330f04ba1175a9378c8d1fc556dca'
_CACHE = 'gs://variant-data/vep/vep_cache_homo_sapiens_GRCh38_104.tar.gz'
_NUM_FORK = 8
_PROJECT = 'variant-transform-dxt'
_LOCATION = 'us-east1'
_REGION = 'us-east1'
_SERVICE_ACCOUNT = '704603241590-compute@developer.gserviceaccount.com'
_MACHINE_TYPE = 'e2-standard-4'
_WATCHDOG_FILE = 'gs://variant-data/output/annotation/test-vep-batch/watchdog.txt'
_WATCHDOG_INTERVAL = 30


class TestBatchVepRunner(unittest.TestCase):
  def setUp(self):
    self.runner = vep_batch_runner.BatchVepRunner(
        project=_PROJECT,
        location=_LOCATION,
        species=_SPECIES,
        assembly=_ASSEMBLY,
        input_pattern='gs://variant-data/biovu-cloud-storage/70k/chr22.shapeit2_integrated_snvindels_v2a_27022019_bialleleOnly.GRCh38.phased.70k.vcf',
        output_dir=_OUTPUT_DIR,
        vep_info_field=_VEP_INFO_FIELD,
        vep_image_uri=_IMAGE,
        vep_cache_path=_CACHE,
        vep_num_fork=_NUM_FORK,
        service_account=_SERVICE_ACCOUNT,
        machine_type=_MACHINE_TYPE,
        watchdog_file=_WATCHDOG_FILE,
        watchdog_interval=_WATCHDOG_INTERVAL)

  def test_e2e(self):
    self.runner.run_on_all_files()
    self.runner.wait_until_done()


if __name__ == '__main__':
  unittest.main()
