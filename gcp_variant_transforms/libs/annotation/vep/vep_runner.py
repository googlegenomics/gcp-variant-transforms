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

import argparse  # pylint: disable=unused-import
import logging
import time

from typing import Dict, List, Any  # pylint: disable=unused-import

from apache_beam.io import filesystem  # pylint: disable=unused-import
from apache_beam.io import filesystems
from apache_beam.options import pipeline_options
from googleapiclient import discovery
from oauth2client import client
from gcp_variant_transforms.libs.annotation.vep import vep_runner_util


# Minimum size of virtual machine disks to account for cache files.
# NOTE(bashir2): The proper way of setting this is to measure the input cache
# file size and use some heuristics to calculate the size of the decompressed
# version. The current value is more than double what is needed for 'human'.
_MINIMUM_DISK_SIZE_GB = 200

# The name of the file placed at the root of output_dir that includes
# information on how the pipelines were run, input files, etc.
_GLOBAL_LOG_FILE = 'VEP_run_info.log'

# TODO(bashir2): Check if instead of raw strings, we can use a protocol
# buffer describing the parameters of the Pipelines API or some other way
# to create typed objects.
#
# API constants:
_API_PIPELINE = 'pipeline'
_API_ACTIONS = 'actions'

# The expected path of the run_vep.sh script in the docker container.
_VEP_RUN_SCRIPT = '/opt/variant_effect_predictor/run_vep.sh'

# The local name of the output file and directory for VEP runs.
_LOCAL_OUTPUT_DIR = '/mnt/vep/output_files'
_LOCAL_OUTPUT_FILE = _LOCAL_OUTPUT_DIR + '/output.vcf'

# The time between operation polling rounds.
_POLLING_INTERVAL_SECONDS = 30


def create_runner_and_update_args(known_args, pipeline_args):
  # type: (argparse.Namespace, List[str]) -> VepRunner
  """Creates an instance of VepRunner using the provided args and updates them.

  In particular, the two arguments that are updated are `input_pattern` and
  `annotation_fields`.

  Args:
    known_args: The list of arguments defined in `variant_transform_options`.
    pipeline_args: The list of remaining arguments meant to be used to
      determine resources like number of workers, machine type, etc.
  """
  credentials = client.GoogleCredentials.get_application_default()
  pipeline_service = discovery.build(
      'genomics', 'v2alpha1', credentials=credentials)
  runner = VepRunner(
      pipeline_service, known_args.input_pattern,
      known_args.annotation_output_dir, known_args.vep_info_field,
      known_args.vep_image_uri, known_args.vep_cache_path,
      known_args.vep_num_fork, pipeline_args)
  known_args.input_pattern = runner.get_output_pattern()
  if known_args.annotation_fields:
    known_args.annotation_fields.append(known_args.vep_info_field)
  else:
    known_args.annotation_fields = [known_args.vep_info_field]
  return runner


class VepRunner(object):
  """A class for running vep through Pipelines API on a set of input files."""

  def __init__(
      self,
      pipeline_service,  # type: discovery.Resource
      input_pattern,  # type: str
      output_dir,  # type: str
      vep_info_field,  # tyep: str
      vep_image_uri,  # type: str
      vep_cache_path,  # type: str
      vep_num_fork,  # type: int
      pipeline_args  # List[str]
      ):
    # type: (...) -> None
    """Constructs an instance for running VEP.

    Note that external users of this class can use create_runner_and_update_args
    function of this module to create an instance of this class from flags.

    Args:
      input_pattern: The pattern to identify all input files.
      output_dir: The location for all output files. This is expected not to
        exist and is created in the process of running VEP pipelines.
      vep_image_uri: The URI of the image that contains VEP.
      vep_cache_path: The URI for the cache file on GCS.
      vep_num_fork: The value of the --fork argument for running VEP.
      pipeline_args: The list of arguments that are meant to be used when
        running Beam; for simplicity we use the same arguments to decide how
        many and what type of workers to use, where to run, etc.
    """
    self._pipeline_service = pipeline_service
    self._vep_image_uri = vep_image_uri
    self._vep_cache_path = vep_cache_path
    self._vep_num_fork = vep_num_fork
    self._input_pattern = input_pattern
    self._output_dir = output_dir
    self._vep_info_field = vep_info_field
    self._process_pipeline_args(pipeline_args)
    self._running_operation_ids = []  # type: List[str]

  def get_output_pattern(self):
    # type: () -> str
    return vep_runner_util.get_output_pattern(self._output_dir)

  def _get_api_request_fixed_parts(self):
    # type: () -> Dict
    """Returns the part of API request that is fixed between actions.

    This includes setting up VEP cache, virtual machine setup, etc. The variant
    parts are the `commands` for processing each file and should be added before
    sending the API request.
    """
    return {
        _API_PIPELINE: {
            _API_ACTIONS: [
                self._make_action('mkdir', '-p', '/mnt/vep/vep_cache'),
                self._make_action('gsutil', '-q', 'cp', self._vep_cache_path,
                                  '/mnt/vep/vep_cache/')
            ],
            'environment': {
                'VEP_CACHE': '/mnt/vep/vep_cache/{}'.format(
                    vep_runner_util.get_base_name(self._vep_cache_path)),
                'NUM_FORKS': str(self._vep_num_fork),
                'VCF_INFO_FILED': self._vep_info_field,
                # TODO(bashir2): Decide how to do proper reference validation,
                # the following --check_ref just drops variants that have
                # wrong REF. If there are too many of them, it indicates that
                # VEP database for a wrong reference sequence is being used
                # and this has to caught and communicated to the user.
                'OTHER_VEP_OPTS':
                    '--everything --check_ref --allow_non_variant',
            },
            'resources': {
                'projectId': self._project,
                'virtualMachine': {
                    'disks': [
                        {
                            'name': 'vep',
                            'sizeGb': _MINIMUM_DISK_SIZE_GB
                        }
                    ],
                    'machineType': self._machine_type,
                    # TODO(bashir2): Add the option of using preemptible
                    # machines and the retry functionality.
                    'preemptible': False,
                    'serviceAccount': {
                        'scopes': [
                            'https://www.googleapis.com/auth/'
                            'devstorage.read_write']
                    }
                },
                'regions': [self._region]
            }
        }
    }

  def _make_action(self, *args, **kwargs):
    # type: (str, str) -> Dict
    command_args = list(args)
    action = {
        'commands': command_args,
        'imageUri': self._vep_image_uri,
        'mounts': [{'disk': 'vep', 'path': '/mnt/vep'}]
    }
    action.update(kwargs)
    # TODO(bashir2): Add a proper `label` based on command arguments.
    return action

  def _process_pipeline_args(self, pipeline_args):
    # type: (List[str]) -> None
    flags_dict = pipeline_options.PipelineOptions(
        pipeline_args).get_all_options()
    self._project = self._get_flag(flags_dict, 'project')
    self._region = self._get_flag(flags_dict, 'region')
    # TODO(bahsir2): Fix the error messages of _check_flag since
    # --worker_machine_type has dest='machine_type'.
    self._machine_type = self._get_flag(flags_dict, 'machine_type')
    # TODO(bashir2): Fall back to num_workers if max_num_workers is not set.
    self._max_num_workers = self._get_flag(flags_dict, 'max_num_workers')
    if self._max_num_workers <= 0:
      raise ValueError(
          '--max_num_workers should be a positive number, got: {}'.format(
              self._max_num_workers))

  def _get_flag(self, pipeline_flags, flag):
    # type: (Dict[str, Any], str) -> Any
    if flag not in pipeline_flags or not pipeline_flags[flag]:
      raise ValueError('Could not find {} among pipeline flags {}'.format(
          flag, pipeline_flags))
    logging.info('Using %s flag: %s.', flag, pipeline_flags[flag])
    return pipeline_flags[flag]

  def wait_until_done(self):
    """Polls currently running operations and waits until all are done."""
    while self._running_operation_ids:
      self._running_operation_ids = [op for op in self._running_operation_ids
                                     if not self._is_done(op)]
      if self._running_operation_ids:
        time.sleep(_POLLING_INTERVAL_SECONDS)

  def _is_done(self, operation):
    # type: (str) -> bool
    # TODO(bashir2): Catch exceptions in the following call and also silence the
    # log messages of googleapiclient.discovery module for the next call of the
    # API since they flood the log file.
    # pylint: disable=no-member
    request = self._pipeline_service.projects().operations().get(name=operation)
    is_done = request.execute()['done']
    # TODO(bashir2): Add better monitoring and log progress within each
    # operation instead of just checking `done`.
    if is_done:
      logging.info('Operation %s is done.', operation)
    return is_done

  def run_on_all_files(self):
    # type: () -> None
    """Runs VEP on all input files.

    The input files structure is recreated under `self._output_dir` and each
    output file will have `_VEP_OUTPUT_SUFFIX`.
    """
    if self._running_operation_ids:
      raise AssertionError('There are already {} operations running.'.format(
          len(self._running_operation_ids)))
    logging.info('Finding all files that match %s', self._input_pattern)
    match_results = filesystems.FileSystems.match(
        [self._input_pattern])  # type: List[filesystem.MatchResult]
    if not match_results:
      raise ValueError('No files matched input_pattern: {}'.format(
          self._input_pattern))
    logging.info('Number of files: %d', len(match_results[0].metadata_list))
    self._check_and_write_to_output_dir(self._output_dir)
    single_vm_actions_list = vep_runner_util.disribute_files_on_workers(
        match_results[0].metadata_list, self._output_dir, self._max_num_workers)
    for vm_index, actions in enumerate(single_vm_actions_list):
      operation_name = self._call_pipelines_api(
          actions, self._get_output_log_path(self._output_dir, vm_index))
      logging.info('Started operation %s on VM %d processing %d input files',
                   operation_name, vm_index, len(actions.io_map))
      self._running_operation_ids.append(operation_name)

  def _call_pipelines_api(self, single_vm_actions, output_log_path):
    # type: (vep_runner_util.SingleWorkerActions, str) -> str
    api_request = self._get_api_request_fixed_parts()
    size_gb = single_vm_actions.disk_size_bytes / (1 << 30)
    api_request[_API_PIPELINE]['resources'][
        'virtualMachine']['disks'][0]['sizeGb'] = (
            size_gb + _MINIMUM_DISK_SIZE_GB)
    for input_file, output_file in single_vm_actions.io_map.iteritems():
      api_request[_API_PIPELINE][_API_ACTIONS].extend(
          self._create_actions(input_file, output_file))
    api_request[_API_PIPELINE][_API_ACTIONS].append(
        self._make_action('gsutil', '-q', 'cp',
                          '/google/logs/output',
                          output_log_path,
                          flags=['ALWAYS_RUN']))
    # pylint: disable=no-member
    request = self._pipeline_service.pipelines().run(body=api_request)
    operation_name = request.execute()['name']
    return operation_name

  def _check_and_write_to_output_dir(self, output_dir):
    # type: (str) -> None
    real_dir = vep_runner_util.format_dir_path(output_dir)
    # NOTE(bashir2): We cannot use exists() because for example on GCS, the
    # directory names are only symbolic and are not physical files.
    match_results = filesystems.FileSystems.match(['{}*'.format(real_dir)])
    if match_results and match_results[0].metadata_list:
      raise ValueError('Output directory {} already exists.'.format(real_dir))
    log_file = filesystems.FileSystems.create(
        filesystems.FileSystems.join(output_dir, _GLOBAL_LOG_FILE))
    # TODO(bashir2): Instead of just creating an empty file, log some
    # information about how the VEP pipelines are executed.
    log_file.close()

  def _get_output_log_path(self, output_dir, vm_index):
    # type: (str, int) -> str
    return '{}/logs/output_VM_{}.log'.format(output_dir, vm_index)

  def _create_actions(self, input_file, output_file):
    # type: (str, str) -> List
    local_input_file = '/mnt/vep/{}'.format(
        vep_runner_util.get_base_name(input_file))
    return [
        self._make_action('gsutil', '-q', 'cp', input_file, local_input_file),
        self._make_action('rm', '-r', '-f', _LOCAL_OUTPUT_DIR),
        self._make_action(_VEP_RUN_SCRIPT, local_input_file,
                          _LOCAL_OUTPUT_FILE),
        # TODO(bashir2): When the output files are local, the output directory
        # structure should be created as well otherwise gsutil fails.
        self._make_action('gsutil', '-q', 'cp', _LOCAL_OUTPUT_FILE,
                          output_file)]
