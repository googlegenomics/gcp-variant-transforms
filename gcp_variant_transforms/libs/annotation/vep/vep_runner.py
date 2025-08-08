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


import argparse  # pylint: disable=unused-import
import uuid
import logging
import time
import json
from typing import Any, Dict, List, Optional  # pylint: disable=unused-import
import math

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
_GLOBAL_LOG_FILE = "VEP_run_info.log"

# TODO(bashir2): Check if instead of raw strings, we can use a protocol
# buffer describing the parameters of the Pipelines API or some other way
# to create typed objects.
#
# API constants:
_API_PIPELINE = "pipeline"
_API_ACTIONS = "actions"
_API_TASKGROUPS = "taskGroups"
_API_TASKSPEC = "taskSpec"
_API_RUNNABLES = "runnables"

# The following image wraps gsutil with additional retry logic.
_GSUTIL_IMAGE = "gcr.io/google.com/cloudsdktool/cloud-sdk:slim"
# The expected path of the run_vep.sh script in the docker container.
_VEP_RUN_SCRIPT = "/opt/variant_effect_predictor/run_vep.sh"

# The expected path of `run_script_with_watchdog.sh` script in the
# docker container. We use this script to run `_VEP_RUN_SCRIPT` in background
# and kill it in case of failure or cancellation.
_WATCHDOG_RUNNER_SCRIPT = "/opt/variant_effect_predictor/run_script_with_watchdog.sh"
# The local name of the output file and directory for VEP runs.
_LOCAL_OUTPUT_DIR = "/mnt/disks/vep/output_files"
_LOCAL_OUTPUT_FILE = _LOCAL_OUTPUT_DIR + "/{}_vep_output.vcf"

# The time between operation polling rounds.
_POLLING_INTERVAL_SECONDS = 30

# Maximum number of times to retry API calls in case of server side errors.
_NUMBER_OF_API_CALL_RETRIES = 5


def create_runner(
    known_args,
    pipeline_args,
    input_pattern,
    watchdog_file,
    watchdog_file_update_interval_seconds,
):
    # type: (argparse.Namespace, List[str], str, Optional[str], int) -> VepRunner
    """Returns an instance of VepRunner using the provided args.

    Args:
      known_args: The list of arguments defined in `variant_transform_options`.
      pipeline_args: The list of remaining arguments meant to be used to
        determine resources like number of workers, machine type, etc.
      input_pattern: The VCF files to be annotated.
      watchdog_file: The file that will be updated by the Dataflow worker every
        `watchdog_file_update_interval_seconds`. Once the file is found to be
        stale, the VEP process running in the VM will be killed.
      watchdog_file_update_interval_seconds: The `watchdog_file` will be updated
        by the Dataflow worker every `watchdog_file_update_interval_seconds`.
    """
    credentials = client.GoogleCredentials.get_application_default()
    pipeline_service = discovery.build("batch", "v1", credentials=credentials)
    runner = VepRunner(
        pipeline_service,
        known_args.location,
        known_args.vep_species,
        known_args.vep_assembly,
        input_pattern,
        known_args.annotation_output_dir,
        known_args.vep_info_field,
        known_args.vep_image_uri,
        known_args.vep_cache_path,
        known_args.vep_num_fork,
        pipeline_args,
        watchdog_file,
        watchdog_file_update_interval_seconds,
        known_args.number_of_runnables_per_job
    )
    return runner


class VepRunner:
    """A class for running vep through Batch API on a set of input files."""

    _VEP_CACHE_BASE = (
        "gs://variant-data/vep/" "vep_cache_{species}_{assembly}_104.tar.gz"
    )

    def __init__(
        self,
        pipeline_service,  # type: discovery.Resource
        location,  # type: str
        species,  # type: str
        assembly,  # type: str
        input_pattern,  # type: str
        output_dir,  # type: str
        vep_info_field,  # type: str
        vep_image_uri,  # type: str
        vep_cache_path,  # type: str
        vep_num_fork,  # type: int
        pipeline_args,  # type: List[str]
        watchdog_file,  # type: Optional[str]
        watchdog_file_update_interval_seconds,  # type: int
        number_of_runnables_per_job # type: int
    ):
        # type: (...) -> None
        """Constructs an instance for running VEP.

        Note that external users of this class can use create_runner_and_update_args
        function of this module to create an instance of this class from flags.

        Args:
        location: The Batch API location to use.
        input_pattern: The pattern to identify all input files.
        output_dir: The location for all output files. This is expected not to
            exist and is created in the process of running VEP jobs.
        vep_image_uri: The URI of the image that contains VEP.
        vep_cache_path: The URI for the cache file on GCS.
        vep_num_fork: The value of the --fork argument for running VEP.
        pipeline_args: The list of arguments that are meant to be used when
            running Beam; for simplicity we use the same arguments to decide how
            many and what type of workers to use, where to run, etc.
        watchdog_file: The file that will be updated by the Dataflow worker every
            `watchdog_file_update_interval_seconds`. Once the file is found to be
            stale, the VEP process running in the VM will be killed.
        watchdog_file_update_interval_seconds: The `watchdog_file` will be updated
            by the Dataflow worker every `watchdog_file_update_interval_seconds`.
        """
        self._pipeline_service = pipeline_service
        self._location = location
        self._species = species
        self._assembly = assembly
        self._vep_image_uri = vep_image_uri
        self._vep_cache_path = self._make_vep_cache_path(vep_cache_path)
        self._vep_num_fork = vep_num_fork
        self._input_pattern = input_pattern
        self._output_dir = output_dir
        self._vep_info_field = vep_info_field
        self._process_pipeline_args(pipeline_args)
        self._watchdog_file = watchdog_file
        self._watchdog_file_update_interval_seconds = (
            watchdog_file_update_interval_seconds
        )
        self._running_operation_ids = []  # type: List[str]
        self._operation_name_to_io_infos = {}
        self._operation_name_to_logs = {}
        self._max_num_workers = 10
        self._number_of_runnables_per_job = number_of_runnables_per_job

    def _make_vep_cache_path(self, vep_cache_path):
        # type: (str) -> str
        if not vep_cache_path:
            vep_cache_path = VepRunner._VEP_CACHE_BASE.format(
                species=self._species, assembly=self._assembly
            )
        return vep_cache_path

    def get_output_pattern(self):
        # type: () -> str
        return vep_runner_util.get_output_pattern(self._output_dir)

    def _get_batch_job_definition(self):
        # type: () -> Dict
        """Returns a Batch v1 API job definition with fixed setup steps."""
        job_name = self._generate_job_name()
        return {
            "name": job_name,
            "taskGroups": [
                {
                    "taskSpec": {
                        "runnables": [
                            self._make_runnable(
                                self._vep_image_uri,
                                "mkdir",
                                "-p",
                                "/mnt/disks/vep/vep_cache",
                            ),
                            self._make_runnable(
                                _GSUTIL_IMAGE,
                                "sh",
                                "-c",
                                f"gsutil cp {self._vep_cache_path} /mnt/disks/vep/vep_cache/{_get_base_name(self._vep_cache_path)} 2>&1",
                            ),
                        ],
                        "environment": {
                            "variables": {
                                "GENOME_ASSEMBLY": self._assembly,
                                "SPECIES": self._species,
                                "VEP_CACHE": "/mnt/disks/vep/vep_cache/{}".format(
                                    _get_base_name(self._vep_cache_path)
                                ),
                                "NUM_FORKS": str(self._vep_num_fork),
                                "VCF_INFO_FILED": self._vep_info_field,
                                "OTHER_VEP_OPTS": "--everything --check_ref --allow_non_variant --format vcf",
                            }
                        },
                        "volumes": [
                            {
                                "deviceName": "vep",
                                "mountPath": "/mnt/disks/vep",
                                "mountOptions": "rw,async",
                            }
                        ],
                        "computeResource": {
                            "cpuMilli": 4000,
                            "memoryMib": 8192,
                            "bootDiskMib": _MINIMUM_DISK_SIZE_GB,
                        },
                        "maxRunDuration": "3600s",
                    },
                    "taskCount": 1,
                    "parallelism": 1,
                }
            ],
            "allocationPolicy": {
                "serviceAccount": {
                    "email": self._service_account,
                    "scopes": ["https://www.googleapis.com/auth/devstorage.read_write"],
                },
                "instances": [
                    {
                        "policy": {
                            "machineType": self._machine_type,
                            "disks": [
                                {
                                    "newDisk": {
                                        "sizeGb": _MINIMUM_DISK_SIZE_GB,
                                        "type": "pd-ssd",
                                    },
                                    "deviceName": "vep",
                                }
                            ],
                        },
                    }
                ],
            },
            "logsPolicy": {
                  "destination": "CLOUD_LOGGING"
                # "destination": "PATH",
                # "logsPath": "/mnt/disks/vep/logs",
            },
            "labels": {"job": "vep-setup"},
        }

    def _make_runnable(self, image_uri, *args, **kwargs):
        # type: (str, str, *str, **Any) -> Dict
        """Creates a Batch API Runnable dictionary."""
        runnable = {
            "container": {
                "imageUri": image_uri,
                "volumes": ["/mnt/disks/vep:/mnt/disks/vep"],
                "commands": list(args),
            },
        }

        runnable.update(kwargs)
        return runnable

    def _process_pipeline_args(self, pipeline_args):
        # type: (List[str]) -> None
        flags_dict = pipeline_options.PipelineOptions(pipeline_args).get_all_options()
        self._project = self._get_flag(flags_dict, "project")
        # TODO(bahsir2): Fix the error messages of _check_flag since
        # --worker_machine_type has dest='machine_type'.
        try:
            self._machine_type = self._get_flag(flags_dict, "machine_type")
        except ValueError:
            self._machine_type = self._get_machine_type_from_fork()
            self._max_num_workers = self._get_flag(
                flags_dict, "max_num_workers", "num_workers"
            )
            if self._max_num_workers <= 0:
                raise ValueError(
                    "--max_num_workers and --num_workers should be positive numbers, "
                    "got: {}".format(self._max_num_workers)
                )
        try:
            self._service_account = self._get_flag(flags_dict, "service_account_email")
        except ValueError:
            self._service_account = "default"

    def _get_flag(self, pipeline_flags, *expected_flags):
        # type: (Dict[str, Any], str) -> Any
        for flag in expected_flags:
            if flag in pipeline_flags and pipeline_flags[flag]:
                logging.info(
                    "Using %s flag for annotation run: %s.", flag, pipeline_flags[flag]
                )
                return pipeline_flags[flag]
        raise ValueError(
            "Could not find any of {} among pipeline flags {}".format(
                expected_flags, pipeline_flags
            )
        )

    def _generate_job_name(self):
        return f"vep-job-{uuid.uuid4().hex[:8]}"

    def _get_machine_type_from_fork(self):
        # type: () -> str
        if self._vep_num_fork == 1:
            return "n1-standard-1"
        elif self._vep_num_fork == 2:
            return "n1-standard-2"
        elif self._vep_num_fork <= 4:
            return "n1-standard-4"
        elif self._vep_num_fork <= 8:
            return "n1-standard-8"
        else:
            # This is just a heuristic since after a certain point having more cores
            # does not help VEP performance much more because of its file I/O.
            return "n1-standard-16"

    def wait_until_done(self):
        # type: () -> None
        """Polls currently running operations and waits until all are done.

        In case of failure, retry the operation for at most
        `_NUMBER_OF_API_CALL_RETRIES` times.
        """
        num_retries = 0
        while num_retries < _NUMBER_OF_API_CALL_RETRIES:
            if not self._running_operation_ids:
                return
            self._running_operation_ids = self._wait_and_retry_operations()
            num_retries += 1

        raise RuntimeError(
            "Annotations for the input {} failed after {} "
            "retries.".format(self._input_pattern, _NUMBER_OF_API_CALL_RETRIES)
        )

    def _wait_and_retry_operations(self):
        # type: () -> Optional(List[str])
        """Waits currently running operations and retries when the operation failed.

        Args:
        running_operation_ids: currently running PAPI operation ids.

        Returns:
        A list of retry operation ids.
        """
        retry_operation_ids = []
        for operation in self._running_operation_ids:
            while not self._is_done(operation):
                time.sleep(_POLLING_INTERVAL_SECONDS)
            error_message = self._get_error_message(operation)
            if error_message:
                new_operation_name = self._retry_operation(operation, error_message)
                retry_operation_ids.append(new_operation_name)
        return retry_operation_ids

    def _retry_operation(self, operation, error_message):
        # type: (str, str) -> str
        """Returns retry operation id."""
        io_infos = self._operation_name_to_io_infos.get(operation)
        logs = self._operation_name_to_logs.get(operation)
        retry_logs = logs + "retry"
        retry_operation_id = self._call_pipelines_api(io_infos)
        self._operation_name_to_io_infos.update({retry_operation_id: io_infos})
        self._operation_name_to_logs.update({retry_operation_id: retry_logs})

        logging.warning(
            "Annotation job failed for the operation %s with error: "
            "%s. Please check the log file (%s) for more information."
            "Retrying with operation id %s.",
            operation,
            error_message,
            logs,
            retry_operation_id,
        )
        return retry_operation_id

    def _is_done(self, operation):
        # type: (str) -> bool
        # TODO(bashir2): Silence the log messages of googleapiclient.discovery
        # module for the next call of the API since they flood the log file.
        # pylint: disable=no-member
        request = (
            self._pipeline_service.projects().locations().jobs().get(name=operation)
        )
        job = request.execute(num_retries=_NUMBER_OF_API_CALL_RETRIES)
        state = job.get("status", {}).get("state")
        # TODO(bashir2): Add better monitoring and log progress within each
        # operation instead of just checking `done`.
        logging.info("Job %s state: %s", operation, state)
        return state in ("SUCCEEDED", "FAILED", "DELETION_IN_PROGRESS")

    def _get_error_message(self, operation):
        request = (
            self._pipeline_service.projects().locations().jobs().get(name=operation)
        )
        job = request.execute(num_retries=_NUMBER_OF_API_CALL_RETRIES)
        if job.get("status", {}).get("state") == "FAILED":
            events = job["status"].get("statusEvents", [])
            if events:
                return events[-1].get("description", "No detailed error message.")
            return "Job failed without detailed error."
        return ""

    def _calculate_chunk_size(self, list_file: List) -> List:
        """ Calculates the chunk size for the input files.
        The input files are split into chunks of size
        Args:
            list_file (list): List of input files to be processed.

        Returns:
            List: List of chunks, each containing a subset of the input files.
        """
        ceil = int(math.ceil(len(list_file) / self._number_of_runnables_per_job))
        chunks = [
            list_file[i * self._number_of_runnables_per_job : (i + 1) * self._number_of_runnables_per_job]
            for i in range(0, ceil)
        ]

        return chunks

    def run_on_all_files(self):
        # type: () -> None
        """Runs VEP on all input files.

        The input files structure is recreated under `self._output_dir` and each
        output file will have `_VEP_OUTPUT_SUFFIX`.
        """
        if self._running_operation_ids:
            raise AssertionError(
                "There are already {} operations running.".format(
                    len(self._running_operation_ids)
                )
            )
        logging.info("Finding all files that match %s", self._input_pattern)
        match_results = filesystems.FileSystems.match(
            [self._input_pattern]
        )  # type: List[filesystem.MatchResult]
        if not match_results:
            raise ValueError(
                "No files matched input_pattern: {}".format(self._input_pattern)
            )
        logging.info("Number of files: %d", len(match_results[0].metadata_list))

        chunks = self._calculate_chunk_size(match_results[0].metadata_list)
        logging.info("Found %d chunks of files to process, each with at most %d files.", len(chunks), self._number_of_runnables_per_job)

        for i, chunk in enumerate(chunks):
            logging.info("Chunk %d/%d: %d files", i + 1, len(chunks), len(chunk))
            vm_io_info = vep_runner_util.get_all_vm_io_info(
                chunk, self._output_dir, self._max_num_workers
            )

            for vm_index, io_info in enumerate(vm_io_info):
                output_log_path = self._get_output_log_path(self._output_dir, vm_index)
                operation_name = self._call_pipelines_api(io_info)
                self._operation_name_to_io_infos.update({operation_name: io_info})
                self._operation_name_to_logs.update({operation_name: output_log_path})

                logging.info(
                    "Started operation %s on VM %d processing %d input files",
                    operation_name,
                    vm_index,
                    len(io_info.io_map),
                )
                self._running_operation_ids.append(operation_name)


        # vm_io_info = vep_runner_util.get_all_vm_io_info(
        #     match_results[0].metadata_list, self._output_dir, self._max_num_workers
        # )
        # for vm_index, io_info in enumerate(vm_io_info):
        #     output_log_path = self._get_output_log_path(self._output_dir, vm_index)
        #     operation_name = self._call_pipelines_api(io_info)
        #     self._operation_name_to_io_infos.update({operation_name: io_info})
        #     self._operation_name_to_logs.update({operation_name: output_log_path})

        #     logging.info(
        #         "Started operation %s on VM %d processing %d input files",
        #         operation_name,
        #         vm_index,
        #         len(io_info.io_map),
        #     )
        #     self._running_operation_ids.append(operation_name)

    def _call_pipelines_api(self, io_infos):
        # type: (vep_runner_util.SingleWorkerActions, str) -> str
        api_request = self._get_batch_job_definition()
        size_gb = io_infos.disk_size_bytes // (1 << 30)
        api_request["allocationPolicy"]["instances"][0]["policy"]["disks"][0][
            "newDisk"
        ]["sizeGb"] = (size_gb + _MINIMUM_DISK_SIZE_GB)

        api_request[_API_TASKGROUPS][0][_API_TASKSPEC][_API_RUNNABLES].extend(
            self._create_runnables(io_infos)
        )

        # pylint: disable=no-member
        parent = "projects/{}/locations/{}".format(self._project, self._location)
        request = (
            self._pipeline_service.projects()
            .locations()
            .jobs()
            .create(parent=parent, body=api_request)
        )
        operation_name = request.execute(num_retries=_NUMBER_OF_API_CALL_RETRIES)[
            "name"
        ]
        return operation_name

    def _check_and_write_to_output_dir(self, output_dir):
        # type: (str) -> None
        real_dir = vep_runner_util.format_dir_path(output_dir)
        # NOTE(bashir2): We cannot use exists() because for example on GCS, the
        # directory names are only symbolic and are not physical files.
        match_results = filesystems.FileSystems.match(["{}*".format(real_dir)])
        if match_results and match_results[0].metadata_list:
            raise ValueError("Output directory {} already exists.".format(real_dir))
        log_file = filesystems.FileSystems.create(
            filesystems.FileSystems.join(output_dir, _GLOBAL_LOG_FILE)
        )
        # TODO(bashir2): Instead of just creating an empty file, log some
        # information about how the VEP pipelines are executed.
        log_file.close()

    def _get_output_log_path(self, output_dir, vm_index):
        # type: (str, int) -> str
        log_filename = "output_VM_{}.log".format(vm_index)
        return filesystems.FileSystems.join(output_dir, "logs", log_filename)

    def _create_runnables(self, io_infos) -> list:
        """Creates a list of Batch v1 `runnables` for processing one input/output pair."""
        local_input_dir = "/mnt/disks/vep/"
        input_path = self._input_pattern.rstrip("**")
        vep_output_dir = f"{self._output_dir}/{input_path.replace('gs://', '', 1)}"
        runnables = []

        # Copy the input file to the local disk of the VM.
        runnables.append(
            self._make_runnable(
                _GSUTIL_IMAGE,
                "sh",
                "-c",
                f"gsutil -m rsync -r {input_path} {local_input_dir} 2>&1",
            )
        )

        # Remove the output directory if it exists.
        # This is needed to ensure that the output directory is empty before
        # running VEP.
        runnables.append(
            self._make_runnable(
                self._vep_image_uri, "rm", "-r", "-f", _LOCAL_OUTPUT_DIR
            )
        )

        # Run VEP
        for input_file, output_file in io_infos.io_map.items():
            base_input = "/".join(input_file.split("/")[-2:])
            local_output_file = _LOCAL_OUTPUT_FILE.format(base_input)
            runnables.append(
                self._make_runnable(
                    self._vep_image_uri,
                    _VEP_RUN_SCRIPT,
                    local_input_dir + base_input,
                    local_output_file,
                )
            )

        # TODO(nhon): Add watchdog
        # if self._watchdog_file:
        #     runnables.append(self._make_runnable(
        #         self._vep_image_uri,
        #         _WATCHDOG_RUNNER_SCRIPT,
        #         _VEP_RUN_SCRIPT,
        #         str(self._watchdog_file_update_interval_seconds),
        #         self._watchdog_file,
        #         local_input_file,
        #         _LOCAL_OUTPUT_FILE
        #     ))
        # else:
        #     runnables.append(self._make_runnable(
        #         self._vep_image_uri,
        #         _VEP_RUN_SCRIPT,
        #         local_input_file,
        #         _LOCAL_OUTPUT_FILE
        #     ))

        # Copy the output file to the output directory.
        runnables.append(
            self._make_runnable(
                _GSUTIL_IMAGE,
                "sh",
                "-c",
                f"gsutil -m rsync -r -x \".*_vep_output\.vcf_summary.html$\" {_LOCAL_OUTPUT_DIR} {vep_output_dir} 2>&1",
            )
        )

        return runnables

    def _create_actions(self, input_file, output_file):
        # type: (str, str) -> List
        local_input_file = "/mnt/vep/{}".format(_get_base_name(input_file))
        if self._watchdog_file:
            action = self._make_action(
                self._vep_image_uri,
                _WATCHDOG_RUNNER_SCRIPT,
                _VEP_RUN_SCRIPT,
                str(self._watchdog_file_update_interval_seconds),
                self._watchdog_file,
                local_input_file,
                _LOCAL_OUTPUT_FILE,
            )
        else:
            action = self._make_action(
                self._vep_image_uri,
                _VEP_RUN_SCRIPT,
                local_input_file,
                _LOCAL_OUTPUT_FILE,
            )
        return [
            self._make_action(
                _GSUTIL_IMAGE, "gsutil", "cp", input_file, local_input_file
            ),
            self._make_action(self._vep_image_uri, "rm", "-r", "-f", _LOCAL_OUTPUT_DIR),
            action,
            # TODO(bashir2): When the output files are local, the output directory
            # structure should be created as well otherwise gsutil fails.
            self._make_action(
                _GSUTIL_IMAGE, "gsutil", "cp", _LOCAL_OUTPUT_FILE, output_file
            ),
        ]


def _get_base_name(file_path):
    # type: (str) -> str
    """Used when we want to copy files to local machines.

    Keeping the file names, gives more context to actions. For example if
    `file_path` is 'gs://my_bucket/my_input.vcf', tis returns 'my_input.vcf'.

    Returns:
      The basename of the input `file_path`.
    """
    _, base_path = filesystems.FileSystems.split(file_path)
    if not base_path:
        raise ValueError(
            "Cannot extract base path from the input path {}".format(file_path)
        )
    return base_path
