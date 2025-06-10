import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from apache_beam.io import filesystems
from googleapiclient import discovery
from oauth2client import client

def create_runner(known_args, pipeline_args, input_pattern, watchdog_file, watchdog_file_update_interval_seconds):
  """Returns an instance of BatchVepRunner using the provided args."""
  from apache_beam.options import pipeline_options
  flags_dict = pipeline_options.PipelineOptions(pipeline_args).get_all_options()

  project = flags_dict.get("project")
  region = flags_dict.get("region")
  service_account = flags_dict.get("service_account_email", "default")
  machine_type = flags_dict.get("machine_type", "e2-standard-4")

  return BatchVepRunner(
      project=project,
      location=region,
      species=known_args.vep_species,
      assembly=known_args.vep_assembly,
      input_pattern=input_pattern,
      output_dir=known_args.annotation_output_dir,
      vep_info_field=known_args.vep_info_field,
      vep_image_uri=known_args.vep_image_uri,
      vep_cache_path=known_args.vep_cache_path,
      vep_num_fork=known_args.vep_num_fork,
      service_account=service_account,
      machine_type=machine_type,
      watchdog_file=watchdog_file,
      watchdog_interval=watchdog_file_update_interval_seconds
  )

class BatchVepRunner:
  def __init__(
      self,
      project: str,
      location: str,
      species: str,
      assembly: str,
      input_pattern: str,
      output_dir: str,
      vep_info_field: str,
      vep_image_uri: str,
      vep_cache_path: str,
      vep_num_fork: int,
      service_account: str,
      machine_type: str = "e2-standard-4",
      watchdog_file: Optional[str] = None,
      watchdog_interval: int = 30
  ):
    credentials = client.GoogleCredentials.get_application_default()
    try:
      self._batch_service = discovery.build('batch', 'v1', credentials=credentials)
      logging.info("Successfully built Google Batch API service client.")
    except Exception as e:
      logging.error("Failed to build Google Batch API service client: %s", e)
      raise

    self._project = project
    self._location = location
    self._species = species
    self._assembly = assembly
    self._input_pattern = input_pattern
    self._output_dir = output_dir
    self._vep_info_field = vep_info_field
    self._vep_image_uri = vep_image_uri
    self._vep_cache_path = vep_cache_path
    self._vep_num_fork = vep_num_fork
    self._service_account = service_account
    self._machine_type = machine_type
    self._watchdog_file = watchdog_file
    self._watchdog_interval = watchdog_interval
    self._job_id = None

  def _get_matched_files(self) -> List[str]:
    matches = filesystems.FileSystems.match([self._input_pattern])
    if not matches:
      raise ValueError(f"No files matched input_pattern: {self._input_pattern}")
    return [m.path for m in matches[0].metadata_list]

  def _generate_job_name(self):
    return f"vep-job-{uuid.uuid4().hex[:8]}"

  def _build_batch_job(self, io_pairs: List[Dict[str, str]]) -> Dict[str, Any]:
    job_name = self._generate_job_name()
    runnables = []
    for pair in io_pairs:
      input_file = pair["input"]
      output_file = pair["output"]
      commands = [
        "/opt/variant_effect_predictor/run_vep.sh",
      ]
      if self._watchdog_file:
        commands = [
          "/opt/variant_effect_predictor/run_script_with_watchdog.sh",
          "/opt/variant_effect_predictor/run_vep.sh",
          str(self._watchdog_interval),
          self._watchdog_file,
          input_file,
          output_file
        ]
      runnables.append({
        "container": {
            "imageUri": self._vep_image_uri,
            "commands": commands,

        },
      })

    job_spec = {
      "name": job_name,
      "taskGroups": [
        {
          "taskSpec": {
            "runnables": runnables,
            "environment": {
                "variables": {
                    "GENOME_ASSEMBLY": self._assembly,
                    "SPECIES": self._species,
                    "VEP_CACHE": self._vep_cache_path,
                    "NUM_FORKS": str(self._vep_num_fork),
                    "VCF_INFO_FILED": self._vep_info_field,
                    "OTHER_VEP_OPTS": "--everything --check_ref --allow_non_variant --format vcf"
                },
            },
          },
          "taskCount": len(runnables),
          "parallelism": len(runnables)
        }
      ],
      "allocationPolicy": {
        "instances": [
          {
            "policy": {
              "machineType": self._machine_type
            }
          }
        ],
        "serviceAccount": {
          "email": self._service_account
        }
      },
      "logsPolicy": {
        "destination": "CLOUD_LOGGING"
      }
    }

    # Validate expected fields in job_spec
    required_keys = ["name", "taskGroups", "allocationPolicy", "logsPolicy"]
    for key in required_keys:
      if key not in job_spec:
        raise ValueError(f"Missing required key in job spec: {key}")

    return job_spec

  def run_on_all_files(self):
    matched_files = self._get_matched_files()
    io_pairs = []
    for input_file in matched_files:
      output_file = f"{self._output_dir}/{input_file.split('/')[-1]}"
      io_pairs.append({"input": input_file, "output": output_file})

    job_body = self._build_batch_job(io_pairs)
    parent = f"projects/{self._project}/locations/{self._location}"

    try:
      result = self._batch_service.projects().locations().jobs().create(
        parent=parent, body=job_body).execute()
      self._job_id = result["name"]
      logging.info("Submitted batch job %s", self._job_id)
    except Exception as e:
      logging.error("Failed to submit batch job: %s", e)
      raise

  def wait_until_done(self):
    if not self._job_id:
      raise RuntimeError("No job submitted. Call run_on_all_files first.")
    while True:
      job = self._batch_service.projects().locations().jobs().get(
        name=self._job_id).execute()
      status = job.get("status", {}).get("state")
      if status in ("SUCCEEDED", "FAILED", "DELETION_IN_PROGRESS"):
        logging.info("Job %s completed with status %s", self._job_id, status)
        break
      time.sleep(30)
