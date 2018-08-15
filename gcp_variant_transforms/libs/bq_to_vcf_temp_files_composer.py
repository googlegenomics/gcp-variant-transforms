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

"""Composes multiple files in GCS to one VCF file."""

import multiprocessing
from typing import Iterable, List  # pylint: disable=unused-import

from apache_beam.io import filesystems
from apache_beam.io.gcp import gcsio

from google.cloud import storage

_MAX_NUM_OF_BLOBS_PER_COMPOSE = 32


def compose_temp_files(project, output_file, bq_to_vcf_temp_folder):
  # type: (str, str, str) -> None
  """Composes intermediate files to one VCF file.

  It composes VCF data shards to one VCF data file and deletes the intermediate
  VCF shards.
  TODO(allieychen): Eventually, it further consolidates the meta information,
  data header line, and the composed VCF data file into the `output_file`.

  Args:
    project: The project name.
    output_file: The final VCF file path.
    bq_to_vcf_temp_folder: The folder that contains all BigQuery to VCF pipeline
     temp files.
  """
  bucket_name, blob_prefix = gcsio.parse_gcs_path(bq_to_vcf_temp_folder)
  client = storage.Client(project)
  bucket = client.get_bucket(bucket_name)
  multi_process_composer = MultiProcessComposer(project, bucket_name,
                                                blob_prefix)
  composed_vcf_data_file = multi_process_composer.run()
  output_file_blob = _create_blob(client, output_file)
  output_file_blob.rewrite(composed_vcf_data_file)
  bucket.delete_blobs(bucket.list_blobs(prefix=blob_prefix))


def _compose_files(project, bucket_name, blob_names, composite_name):
  # type: (str, str, List[str], str) -> None
  """Composes multiple files (up to 32 objects) in GCS to one.

  Args:
    project: The project name.
    bucket_name: The name of the bucket where the `components` and the new
      composite are saved.
    blob_names: A list of blob object names.
    composite_name: Name of the new composite.
  """
  bucket = storage.Client(project).get_bucket(bucket_name)
  output_file_blob = bucket.blob(composite_name)
  output_file_blob.content_type = 'text/plain'
  blobs = [bucket.get_blob(blob_name) for blob_name in blob_names]
  output_file_blob.compose(blobs)


def _create_blob(client, file_path):
  # type: (storage.Client, str) -> storage.Blob
  bucket_name, blob_name = gcsio.parse_gcs_path(file_path)
  file_blob = client.get_bucket(bucket_name).blob(blob_name)
  file_blob.content_type = 'text/plain'
  return file_blob


class MultiProcessComposer(object):
  """Class to compose (a large number of) files in GCS in parallel."""

  _NUM_PROCS = 63

  def __init__(self, project, bucket_name, blob_prefix):
    # type: (str, str, str) -> None
    """Initialize a `MultiProcessComposer`.

    This class composes all blobs that start with `blob_prefix` to one.

    Args:
      project: The project name.
      bucket_name: The name of the bucket where the blob components and the new
        composite are saved.
      blob_prefix: The prefix used to filter blobs. Only the VCF files with this
        prefix will be composed.
    """
    self._project = project
    self._bucket_name = bucket_name
    self._blob_prefix = blob_prefix
    self._bucket = storage.Client(project).get_bucket(bucket_name)

  def run(self):
    # type: () -> storage.Blob
    """Returns the final blob that all blobs composed to."""
    return self._compose_vcf_shards_to_one(self._blob_prefix)

  def _compose_vcf_shards_to_one(self, blob_prefix):
    # type: (str) -> storage.Blob
    """Composes multiple VCF shards in GCS to one.

    Note that Cloud Storage allows to compose up to 32 objects. This method
    composes the VCF files recursively until there is only one file.

    Args:
      blob_prefix: the prefix used to filter blobs. Only the VCF files with this
        prefix will be composed.

    Returns:
      The final blob that all VCFs composed to.
    """
    blobs_to_be_composed = list(self._bucket.list_blobs(prefix=blob_prefix))
    if len(blobs_to_be_composed) == 1:
      return blobs_to_be_composed[0]
    new_blob_prefix = filesystems.FileSystems.join(blob_prefix, 'composed_')

    proc_pool = multiprocessing.Pool(processes=self._NUM_PROCS)
    for blob_names in self._break_list_in_chunks(blobs_to_be_composed,
                                                 _MAX_NUM_OF_BLOBS_PER_COMPOSE):
      _, file_name = filesystems.FileSystems.split(blob_names[0])
      new_blob_name = ''.join([new_blob_prefix + file_name])
      proc_pool.apply_async(func=_compose_files, args=(self._project,
                                                       self._bucket_name,
                                                       blob_names,
                                                       new_blob_name))
    proc_pool.close()
    proc_pool.join()
    return self._compose_vcf_shards_to_one(new_blob_prefix)

  def _break_list_in_chunks(self, blob_list, chunk_size):
    # type: (List, int) -> Iterable[List[str]]
    """Breaks blob_list into n-size chunks."""
    for i in range(0, len(blob_list), chunk_size):
      yield [blob.name for blob in blob_list[i:i + chunk_size]]
