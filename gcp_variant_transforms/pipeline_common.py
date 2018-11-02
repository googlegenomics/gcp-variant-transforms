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

"""Common functions used by vcf_to_bq, bq_to_vcf and vcf_to_bq_preprocessor.

It includes parsing the command line arguments, reading the input, applying the
PTransforms and writing the output.
"""

from typing import List  # pylint: disable=unused-import
import argparse
import enum
import uuid
from datetime import datetime

import apache_beam as beam
from apache_beam import pvalue  # pylint: disable=unused-import
from apache_beam.io import filesystems
from apache_beam.options import pipeline_options
from apache_beam.runners.direct import direct_runner

from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.transforms import merge_headers

# If the # of files matching the input file_pattern exceeds this value, then
# headers will be merged in beam.
_SMALL_DATA_THRESHOLD = 100
_LARGE_DATA_THRESHOLD = 50000

_DATAFLOW_RUNNER_ARG_VALUE = 'DataflowRunner'


class PipelineModes(enum.Enum):
  """An Enum specifying the mode of the pipeline based on the data size."""
  SMALL = 0
  MEDIUM = 1
  LARGE = 2


def parse_args(argv, command_line_options):
  # type: (List[str], List[type]) -> (argparse.Namespace, List[str])
  """Parses the arguments.

  Args:
    argv: A list of string representing the pipeline arguments.
    command_line_options: A list of type ``VariantTransformsOptions`` that
      specifies the options that will be added to parser.
  """
  parser = argparse.ArgumentParser()
  parser.register('type', 'bool', lambda v: v.lower() == 'true')
  options = [option() for option in command_line_options]
  for transform_options in options:
    transform_options.add_arguments(parser)
  known_args, pipeline_args = parser.parse_known_args(argv)
  for transform_options in options:
    transform_options.validate(known_args)
  _raise_error_on_invalid_flags(pipeline_args)
  return known_args, pipeline_args


def get_pipeline_mode(input_pattern, optimize_for_large_inputs=False):
  # type: (str, bool) -> int
  """Returns the mode the pipeline should operate in based on input size."""
  if optimize_for_large_inputs:
    return PipelineModes.LARGE

  match_results = filesystems.FileSystems.match([input_pattern])
  if not match_results:
    raise ValueError('No files matched input_pattern: {}'.format(input_pattern))

  total_files = len(match_results[0].metadata_list)
  if total_files > _LARGE_DATA_THRESHOLD:
    return PipelineModes.LARGE
  elif total_files > _SMALL_DATA_THRESHOLD:
    return PipelineModes.MEDIUM
  return PipelineModes.SMALL


def read_headers(pipeline, pipeline_mode, known_args):
  # type: (beam.Pipeline, int, argparse.Namespace) -> pvalue.PCollection
  """Creates an initial PCollection by reading the VCF file headers."""
  if pipeline_mode == PipelineModes.LARGE:
    headers = (pipeline
               | beam.Create([known_args.input_pattern])
               | vcf_header_io.ReadAllVcfHeaders())
  else:
    headers = pipeline | vcf_header_io.ReadVcfHeaders(known_args.input_pattern)
  return headers


def get_merged_headers(headers,
                       split_alternate_allele_info_fields=True,
                       allow_incompatible_records=True):
  # type: (pvalue.PCollection, bool, bool) -> pvalue.PCollection
  """Applies the ``MergeHeaders`` PTransform on PCollection of ``VcfHeader``.

  Args:
    headers: The VCF headers.
    split_alternate_allele_info_fields: If true, the INFO fields with `Number=A`
      in BigQuery schema is not repeated. This is relevant as it changes the
      header compatibility rules.
    allow_incompatible_records: If true, always resolve the conflicts when
      merging headers.
  """
  return (headers | 'MergeHeaders' >> merge_headers.MergeHeaders(
      split_alternate_allele_info_fields, allow_incompatible_records))


def write_headers(merged_header, file_path):
  # type: (pvalue.PCollection, str) -> None
  """Writes a PCollection of ``VcfHeader`` to location ``file_path``."""
  _ = (merged_header | 'WriteHeaders' >>
       vcf_header_io.WriteVcfHeaders(file_path))


def _raise_error_on_invalid_flags(pipeline_args):
  # type: (List[str]) -> None
  """Raises an error if there are unrecognized flags."""
  parser = argparse.ArgumentParser()
  for cls in pipeline_options.PipelineOptions.__subclasses__():
    if '_add_argparse_args' in cls.__dict__:
      cls._add_argparse_args(parser)
  known_pipeline_args, unknown = parser.parse_known_args(pipeline_args)
  if unknown:
    raise ValueError('Unrecognized flag(s): {}'.format(unknown))
  if (known_pipeline_args.runner == _DATAFLOW_RUNNER_ARG_VALUE and
      not known_pipeline_args.setup_file):
    raise ValueError('The --setup_file flag is required for DataflowRunner. '
                     'Please provide a path to the setup.py file.')


def is_pipeline_direct_runner(pipeline):
  # type: (beam.Pipeline) -> bool
  """Returns True if the pipeline's runner is DirectRunner."""
  return isinstance(pipeline.runner, direct_runner.DirectRunner)


def generate_unique_name(job_name):
  # type: (str) -> str
  """Returns a unique name with time suffix and random UUID."""
  return '-'.join([job_name,
                   datetime.now().strftime('%Y%m%d-%H%M%S'),
                   str(uuid.uuid4())])
