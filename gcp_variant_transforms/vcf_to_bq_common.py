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

"""Common functions that are used by both vcf_to_bq and vcf_to_bq_preprocessor.

It includes parsing the command line arguments, reading the input, applying the
PTransforms and writing the output.
"""

from typing import List  # pylint: disable=unused-import
import argparse
import datetime
import enum

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import filesystems

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.transforms import filter_variants
from gcp_variant_transforms.transforms import infer_undefined_headers
from gcp_variant_transforms.transforms import merge_headers

# If the # of files matching the input file_pattern exceeds this value, then
# headers will be merged in beam.
_SMALL_DATA_THRESHOLD = 100
_LARGE_DATA_THRESHOLD = 50000


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
  return known_args, pipeline_args


def get_pipeline_mode(known_args):
  # type: (argparse.Namespace) -> int
  """Returns the mode the pipeline should operate in based on input size."""
  if known_args.optimize_for_large_inputs:
    return PipelineModes.LARGE

  match_results = filesystems.FileSystems.match([known_args.input_pattern])
  if not match_results:
    raise ValueError('No files matched input_pattern: {}'.format(
        known_args.input_pattern))

  total_files = len(match_results[0].metadata_list)
  if total_files > _LARGE_DATA_THRESHOLD:
    return PipelineModes.LARGE
  elif total_files > _SMALL_DATA_THRESHOLD:
    return PipelineModes.MEDIUM
  return PipelineModes.SMALL


def form_absolute_file_name(directory, job_name, file_name):
  # type: (str, str, str) -> str
  """Returns the absolute file name."""
  # Adds a time prefix to ensure files are unique in case multiple pipelines are
  # run at the same time.
  file_name = '-'.join([datetime.datetime.now().strftime('%Y%m%d-%H%M%S'),
                        job_name,
                        file_name])
  return filesystems.FileSystems.join(directory, file_name)


def read_variants(pipeline, known_args):
  # type: (beam.Pipeline, argparse.Namespace) -> pvalue.PCollection
  """Helper method for returning a PCollection of Variants from VCFs."""
  if known_args.optimize_for_large_inputs:
    variants = (pipeline
                | 'InputFilePattern' >> beam.Create([known_args.input_pattern])
                | 'ReadAllFromVcf' >> vcfio.ReadAllFromVcf(
                    allow_malformed_records=(
                        known_args.allow_malformed_records)))
  else:
    variants = pipeline | 'ReadFromVcf' >> vcfio.ReadFromVcf(
        known_args.input_pattern,
        allow_malformed_records=known_args.allow_malformed_records)
  return variants


def get_inferred_headers(pipeline,  # type: beam.Pipeline
                         known_args,  # type: argparse.Namespace
                         merged_header  # type: pvalue.PCollection
                        ):
  # type: (...) -> pvalue.PCollection
  """Infers the missing headers."""
  return (read_variants(pipeline, known_args)
          | 'FilterVariants' >> filter_variants.FilterVariants(
              reference_names=known_args.reference_names)
          | ' InferUndefinedHeaderFields' >>
          infer_undefined_headers.InferUndefinedHeaderFields(
              pvalue.AsSingleton(merged_header)))


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
