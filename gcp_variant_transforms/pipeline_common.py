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
import os
import uuid
from datetime import datetime

import apache_beam as beam
from apache_beam import pvalue  # pylint: disable=unused-import
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.options import pipeline_options
from apache_beam.runners.direct import direct_runner

from gcp_variant_transforms.beam_io import bgzf_io
from gcp_variant_transforms.beam_io import vcf_estimate_io
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.beam_io import vcf_parser
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.transforms import fusion_break
from gcp_variant_transforms.transforms import merge_headers

# If the # of files matching the input file_pattern exceeds this value, then
# headers will be merged in beam.
_SMALL_DATA_THRESHOLD = 100
_LARGE_DATA_THRESHOLD = 50000

_DATAFLOW_RUNNER_ARG_VALUE = 'DataflowRunner'
SampleNameEncoding = vcf_parser.SampleNameEncoding


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
  if hasattr(known_args, 'input_pattern') or hasattr(known_args, 'input_file'):
    known_args.all_patterns = _get_all_patterns(
        known_args.input_pattern, known_args.input_file)
  return known_args, pipeline_args


def _get_all_patterns(input_pattern, input_file):
  # type: (str, str) -> List[str]
  patterns = [input_pattern] if input_pattern else _get_file_names(input_file)

  # Validate inputs.
  try:
    # Gets at most 1 pattern match result of type `filesystems.MatchResult`.
    matches = filesystems.FileSystems.match(patterns, [1] * len(patterns))
    for match in matches:
      if not match.metadata_list:
        if input_file:
          raise ValueError(
              'Input pattern {} from {} did not match any files.'.format(
                  match.pattern, input_file))
        else:
          raise ValueError(
              'Input pattern {} did not match any files.'.format(match.pattern))
  except filesystem.BeamIOError:
    if input_file:
      raise ValueError(
          'Some patterns in {} are invalid or inaccessible.'.format(
              input_file))
    else:
      raise ValueError('Invalid or inaccessible input pattern {}.'.format(
          input_pattern))
  return patterns


def get_compression_type(input_patterns):
  # type: (List[str]) -> filesystem.CompressionTypes
  """Returns the compression type.

  Raises:
    ValueError: if the input files are not in the same format.
  """
  matches = filesystems.FileSystems.match(input_patterns)
  extensions = [os.path.splitext(metadata.path)[1] for match in matches
                for metadata in match.metadata_list]
  if len(set(extensions)) != 1:
    raise ValueError('All input files must be in the same format.')
  if extensions[0].endswith('.bgz') or extensions[0].endswith('.gz'):
    return filesystem.CompressionTypes.GZIP
  else:
    return filesystem.CompressionTypes.AUTO


def _get_splittable_bgzf(all_patterns):
  # type: (List[str]) -> List[str]
  """Returns the splittable bgzf matching `all_patterns`."""
  matches = filesystems.FileSystems.match(all_patterns)
  splittable_bgzf = []
  count = 0
  for match in matches:
    for metadata in match.metadata_list:
      count += 1
      if (metadata.path.startswith('gs://') and
          bgzf_io.exists_tbi_file(metadata.path)):
        splittable_bgzf.append(metadata.path)
  if splittable_bgzf and len(splittable_bgzf) < count:
    raise ValueError("Some index files are missing for {}.".format(
        all_patterns))
  return splittable_bgzf


def _get_file_names(input_file):
  # type: (str) -> List[str]
  """Reads the input file and extracts list of patterns out of it."""
  if not filesystems.FileSystems.exists(input_file):
    raise ValueError('Input file {} doesn\'t exist'.format(input_file))
  with filesystems.FileSystems.open(input_file) as f:
    contents = map(str.strip, f.readlines())
    if not contents:
      raise ValueError('Input file {} is empty.'.format(input_file))
    return contents


def get_pipeline_mode(all_patterns, optimize_for_large_inputs=False):
  # type: (List[str], bool) -> int
  """Returns the mode the pipeline should operate in based on input size."""
  if optimize_for_large_inputs or len(all_patterns) > 1:
    return PipelineModes.LARGE

  match_results = filesystems.FileSystems.match(all_patterns)
  if not match_results:
    raise ValueError(
        'No files matched input_pattern: {}'.format(all_patterns[0]))

  total_files = len(match_results[0].metadata_list)
  if total_files > _LARGE_DATA_THRESHOLD:
    return PipelineModes.LARGE
  elif total_files > _SMALL_DATA_THRESHOLD:
    return PipelineModes.MEDIUM
  return PipelineModes.SMALL

def get_estimates(pipeline, pipeline_mode, all_patterns):
  # type: (beam.Pipeline, int, List[str]) -> pvalue.PCollection
  """Creates a PCollection by reading the VCF files and deriving estimates."""
  if pipeline_mode == PipelineModes.LARGE:
    estimates = (pipeline
                 | beam.Create(all_patterns)
                 | vcf_estimate_io.GetAllEstimates())
  else:
    estimates = pipeline | vcf_estimate_io.GetEstimates(all_patterns[0])

  return estimates


def read_headers(
    pipeline,  #type: beam.Pipeline
    pipeline_mode,  #type: int
    all_patterns  #type: List[str]
    ):
  # type: (...) -> pvalue.PCollection
  """Creates an initial PCollection by reading the VCF file headers."""
  compression_type = get_compression_type(all_patterns)
  if pipeline_mode == PipelineModes.LARGE:
    headers = (pipeline
               | beam.Create(all_patterns)
               | vcf_header_io.ReadAllVcfHeaders(
                   compression_type=compression_type))
  else:
    headers = pipeline | vcf_header_io.ReadVcfHeaders(
        all_patterns[0],
        compression_type=compression_type)

  return headers


def read_variants(
    pipeline,  # type: beam.Pipeline
    all_patterns,  # type: List[str]
    pipeline_mode,  # type: PipelineModes
    allow_malformed_records,  # type: bool
    representative_header_lines=None,  # type: List[str]
    pre_infer_headers=False,  # type: bool
    sample_name_encoding=SampleNameEncoding.WITHOUT_FILE_PATH  # type: int
    ):
  # type: (...) -> pvalue.PCollection
  """Returns a PCollection of Variants by reading VCFs."""
  compression_type = get_compression_type(all_patterns)
  if compression_type == filesystem.CompressionTypes.GZIP:
    splittable_bgzf = _get_splittable_bgzf(all_patterns)
    if splittable_bgzf:
      return (pipeline
              | 'ReadVariants'
              >> vcfio.ReadFromBGZF(splittable_bgzf,
                                    representative_header_lines,
                                    allow_malformed_records,
                                    pre_infer_headers,
                                    sample_name_encoding))

  if pipeline_mode == PipelineModes.LARGE:
    variants = (pipeline
                | 'InputFilePattern' >> beam.Create(all_patterns)
                | 'ReadAllFromVcf' >> vcfio.ReadAllFromVcf(
                    representative_header_lines=representative_header_lines,
                    compression_type=compression_type,
                    allow_malformed_records=allow_malformed_records,
                    pre_infer_headers=pre_infer_headers,
                    sample_name_encoding=sample_name_encoding))
  else:
    variants = pipeline | 'ReadFromVcf' >> vcfio.ReadFromVcf(
        all_patterns[0],
        representative_header_lines=representative_header_lines,
        compression_type=compression_type,
        allow_malformed_records=allow_malformed_records,
        pre_infer_headers=pre_infer_headers,
        sample_name_encoding=sample_name_encoding)

  if compression_type == filesystem.CompressionTypes.GZIP:
    variants |= 'FusionBreak' >> fusion_break.FusionBreak()
  return variants


def add_annotation_headers(pipeline, known_args, pipeline_mode,
                           merged_header,
                           annotated_vcf_pattern):
  if pipeline_mode == PipelineModes.LARGE:
    annotation_headers = (pipeline
                          | 'ReadAnnotatedVCF'
                          >> beam.Create([annotated_vcf_pattern])
                          | 'ReadHeaders' >> vcf_header_io.ReadAllVcfHeaders())
  else:
    annotation_headers = (
        pipeline
        | 'ReadHeaders'
        >> vcf_header_io.ReadVcfHeaders(annotated_vcf_pattern))
  merged_header = (
      (merged_header, annotation_headers)
      | beam.Flatten()
      | 'MergeWithOriginalHeaders' >> merge_headers.MergeHeaders(
          known_args.split_alternate_allele_info_fields,
          known_args.allow_incompatible_records))
  return merged_header


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



BQ_CREATE_PARTITIONED_TABLE = (
    'bq mk --table '
    '--range_partitioning=start_position,0,{TOTAL_BASE_PAIRS},{PARTITION_SIZE} '
    '--clustering_fields=start_position,end_position '
    '{FULL_TABLE_ID} {SCHEMA_JSON}')

def make_output_table_if_needed(known_args, full_table_id, total_base_pairs):
  if not known_args.append:
    (partition_size, total_base_pairs_enlarged) = (
        bigquery_util.calculate_optimize_partition_size(total_base_pairs))
    bq_command = BQ_CREATE_PARTITIONED_TABLE.format(
        TOTAL_BASE_PAIRS=total_base_pairs_enlarged,
        PARTITION_SIZE=partition_size,
        FULL_TABLE_ID=full_table_id,
        SCHEMA_JSON=known_args.schema_json)
    result = os.system(bq_command)
    if result != 0:
      raise ValueError(
          'Failed to create a bigquery table using "{}" command.'.format(
              bq_make_command))