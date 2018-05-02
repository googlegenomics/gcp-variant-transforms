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

r"""Pipeline for preprocessing the VCF files.

This pipeline is aimed to help the user to easily identify and further import
the malformed/incompatible VCF files to BigQuery. It generates two files as the
output:
- Conflicts report: A file that lists the incompatible headers, undefined header
  fields, the suggested resolutions and eventually malformed records.
- Resolved headers file: A VCF file that contains the resolved fields
  definitions.

The report is generated in the ``report_path``, while the resolved headers file
is generated in ``resolved_headers_path`` if provided.

Run locally:
python -m gcp_variant_transforms.vcf_to_bq_preprocess \
  --input_pattern <path to VCF file(s)> \
  --report_path <local path to the report file> \
  --resolved_headers_path <local path to the resolved headers file> \
  --report_all True

Run on Dataflow:
python -m gcp_variant_transforms.vcf_to_bq_preprocess \
  --input_pattern <path to VCF file(s)>
  --report_path <cloud path to the report file> \
  --resolved_headers_path <cloud path to the resolved headers file> \
  --report_all True \
  --project gcp-variant-transforms-test \
  --job_name preprocess \
  --staging_location "gs://integration_test_runs/staging" \
  --temp_location "gs://integration_test_runs/temp" \
  --runner DataflowRunner \
  --setup_file ./setup.py
"""

import logging
import sys

import apache_beam as beam
from apache_beam.options import pipeline_options

from gcp_variant_transforms import vcf_to_bq_common
from gcp_variant_transforms.libs import preprocess_reporter
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.transforms import merge_header_definitions

_COMMAND_LINE_OPTIONS = [
    variant_transform_options.FilterOptions,
    variant_transform_options.PreprocessOptions,
    variant_transform_options.VcfReadOptions
]


def _get_inferred_headers(pipeline,  # type: beam.Pipeline
                          known_args,  # type: argparse.Namespace
                          merged_header  # type: pvalue.PCollection
                         ):
  # type: (...) -> (pvalue.PCollection, pvalue.PCollection)
  inferred_headers = vcf_to_bq_common.get_inferred_headers(pipeline, known_args,
                                                           merged_header)
  merged_header = (
      (inferred_headers, merged_header)
      | beam.Flatten()
      | 'MergeHeadersFromVcfAndVariants' >> merge_headers.MergeHeaders(
          allow_incompatible_records=True))
  return inferred_headers, merged_header


def run(argv=None):
  # type: (List[str]) -> (str, str)
  """Runs preprocess pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  known_args, pipeline_args = vcf_to_bq_common.parse_args(argv,
                                                          _COMMAND_LINE_OPTIONS)
  options = pipeline_options.PipelineOptions(pipeline_args)
  pipeline_mode = vcf_to_bq_common.get_pipeline_mode(known_args)

  with beam.Pipeline(options=options) as p:
    headers = vcf_to_bq_common.read_headers(p, pipeline_mode, known_args)
    merged_headers = vcf_to_bq_common.get_merged_headers(headers)
    merged_definitions = (headers
                          | 'MergeDefinitions' >>
                          merge_header_definitions.MergeDefinitions())
    inferred_headers_side_input = None
    if known_args.report_all:
      inferred_headers, merged_headers = _get_inferred_headers(
          p, known_args, merged_headers)
      inferred_headers_side_input = beam.pvalue.AsSingleton(inferred_headers)

    _ = (merged_definitions
         | 'GenerateConflictsReport' >>
         beam.ParDo(preprocess_reporter.generate_report,
                    known_args.report_path,
                    beam.pvalue.AsSingleton(merged_headers),
                    inferred_headers_side_input))
    if known_args.resolved_headers_path:
      vcf_to_bq_common.write_headers(merged_headers,
                                     known_args.resolved_headers_path)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
