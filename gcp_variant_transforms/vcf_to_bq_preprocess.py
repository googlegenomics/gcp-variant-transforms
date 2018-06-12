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

"""Pipeline for preprocessing the VCF files.

This pipeline is aimed to help the user to easily identify and further import
the malformed/incompatible VCF files to BigQuery. It generates two files as the
output:
- Report: A file that lists the conflicting headers, undefined header fields,
  the suggested resolutions and malformed records.
- Resolved headers file: A VCF file that contains the resolved fields
  definitions.

The report is generated in the ``report_path``, while the resolved headers file
is generated in ``resolved_headers_path`` if provided.

Run locally:
python -m gcp_variant_transforms.vcf_to_bq_preprocess \
  --input_pattern <path to VCF file(s)> \
  --report_path <local path to the report file> \
  --resolved_headers_path <local path to the resolved headers file> \
  --report_all_conflicts True

Run on Dataflow:
python -m gcp_variant_transforms.vcf_to_bq_preprocess \
  --input_pattern <path to VCF file(s)>
  --report_path <cloud path to the report file> \
  --resolved_headers_path <cloud path to the resolved headers file> \
  --report_all_conflicts True \
  --project gcp-variant-transforms-test \
  --job_name preprocess \
  --staging_location "gs://integration_test_runs/staging" \
  --temp_location "gs://integration_test_runs/temp" \
  --runner DataflowRunner \
  --setup_file ./setup.py
"""

from __future__ import absolute_import

import logging
import sys

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options import pipeline_options

from gcp_variant_transforms import pipeline_common
from gcp_variant_transforms.beam_io import vcf_file_size_io
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import preprocess_reporter
from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.transforms import filter_variants
from gcp_variant_transforms.transforms import infer_headers
from gcp_variant_transforms.transforms import merge_headers
from gcp_variant_transforms.transforms import merge_header_definitions

_COMMAND_LINE_OPTIONS = [variant_transform_options.PreprocessOptions]

# Number of lines from each VCF that should be read when estimating disk usage.
_SNIPPET_READ_SIZE = 50

def _get_inferred_headers(variants,  # type: pvalue.PCollection
                          merged_header  # type: pvalue.PCollection
                         ):
  # type: (...) -> (pvalue.PCollection, pvalue.PCollection)
  inferred_headers = (variants
                      | 'FilterVariants' >> filter_variants.FilterVariants()
                      | 'InferHeaderFields' >>
                      infer_headers.InferHeaderFields(
                          pvalue.AsSingleton(merged_header),
                          allow_incompatible_records=True,
                          infer_headers=True))

  merged_header = (
      (inferred_headers, merged_header)
      | beam.Flatten()
      | 'MergeHeadersFromVcfAndVariants' >> merge_headers.MergeHeaders(
          allow_incompatible_records=True))
  return inferred_headers, merged_header


# TODO(hanjohn): Add an e2e test
def _estimate_disk_resources(p, input_pattern):
  # type: (pvalue.PCollection, str) -> (pvalue.PCollection)
  # TODO(hanjohn): Add support for `ReadAll` pattern for inputs with very large
  # numbers of files.
  result = (
      p
      | 'InputFilePattern' >> beam.Create([input_pattern])
      | 'ReadFileSizeAndSampleVariants' >> vcf_file_size_io.EstimateVcfSize(
          input_pattern, _SNIPPET_READ_SIZE)
      | 'SumFileSizeEstimates' >> beam.CombineGlobally(
          vcf_file_size_io.FileSizeInfoSumFn()))
  result | ('PrintEstimate' >>  # pylint: disable=expression-not-assigned
            beam.Map(lambda x: logging.info(
                "Final estimate of encoded size: %d GB", x.encoded_size / 1e9)))
  return result


def run(argv=None):
  # type: (List[str]) -> (str, str)
  """Runs preprocess pipeline."""
  logging.info('Command: %s', ' '.join(argv or sys.argv))
  known_args, pipeline_args = pipeline_common.parse_args(argv,
                                                         _COMMAND_LINE_OPTIONS)
  options = pipeline_options.PipelineOptions(pipeline_args)
  all_patterns = known_args.all_patterns
  pipeline_mode = pipeline_common.get_pipeline_mode(all_patterns)

  with beam.Pipeline(options=options) as p:
    headers = pipeline_common.read_headers(p, pipeline_mode, all_patterns)
    merged_headers = pipeline_common.get_merged_headers(headers)
    merged_definitions = (headers
                          | 'MergeDefinitions' >>
                          merge_header_definitions.MergeDefinitions())

    disk_usage_estimate = None
    if known_args.estimate_disk_usage:
      disk_usage_estimate = beam.pvalue.AsSingleton(
          _estimate_disk_resources(p, known_args.input_pattern))
    if known_args.report_all_conflicts:
      if len(all_patterns) == 1:
        variants = p | 'ReadFromVcf' >> vcfio.ReadFromVcf(
            all_patterns[0], allow_malformed_records=True)
      else:
        variants = (p
                    | 'InputFilePattern' >> beam.Create(all_patterns)
                    | 'ReadAllFromVcf' >> vcfio.ReadAllFromVcf(
                        allow_malformed_records=True))

      malformed_records = variants | filter_variants.ExtractMalformedVariants()
      inferred_headers, merged_headers = (_get_inferred_headers(variants,
                                                                merged_headers))
      _ = (merged_definitions
           | 'GenerateConflictsReport' >>
           beam.ParDo(preprocess_reporter.generate_report,
                      known_args.report_path,
                      disk_usage_estimate,
                      beam.pvalue.AsSingleton(merged_headers),
                      beam.pvalue.AsSingleton(inferred_headers),
                      beam.pvalue.AsIter(malformed_records)))
    else:
      _ = (merged_definitions
           | 'GenerateConflictsReport' >>
           beam.ParDo(preprocess_reporter.generate_report,
                      known_args.report_path,
                      disk_usage_estimate,
                      beam.pvalue.AsSingleton(merged_headers)))

    if known_args.resolved_headers_path:
      pipeline_common.write_headers(merged_headers,
                                    known_args.resolved_headers_path)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
