# Copyright 2019 Google Inc.  All Rights Reserved.
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

"""A PTransform to output a PCollection of ``Variant`` records to BigQuery."""

from __future__ import absolute_import

import apache_beam as beam

from apache_beam.io import filesystems
from apache_beam.typehints import Any, List, Iterable
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types
from gcp_variant_transforms.beam_io import vcf_estimate_io


@beam.typehints.with_input_types(vcf_estimate_io.VcfEstimate)
class ExtractLineCount(beam.DoFn):
  def process(self, estimate):
    yield estimate.estimated_line_count


@beam.typehints.with_input_types(vcf_estimate_io.VcfEstimate)
class ExtractFileSize(beam.DoFn):
  def process(self, estimate):
    yield estimate.size_in_bytes


@with_input_types(List[int])
@with_output_types(int)
class SumEstimations(beam.CombineFn):
  """CombineFn for computing PCollection size."""

  def create_accumulator(self):
    return 0

  def add_input(self, accumulator, element):
    return accumulator + element

  def add_inputs(self, accumulator, elements):
    return sum(elements) + accumulator

  def merge_accumulators(self, accumulators):
    return sum(accumulators)

  def extract_output(self, accumulator):
    return accumulator


@with_input_types(Iterable[Any])
@with_output_types(int)
class SumRecordCounts(beam.CombineFn):
  """CombineFn for computing PCollection size."""

  def create_accumulator(self):
    return 0

  def add_input(self, accumulator, element):
    return accumulator + sum(element)

  def add_inputs(self, accumulator, elements):
    return accumulator + sum(map(sum, elements))

  def merge_accumulators(self, accumulators):
    return sum(accumulators)

  def extract_output(self, accumulator):
    return accumulator


class GetFilesSize(beam.PTransform):
  def expand(self, estimates):
    return (estimates
            | 'ExtractFileSize' >> beam.ParDo(ExtractFileSize())
            | 'SumFileSizes' >> beam.CombineGlobally(SumEstimations()))


class GetEstimatedLineCount(beam.PTransform):
  def expand(self, estimates):
    return (estimates
            | 'ExtractLineCount' >> beam.ParDo(ExtractLineCount())
            | 'SumLineCounts' >> beam.CombineGlobally(SumEstimations()))


class GetSampleMap(beam.PTransform):
  def _get_call_names(self, estimate):
    # type: (vcf_parser.Variant) -> Tuple[str]
    """Returns the names of all calls for the variant."""
    return tuple(
        zip(estimate.samples,
            [estimate.estimated_line_count] * len(estimate.samples)))

  def expand(self, estimates):
    return (estimates
            | 'MapSamplesToRecordCount' >> beam.FlatMap(self._get_call_names)
            | 'GroupAllSamples' >> beam.GroupByKey())


class GetEstimatedRecordCount(beam.PTransform):
  def expand(self, sample_map):
    return (sample_map
            | 'GetListsOfRecordCounts' >> beam.Values()
            | 'SumRecordCounts' >> beam.CombineGlobally(SumRecordCounts()))

class GetEstimatedSampleCount(beam.PTransform):
  def expand(self, sample_map):
    return (sample_map
            | 'GetListOfSamles' >> beam.Keys()
            | 'CountAllUniqueSamples' >> beam.combiners.Count.Globally())


def print_estimates_to_file(line_count,
                            sample_count,
                            record_count,
                            files_size,
                            file_count,
                            file_path):
  with filesystems.FileSystems.create(file_path) as file_to_write:
    file_to_write.write('{}\n{}\n{}\n{}\n{}\n'.format(int(line_count),
                                                      sample_count,
                                                      int(record_count),
                                                      files_size,
                                                      file_count))
