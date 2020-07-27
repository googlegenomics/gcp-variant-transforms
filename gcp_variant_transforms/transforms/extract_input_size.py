# Copyright 2019 Google LLC.
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

"""A PTransform to extract comprehensive size signals from ``VcfEstimates``.

This module is used to create 5 main signals that describe the supplied input:
  - variant count: approximate number of variants in all VCF files.
  - sample count: number of distinct samples across all VCF files.
  - value count: approximate number of variant by sample data points.
  - files size: total size of all supplied files.
  - file count: number of input files.
"""



import apache_beam as beam

from apache_beam.io import filesystems


class GetFilesSize(beam.PTransform):
  def expand(self, estimates):
    return (estimates
            | 'ExtractFileSize' >> beam.Map(
                lambda estimate: estimate.size_in_bytes)
            | 'SumFileSizes' >> beam.CombineGlobally(sum))


class GetEstimatedVariantCount(beam.PTransform):
  def expand(self, estimates):
    return (estimates
            | 'ExtractVariantCount' >> beam.Map(
                lambda estimate: estimate.estimated_variant_count)
            | 'SumVariantCounts' >> beam.CombineGlobally(sum))


class GetSampleMap(beam.PTransform):
  """ Converts estimate objects into a distinct sample->List[variant count] map.

  The keys of this map will be used to find the count of distinct samples, while
  the sum of values will give us estimated value count.
  """
  def _get_sample_ids(self, estimate):
    # type: (vcf_parser.Variant) -> Tuple[int]
    """Returns the ids of all calls for the variant."""
    return tuple(
        zip(estimate.samples,
            [estimate.estimated_variant_count] * len(estimate.samples)))

  def expand(self, estimates):
    return (estimates
            | 'MapSamplesToValueCount' >> beam.FlatMap(
                self._get_sample_ids)
            | 'GroupAllSamples' >> beam.GroupByKey())


class GetEstimatedValueCount(beam.PTransform):
  def expand(self, sample_map):
    return (sample_map
            | 'GetListsOfValueCounts' >> beam.Values()
            | 'SumValueCountsPerSample' >> beam.Map(sum)
            | 'SumTotalValueCounts' >> beam.CombineGlobally(sum))

class GetEstimatedSampleCount(beam.PTransform):
  def expand(self, sample_map):
    return (sample_map
            | 'GetListOfSamles' >> beam.Keys()
            | 'CountAllUniqueSamples' >> beam.combiners.Count.Globally())


def print_estimates_to_file(variant_count,
                            sample_count,
                            value_count,
                            files_size,
                            file_count,
                            file_path):
  with filesystems.FileSystems.create(file_path) as file_to_write:
    file_to_write.write(('{}\n{}\n{}\n{}\n{}\n'.format(
        int(variant_count),
        sample_count,
        int(value_count),
        files_size,
        file_count)).encode('utf-8'))
