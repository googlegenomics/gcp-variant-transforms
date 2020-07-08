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

import uuid
from datetime import datetime
from typing import List  # pylint: disable=unused-import

import apache_beam as beam
from apache_beam.io import filesystems

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs.annotation.vep import vep_runner_util


class _WriteVariantsToVCFShards(beam.DoFn):
  """Writes variants to VCF shards."""

  def __init__(self, vcf_shards_output_dir, number_of_variants_per_shard):
    # type: (str, int) -> None
    """Initializes `_WriteVariantsToVCFShards` object.

    Args:
      vcf_shards_output_dir: The location for all VCF shards.
      number_of_variants_per_shard: The maximum number of variants that is
        written into one VCF shard.
    """
    self._vcf_shards_output_dir = vcf_shards_output_dir
    self._number_of_variants_per_shard = number_of_variants_per_shard
    # Write shards as is.
    # When writing variants into shards, assume variant start positions are
    # 0-based - even if `--use_1_based_coordinate` is passed, variants read
    # for sharding are converted into 0-based format.
    self._coder = vcfio._ToVcfRecordCoder(bq_uses_1_based_coordinate=False)
    self._sample_names = []
    self._variant_lines = []
    self._counter = 0

  def start_bundle(self):
    self._variant_lines[:] = []
    self._counter = 0

  def finish_bundle(self):
    if self._variant_lines:
      self._write_variant_lines_to_vcf_shard(self._variant_lines)

  def process(self, variant, sample_names):
    # type: (vcfio.Variant, List[str]) -> None
    self._counter += 1
    self._sample_names = sample_names
    self._variant_lines.append(self._coder.encode(variant).strip('\n'))
    if self._counter == self._number_of_variants_per_shard:
      self._write_variant_lines_to_vcf_shard(self._variant_lines)
      self._counter = 0
      self._variant_lines[:] = []

  def _write_variant_lines_to_vcf_shard(self, variant_lines):
    # type: (List[str]) -> None
    """Writes variant lines to one VCF shard."""
    vcf_fixed_columns = ['#CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER',
                         'INFO', 'FORMAT']
    str_sample_names = [str(sample_name) for sample_name in self._sample_names]
    vcf_header = str('\t'.join(vcf_fixed_columns + str_sample_names))
    vcf_data_file = self._generate_unique_file_path(len(variant_lines))
    with filesystems.FileSystems.create(vcf_data_file) as file_to_write:
      file_to_write.write(vcf_header)
      for v in variant_lines:
        file_to_write.write('\n')
        file_to_write.write(v)

  def _generate_unique_file_path(self, variants_num):
    # type: (int) -> str
    """Generates a unique file path."""
    unique_dir = filesystems.FileSystems.join(
        self._vcf_shards_output_dir, self._generate_unique_key())
    if filesystems.FileSystems.exists(unique_dir):
      raise RuntimeError('The shard dir {} already exists.'.format(unique_dir))
    filesystems.FileSystems.mkdirs(unique_dir)
    return filesystems.FileSystems.join(
        unique_dir, vep_runner_util._SHARD_PREFIX + str(variants_num))

  def _generate_unique_key(self):
    # type: () -> str
    return '-'.join([str(uuid.uuid4()),
                     datetime.now().strftime('%Y%m%d-%H%M%S')])


class WriteToShards(beam.PTransform):
  """A PTransform for writing variants to shards."""

  def __init__(self,
               vcf_shards_output_dir,
               sample_names,
               number_of_variants_per_shard=20000):
    # type (str, List[str], int) -> None
    """Initializes `WriteToShards` object.

     Args:
       vcf_shards_output_dir: The location for all VCF shards.
       sample_names: Names of all samples.
       number_of_variants_per_shard: The maximum number of variants that is
         written into one VCF shard.
    """
    self._vcf_shards_output_dir = vcf_shards_output_dir
    self._sample_names = sample_names
    self._number_of_variants_per_shard = number_of_variants_per_shard

  def expand(self, pcoll):
    _ = (pcoll
         | 'GenerateKeys' >> beam.ParDo(
             _WriteVariantsToVCFShards(self._vcf_shards_output_dir,
                                       self._number_of_variants_per_shard),
             self._sample_names))
