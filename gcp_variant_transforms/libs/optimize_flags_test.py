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

"""Tests for optimize_flags module."""

import unittest
import argparse

from apache_beam.options import pipeline_options

from gcp_variant_transforms.libs import optimize_flags
from gcp_variant_transforms.options import variant_transform_options

TOOL_OPTIONS = [
    variant_transform_options.VcfReadOptions,
    variant_transform_options.AvroWriteOptions,
    variant_transform_options.BigQueryWriteOptions,
    variant_transform_options.AnnotationOptions,
    variant_transform_options.FilterOptions,
    variant_transform_options.MergeOptions,
    variant_transform_options.PartitionOptions,
    variant_transform_options.ExperimentalOptions]

PIPELINE_OPTIONS = [
    pipeline_options.WorkerOptions
]

def add_defaults(known_args):
  known_args.run_annotation_pipeline = False

def make_known_args_with_default_values(options):
  parser = argparse.ArgumentParser()
  parser.register('type', 'bool', lambda v: v.lower() == 'true')
  _ = [option().add_arguments(parser) for option in options]
  known_args, unknown_known_args = parser.parse_known_args([])

  parser = argparse.ArgumentParser()
  for cls in pipeline_options.PipelineOptions.__subclasses__():
    if '_add_argparse_args' in cls.__dict__:
      cls._add_argparse_args(parser)
  pipeline_args, unknown_pipeline_args = parser.parse_known_args([])
  assert not unknown_known_args
  assert not unknown_pipeline_args
  return known_args, pipeline_args

class OptimizeFlagsTest(unittest.TestCase):

  known_args = pipeline_args = supplied_args = None

  def setUp(self):
    self.known_args, self.pipeline_args = (
        make_known_args_with_default_values(TOOL_OPTIONS))
    self.supplied_args = []

  def _set_up_dimensions(
      self,
      line_count,
      sample_count,
      record_count,
      files_size,
      file_count):
    self.known_args.estimated_line_count = line_count
    self.known_args.estimated_sample_count = sample_count
    self.known_args.estimated_record_count = record_count
    self.known_args.files_size = files_size
    self.known_args.file_count = file_count

  def _run_tests(self):
    optimize_flags.optimize_flags(
        self.supplied_args, self.known_args, self.pipeline_args)

  def test_optimize_for_large_inputs_passes_records(self):
    self._set_up_dimensions(1, 1, 3000000001, 1, 1)
    self.known_args.optimize_for_large_inputs = False

    self._run_tests()

    self.assertEqual(self.known_args.optimize_for_large_inputs, True)

  def test_optimize_for_large_inputs_passes_files(self):
    self._set_up_dimensions(1, 1, 3000000000, 1, 50001)
    self.known_args.optimize_for_large_inputs = False

    self._run_tests()

    self.assertEqual(self.known_args.optimize_for_large_inputs, True)

  def test_optimize_for_large_inputs_fails(self):
    self._set_up_dimensions(1, 1, 3000000000, 1, 50000)
    self.known_args.optimize_for_large_inputs = False

    self._run_tests()

    self.assertEqual(self.known_args.optimize_for_large_inputs, False)

  def test_optimize_for_large_inputs_supplied(self):
    self._set_up_dimensions(1, 1, 3000000001, 1, 50001)
    self.supplied_args = ['optimize_for_large_inputs']
    self.known_args.optimize_for_large_inputs = False

    self._run_tests()

    self.assertEqual(self.known_args.optimize_for_large_inputs, False)

  def test_infer_headers_passes(self):
    self._set_up_dimensions(1, 1, 5000000000, 1, 1)
    self.known_args.infer_headers = False

    self._run_tests()

    self.assertEqual(self.known_args.infer_headers, True)

  def test_infer_headers_fails(self):
    self._set_up_dimensions(1, 1, 5000000001, 1, 1)
    self.known_args.infer_headers = False

    self._run_tests()

    self.assertEqual(self.known_args.infer_headers, False)

  def test_infer_headers_supplied(self):
    self._set_up_dimensions(1, 1, 5000000000, 1, 1)
    self.supplied_args = ['infer_headers']
    self.known_args.infer_headers = False

    self._run_tests()

    self.assertEqual(self.known_args.infer_headers, False)

  def test_num_bigquery_write_shards_passes_records(self):
    self._set_up_dimensions(1, 1, 1000000001, 500000000000, 1)
    self.known_args.num_bigquery_write_shards = 1

    self._run_tests()

    self.assertEqual(self.known_args.num_bigquery_write_shards, 20)

  def test_num_bigquery_write_shards_passes_size(self):
    self._set_up_dimensions(1, 1, 1000000000, 500000000001, 1)
    self.known_args.num_bigquery_write_shards = 1

    self._run_tests()

    self.assertEqual(self.known_args.num_bigquery_write_shards, 20)

  def test_num_bigquery_write_shards_fails(self):
    self._set_up_dimensions(1, 1, 1000000000, 500000000000, 1)
    self.known_args.num_bigquery_write_shards = 1

    self._run_tests()

    self.assertEqual(self.known_args.num_bigquery_write_shards, 1)

  def test_num_bigquery_write_shards_supplied(self):
    self._set_up_dimensions(1, 1, 1000000001, 500000000000, 1)
    self.supplied_args = ['num_bigquery_write_shards']
    self.known_args.num_bigquery_write_shards = 1

    self._run_tests()

    self.assertEqual(self.known_args.num_bigquery_write_shards, 1)

  def test_num_workers_passes_records(self):
    self._set_up_dimensions(1, 1, 1000000001, 1, 1)
    self.known_args.run_annotation_pipeline = False
    self.pipeline_args.num_workers = 1

    self._run_tests()

    self.assertEqual(self.pipeline_args.num_workers, 100)

  def test_num_workers_passes_size(self):
    self._set_up_dimensions(1, 1, 1000000001, 1, 1)
    self.known_args.run_annotation_pipeline = True
    self.pipeline_args.num_workers = 1

    self._run_tests()

    self.assertEqual(self.pipeline_args.num_workers, 400)

  def test_num_workers_fails(self):
    self._set_up_dimensions(1, 1, 1000000000, 1, 1)
    self.known_args.run_annotation_pipeline = False
    self.pipeline_args.num_workers = 1

    self._run_tests()

    self.assertEqual(self.pipeline_args.num_workers, 1)

  def test_num_workers_supplied(self):
    self._set_up_dimensions(1, 1, 1000000001, 1, 1)
    self.supplied_args = ['num_workers']
    self.known_args.run_annotation_pipeline = True
    self.pipeline_args.num_workers = 1

    self._run_tests()

    self.assertEqual(self.pipeline_args.num_workers, 1)

  def test_shard_variants_passes(self):
    self._set_up_dimensions(1, 1, 1000000000, 1, 1)
    self.known_args.shard_variants = True

    self._run_tests()

    self.assertEqual(self.known_args.shard_variants, False)

  def test_shard_variants_fails(self):
    self._set_up_dimensions(1, 1, 1000000001, 1, 1)
    self.known_args.shard_variants = True

    self._run_tests()

    self.assertEqual(self.known_args.shard_variants, True)

  def test_shard_variants_supplied(self):
    self._set_up_dimensions(1, 1, 1000000000, 1, 1)
    self.supplied_args = ['shard_variants']
    self.known_args.shard_variants = True

    self._run_tests()

    self.assertEqual(self.known_args.shard_variants, True)
