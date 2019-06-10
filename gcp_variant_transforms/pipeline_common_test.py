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

"""Tests for pipeline_common script."""

import collections
import unittest

from apache_beam.io import filesystem
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that

import mock

from gcp_variant_transforms import pipeline_common
from gcp_variant_transforms.pipeline_common import PipelineModes
from gcp_variant_transforms.testing import asserts
from gcp_variant_transforms.testing import temp_dir
from gcp_variant_transforms.testing import testdata_util


class PipelineCommonWithPatternTest(unittest.TestCase):
  """Tests cases for the `pipeline_common` script with pattern input."""

  def _create_mock_args(self, **args):
    return collections.namedtuple('MockArgs', args.keys())(*args.values())

  def _get_pipeline_mode(self, args):
    all_patterns = pipeline_common._get_all_patterns(args.input_pattern,
                                                     args.input_file)
    return pipeline_common.get_pipeline_mode(all_patterns,
                                             args.optimize_for_large_inputs)

  def test_validation_failure_for_invalid_input_pattern(self):
    with self.assertRaisesRegexp(
        ValueError, 'Input pattern .* did not match any files.'):
      pipeline_common._get_all_patterns(
          input_pattern='nonexistent_file.vcf', input_file=None)

  def test_get_mode_optimize_set(self):
    args = self._create_mock_args(
        input_pattern='**', input_file=None, optimize_for_large_inputs=True)
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])
    match = match_result([None for _ in range(100)])
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(self._get_pipeline_mode(args), PipelineModes.LARGE)

  def test_get_mode_small(self):
    args = self._create_mock_args(
        input_pattern='*', input_file=None, optimize_for_large_inputs=False)
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])
    match = match_result([None for _ in range(100)])

    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(self._get_pipeline_mode(args), PipelineModes.SMALL)

  def test_get_mode_medium(self):
    args = self._create_mock_args(
        input_pattern='*', input_file=None, optimize_for_large_inputs=False)
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])

    match = match_result(range(101))
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(self._get_pipeline_mode(args), PipelineModes.MEDIUM)

    match = match_result(range(50000))
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(self._get_pipeline_mode(args), PipelineModes.MEDIUM)

  def test_get_mode_large(self):
    args = self._create_mock_args(
        input_pattern='test', input_file=None, optimize_for_large_inputs=False)
    match_result = collections.namedtuple('MatchResult', ['metadata_list'])

    match = match_result(range(50001))
    with mock.patch.object(FileSystems, 'match', return_value=[match]):
      self.assertEqual(self._get_pipeline_mode(args), PipelineModes.LARGE)

  def test_fail_on_invalid_flags(self):
    # Start with valid flags, without setup.py.
    pipeline_args = ['--project',
                     'gcp-variant-transforms-test',
                     '--staging_location',
                     'gs://integration_test_runs/staging']
    pipeline_common._raise_error_on_invalid_flags(pipeline_args)

    # Add Dataflow runner (requires --setup_file).
    pipeline_args.extend(['--runner', 'DataflowRunner'])
    with self.assertRaisesRegexp(ValueError, 'setup_file'):
      pipeline_common._raise_error_on_invalid_flags(pipeline_args)

    # Add setup.py (required for Variant Transforms run). This is now valid.
    pipeline_args.extend(['--setup_file', 'setup.py'])
    pipeline_common._raise_error_on_invalid_flags(pipeline_args)

    # Add an unknown flag.
    pipeline_args.extend(['--unknown_flag', 'somevalue'])
    with self.assertRaisesRegexp(ValueError, 'Unrecognized.*unknown_flag'):
      pipeline_common._raise_error_on_invalid_flags(pipeline_args)

  def test_get_compression_type(self):
    vcf_metadata_list = [filesystem.FileMetadata(path, size) for
                         (path, size) in [('gs://1.vcf', 100), ('2.vcf', 100)]]
    with mock.patch.object(
        FileSystems, 'match',
        return_value=[filesystem.MatchResult('vcf', vcf_metadata_list)]):
      self.assertEqual(pipeline_common.get_compression_type(['vcf']),
                       filesystem.CompressionTypes.AUTO)

    gzip_metadata_list = [
        filesystem.FileMetadata(path, size) for
        (path, size) in [('gs://1.vcf.gz', 100), ('2.vcf.gz', 100)]]
    with mock.patch.object(
        FileSystems, 'match',
        return_value=[filesystem.MatchResult('gzip', gzip_metadata_list)]):
      self.assertEqual(pipeline_common.get_compression_type('gzip'),
                       filesystem.CompressionTypes.GZIP)

    mixed_metadata_list = [
        filesystem.FileMetadata(path, size) for
        (path, size) in [('gs://1.vcf.gz', 100), ('2.vcf', 100)]]
    with mock.patch.object(
        FileSystems, 'match',
        return_value=[filesystem.MatchResult('mixed', mixed_metadata_list)]):
      with self.assertRaises(ValueError):
        pipeline_common.get_compression_type('mixed')

  def test_get_splittable_bgzf(self):
    non_gs_metadata_list = [filesystem.FileMetadata(path, size) for
                            (path, size) in [('1.vcf', 100),
                                             ('2.vcf', 100)]]
    with mock.patch.object(
        FileSystems, 'match',
        return_value=[filesystem.MatchResult('non_gs', non_gs_metadata_list)]):
      self.assertEqual(pipeline_common.get_splittable_bgzf(['non_gs']),
                       [])

    gs_metadata_list = [filesystem.FileMetadata(path, size) for
                        (path, size) in [('gs://1.vcf.bgz', 100),
                                         ('gs://2.vcf.bgz', 100)]]
    with mock.patch.object(
        FileSystems, 'match',
        return_value=[filesystem.MatchResult('gs', gs_metadata_list)]):
      with mock.patch.object(FileSystems, 'exists', return_value=True):
        self.assertEqual(
            pipeline_common.get_splittable_bgzf(['index file exists']),
            ['gs://1.vcf.bgz', 'gs://2.vcf.bgz'])

      with mock.patch.object(FileSystems, 'exists', return_value=False):
        self.assertEqual(
            pipeline_common.get_splittable_bgzf(['no index file']),
            [])


class PipelineCommonWithFileTest(unittest.TestCase):
  """Tests cases for the `pipeline_common` script with file input."""

  SAMPLE_LINES = ['./gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf\n',
                  './gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf\n',
                  './gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf\n']


  def _create_mock_args(self, **args):
    return collections.namedtuple('MockArgs', args.keys())(*args.values())

  def _get_pipeline_mode(self, args):
    all_patterns = pipeline_common._get_all_patterns(args.input_pattern,
                                                     args.input_file)
    return pipeline_common.get_pipeline_mode(all_patterns,
                                             args.optimize_for_large_inputs)

  def test_get_mode_optimize_set(self):
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(lines=self.SAMPLE_LINES)
      args = self._create_mock_args(
          input_pattern=None,
          input_file=filename,
          optimize_for_large_inputs=True)

      self.assertEqual(self._get_pipeline_mode(args), PipelineModes.LARGE)

  def test_get_mode_small_still_large(self):
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(lines=self.SAMPLE_LINES)
      args = self._create_mock_args(
          input_pattern=None,
          input_file=filename,
          optimize_for_large_inputs=False)
      match_result = collections.namedtuple('MatchResult', ['metadata_list'])

      match = match_result([None for _ in range(100)])
      with mock.patch.object(FileSystems, 'match', return_value=[match]):
        self.assertEqual(self._get_pipeline_mode(args), PipelineModes.LARGE)

  def test_get_mode_large(self):
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(lines=self.SAMPLE_LINES)
      args = self._create_mock_args(
          input_pattern=None,
          input_file=filename,
          optimize_for_large_inputs=False)
      match_result = collections.namedtuple('MatchResult', ['metadata_list'])

      match = match_result(range(50001))
      with mock.patch.object(FileSystems, 'match', return_value=[match]):
        self.assertEqual(self._get_pipeline_mode(args), PipelineModes.LARGE)

      matches = [match_result(range(25000)),
                 match_result(range(25000)),
                 match_result(range(1))]
      with mock.patch.object(FileSystems, 'match', return_value=matches):
        self.assertEqual(self._get_pipeline_mode(args), PipelineModes.LARGE)

  def test_validation_failure_for_invalid_input_file(self):
    with self.assertRaisesRegexp(ValueError, 'Input file .* doesn\'t exist'):
      pipeline_common._get_all_patterns(
          input_pattern=None, input_file='nonexistent_file.vcf')

  def test_validation_failure_for_empty_input_file(self):
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(lines=[])
      with self.assertRaisesRegexp(ValueError, 'Input file .* is empty.'):
        pipeline_common._get_all_patterns(
            input_pattern=None, input_file=filename)

  def test_validation_failure_for_wrong_pattern_in_input_file(self):
    lines = ['./gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf\n',
             'non_existent.vcf\n',
             './gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf\n']
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(lines=lines)
      with self.assertRaisesRegexp(
          ValueError, 'Input pattern .* from .* did not match any files.'):
        pipeline_common._get_all_patterns(
            input_pattern=None, input_file=filename)


class CommonPipelineTest(unittest.TestCase):

  def test_read_variants(self):
    pipeline = test_pipeline.TestPipeline()
    all_patterns = [testdata_util.get_full_file_path('valid-4.0.vcf')]
    variants = pipeline_common.read_variants(pipeline,
                                             all_patterns,
                                             PipelineModes.SMALL,
                                             False)
    assert_that(variants, asserts.count_equals_to(5))
    pipeline.run()

  def test_read_variants_large_mode(self):
    pipeline = test_pipeline.TestPipeline()
    all_patterns = [testdata_util.get_full_file_path('valid-4.0.vcf')]
    variants = pipeline_common.read_variants(pipeline,
                                             all_patterns,
                                             PipelineModes.LARGE,
                                             False)
    assert_that(variants, asserts.count_equals_to(5))
    pipeline.run()
