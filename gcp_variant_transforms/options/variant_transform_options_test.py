# Copyright 2017 Google Inc.  All Rights Reserved.
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

"""Tests for derivatives variant_transform_options."""

import unittest

import argparse

from typing import List  # pylint: disable=unused-import

import mock

from apache_beam.io.gcp.internal.clients import bigquery
from apitools.base.py import exceptions

from gcp_variant_transforms.options import variant_transform_options
from gcp_variant_transforms.testing import temp_dir


def make_args(options, args):
  parser = argparse.ArgumentParser()
  parser.register('type', 'bool', lambda v: v.lower() == 'true')
  options.add_arguments(parser)
  namespace, remaining_args = parser.parse_known_args(args)
  assert not remaining_args
  return namespace


class VcfReadOptionsTest(unittest.TestCase):
  """Tests cases for the VcfReadOptions class."""

  def setUp(self):
    self._options = variant_transform_options.VcfReadOptions()

  def _make_args(self, args):
    # type: (List[str]) -> argparse.Namespace
    return make_args(self._options, args)

  def test_no_inputs(self):
    args = self._make_args([])
    self.assertRaises(ValueError, self._options.validate, args)

  def test_failure_for_conflicting_flags_inputs(self):
    args = self._make_args(['--input_pattern', '*',
                            '--input_file', 'asd'])
    self.assertRaises(ValueError, self._options.validate, args)

  def test_failure_for_conflicting_flags_headers(self):
    args = self._make_args(['--input_pattern', '*',
                            '--infer_headers',
                            '--representative_header_file', 'gs://some_file'])
    self.assertRaises(ValueError, self._options.validate, args)

  def test_failure_for_conflicting_flags_no_errors_with_pattern_input(self):
    args = self._make_args(['--input_pattern', '*',
                            '--representative_header_file', 'gs://some_file'])
    self._options.validate(args)

  def test_failure_for_conflicting_flags_no_errors_with_file_input(self):
    lines = ['./gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf\n',
             './gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf\n',
             './gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf\n']
    with temp_dir.TempDir() as tempdir:
      filename = tempdir.create_temp_file(lines=lines)
      args = self._make_args([
          '--input_file',
          filename,
          '--representative_header_file', 'gs://some_file'])
      self._options.validate(args)


class BigQueryWriteOptionsTest(unittest.TestCase):
  """Tests cases for the BigQueryWriteOptions class."""

  def setUp(self):
    self._options = variant_transform_options.BigQueryWriteOptions()

  def _make_args(self, args):
    # type: (List[str]) -> argparse.Namespace
    return make_args(self._options, args)

  def test_valid_table_path(self):
    args = self._make_args(['--append',
                            '--output_table', 'project:dataset.table',
                            '--schema_version', 'v1'])
    client = mock.Mock()
    client.datasets.Get.return_value = bigquery.Dataset(
        datasetReference=bigquery.DatasetReference(
            projectId='project', datasetId='dataset'))
    self._options.validate(args, client)

  def test_existing_table(self):
    args = self._make_args(['--append', 'False',
                            '--output_table', 'project:dataset.table'])
    client = mock.Mock()
    with self.assertRaisesRegexp(ValueError, 'project:dataset.table '):
      self._options.validate(args, client)

    args = self._make_args(['--append', 'False',
                            '--output_table', 'project:dataset.table',
                            '--schema_version', 'v2'])
    client = mock.Mock()
    with self.assertRaisesRegexp(ValueError, 'project:dataset.table_call_info'):
      self._options.validate(args, client)

  def test_no_project(self):
    args = self._make_args(['--output_table', 'dataset.table'])
    client = mock.Mock()
    self.assertRaises(ValueError, self._options.validate, args, client)

  def test_invalid_table_path(self):
    no_table = self._make_args(['--output_table', 'project:dataset'])
    incorrect_sep1 = self._make_args(['--output_table',
                                      'project.dataset.table'])
    incorrect_sep2 = self._make_args(['--output_table',
                                      'project:dataset:table'])
    client = mock.Mock()
    self.assertRaises(
        ValueError, self._options.validate, no_table, client)
    self.assertRaises(
        ValueError, self._options.validate, incorrect_sep1, client)
    self.assertRaises(
        ValueError, self._options.validate, incorrect_sep2, client)

  def test_dataset_does_not_exists(self):
    args = self._make_args(['--output_table', 'project:dataset.table'])
    client = mock.Mock()
    client.datasets.Get.side_effect = exceptions.HttpError(
        response={'status': '404'}, url='', content='')
    self.assertRaises(ValueError, self._options.validate, args, client)


class AnnotationOptionsTest(unittest.TestCase):

  def setUp(self):
    self._options = variant_transform_options.AnnotationOptions()

  def _make_args(self, args):
    # type: (List[str]) -> argparse.Namespace
    return make_args(self._options, args)

  def test_validate_okay(self):
    """Tests that no exceptions are raised for valid arguments."""
    args = self._make_args(['--run_annotation_pipeline',
                            '--annotation_output_dir', 'gs://GOOD_DIR',
                            '--vep_image_uri', 'AN_IMAGE',
                            '--vep_cache_path', 'gs://VEP_CACHE'])
    self._options.validate(args)

  def test_invalid_output_dir(self):
    args = self._make_args(['--run_annotation_pipeline',
                            '--annotation_output_dir', 'BAD_DIR',
                            '--vep_image_uri', 'AN_IMAGE',
                            '--vep_cache_path', 'gs://VEP_CACHE'])
    self.assertRaises(ValueError, self._options.validate, args)

  def test_failure_for_no_image(self):
    args = self._make_args(['--run_annotation_pipeline',
                            '--annotation_output_dir', 'BAD_DIR',
                            '--vep_cache_path', 'gs://VEP_CACHE'])
    self.assertRaises(ValueError, self._options.validate, args)

  def test_failure_for_invalid_vep_cache(self):
    args = self._make_args(['--run_annotation_pipeline',
                            '--annotation_output_dir', 'gs://GOOD_DIR',
                            '--vep_image_uri', 'AN_IMAGE',
                            '--vep_cache_path', 'VEP_CACHE'])
    self.assertRaises(ValueError, self._options.validate, args)


class PreprocessOptionsTest(unittest.TestCase):
  """Tests cases for the PreprocessOptions class."""

  def setUp(self):
    self._options = variant_transform_options.PreprocessOptions()

  def _make_args(self, args):
    # type: (List[str]) -> argparse.Namespace
    return make_args(self._options, args)

  def test_failure_for_conflicting_flags_inputs(self):
    args = self._make_args(['--input_pattern', '*',
                            '--report_path', 'some_path',
                            '--input_file', 'asd'])
    self.assertRaises(ValueError, self._options.validate, args)

  def test_failure_for_conflicting_flags_no_errors(self):
    args = self._make_args(['--input_pattern', '*',
                            '--report_path', 'some_path'])
    self._options.validate(args)

  def test_failure_for_conflicting_flags_no_errors_with_pattern_input(self):
    args = self._make_args(['--input_pattern', '*',
                            '--report_path', 'some_path'])
    self._options.validate(args)
