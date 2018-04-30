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
import collections

from typing import List  # pylint: disable=unused-import

import mock

from apache_beam.io.gcp.internal.clients import bigquery
from apitools.base.py import exceptions

from gcp_variant_transforms.options import variant_transform_options


BigQueryArgs = collections.namedtuple('BigQueryArgs', ['output_table'])


class BigQueryWriteOptionsTest(unittest.TestCase):
  """Tests cases for the BigQueryWriteOptions class."""

  def setUp(self):
    self.options = variant_transform_options.BigQueryWriteOptions()

  def test_valid_table_path(self):
    args = BigQueryArgs('project:dataset.table')
    client = mock.Mock()
    client.datasets.Get.return_value = bigquery.Dataset(
        datasetReference=bigquery.DatasetReference(
            projectId='project', datasetId='dataset'))
    self.options.validate(args, client)

  def test_no_project(self):
    args = BigQueryArgs('dataset.table')
    client = mock.Mock()
    self.assertRaises(ValueError, self.options.validate, args, client)

  def test_invalid_table_path(self):
    no_table = BigQueryArgs('project:dataset')
    incorrect_sep1 = BigQueryArgs('project.dataset.table')
    incorrect_sep2 = BigQueryArgs('project:dataset:table')
    client = mock.Mock()
    self.assertRaises(ValueError, self.options.validate, no_table, client)
    self.assertRaises(ValueError, self.options.validate, incorrect_sep1, client)
    self.assertRaises(ValueError, self.options.validate, incorrect_sep2, client)

  def test_dataset_does_not_exists(self):
    args = BigQueryArgs('project:dataset.table')
    client = mock.Mock()
    client.datasets.Get.side_effect = exceptions.HttpError(
        response={'status': '404'}, url='', content='')
    self.assertRaises(ValueError, self.options.validate, args, client)


class AnnotationOptionsTest(unittest.TestCase):

  def setUp(self):
    self._options = variant_transform_options.AnnotationOptions()

  def _make_args(self, args):
    # type: (List[str]) -> argparse.Namespace
    parser = argparse.ArgumentParser()
    parser.register('type', 'bool', lambda v: v.lower() == 'true')
    self._options.add_arguments(parser)
    namespace, remining_args = parser.parse_known_args(args)
    assert not remining_args
    return namespace

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

  def test_failure_for_run_not_set(self):
    args = self._make_args(['--annotation_output_dir', 'gs://GOOD_DIR',
                            '--vep_image_uri', 'AN_IMAGE',
                            '--vep_cache_path', 'gs://VEP_CACHE'])
    self.assertRaises(ValueError, self._options.validate, args)
