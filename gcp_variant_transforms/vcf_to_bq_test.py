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

"""Tests for vcf_to_bq script."""

import mock
import unittest

from apache_beam.io.gcp.internal.clients import bigquery
from apitools.base.py import exceptions

from gcp_variant_transforms.vcf_to_bq import _validate_bq_path


class VcfToBqTest(unittest.TestCase):
  """Tests cases for the ``vcf_to_bq`` script."""

  def test_valid_table_path(self):
    output_table = 'project:dataset.table'
    client = mock.Mock()
    client.datasets.Get.return_value = bigquery.Dataset(
        datasetReference=bigquery.DatasetReference(
            projectId='project', datasetId='dataset'))
    _validate_bq_path(output_table, client)

  def test_no_project(self):
    output_table = 'dataset.table'
    client = mock.Mock()
    self.assertRaises(ValueError, _validate_bq_path, output_table, client)

  def test_invalid_table_path(self):
    no_table = 'project:dataset'
    incorrect_sep1 = 'project.dataset.table'
    incorrect_sep2 = 'project:dataset:table'
    client = mock.Mock()
    self.assertRaises(ValueError, _validate_bq_path, no_table, client)
    self.assertRaises(ValueError, _validate_bq_path, incorrect_sep1, client)
    self.assertRaises(ValueError, _validate_bq_path, incorrect_sep2, client)

  def test_dataset_does_not_exists(self):
    output_table = 'project:dataset.table'
    client = mock.Mock()
    client.datasets.Get.side_effect = exceptions.HttpError(
        response={'status': '404'}, url='', content='')
    self.assertRaises(ValueError, _validate_bq_path, output_table, client)
