# Copyright 2020 Google LLC.  All Rights Reserved.
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

"""Tests for densify_variants module."""

from __future__ import absolute_import

import unittest

from apache_beam import combiners
from apache_beam.pvalue import AsSingleton
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.transforms import Create

from gcp_variant_transforms.testing import asserts
from gcp_variant_transforms.transforms.sample_mapping_table import GetSampleIds
from gcp_variant_transforms.transforms.sample_mapping_table import GetSampleNames
from gcp_variant_transforms.transforms.sample_mapping_table import SAMPLE_ID_COLUMN
from gcp_variant_transforms.transforms.sample_mapping_table import SAMPLE_NAME_COLUMN
from gcp_variant_transforms.transforms.sample_mapping_table import SampleIdToNameDict
from gcp_variant_transforms.transforms.sample_mapping_table import SampleNameToIdDict
from gcp_variant_transforms.transforms.sample_mapping_table import FILE_PATH_COLUMN



def _generate_bq_row(sample_id, sample_name, file_path):
  return {SAMPLE_ID_COLUMN: sample_id,
          SAMPLE_NAME_COLUMN: sample_name,
          FILE_PATH_COLUMN: file_path}

BQ_ROWS = [_generate_bq_row(1, 'N01', 'file1'),
           _generate_bq_row(2, 'N02', 'file2'),
           _generate_bq_row(3, 'N03', 'file3')]

class SampleIdToNameDictTest(unittest.TestCase):
  """Test cases for the ``SampleTableToDict`` transform."""

  def test_sample_table_to_dict(self):
    expected_dict = {1: 'N01',
                     2: 'N02',
                     3: 'N03'}

    pipeline = TestPipeline()
    hash_table = (
        pipeline
        | Create(BQ_ROWS)
        | 'GenerateHashTable' >> SampleIdToNameDict())
    assert_that(hash_table, asserts.dict_values_equal(expected_dict))
    pipeline.run()

class SampleNameToIdDictTest(unittest.TestCase):
  """Test cases for the ``SampleTableToDict`` transform."""

  def test_sample_table_to_dict(self):
    expected_dict = {'N01': 1, 'N02': 2, 'N03': 3}

    pipeline = TestPipeline()
    hash_table = (
        pipeline
        | Create(BQ_ROWS)
        | 'GenerateHashTable' >> SampleNameToIdDict())

    assert_that(hash_table, asserts.dict_values_equal(expected_dict))

    pipeline.run()

class GetSampleNamesTest(unittest.TestCase):
  """Test cases for the ``SampleTableToDict`` transform."""

  def test_get_sample_names(self):
    hash_dict = {1: 'N01', 2: 'N02', 3: 'N03', 4: 'N04'}
    sample_ids = [1, 2, 3, 4]
    expected_sample_names = ['N01', 'N02', 'N03', 'N04']

    pipeline = TestPipeline()
    hash_dict_pc = (
        pipeline
        | 'CreateHashDict' >> Create(hash_dict)
        | combiners.ToDict())
    sample_names = (
        pipeline
        | Create(sample_ids)
        | 'GetSampleNames' >> GetSampleNames(AsSingleton(hash_dict_pc)))

    assert_that(sample_names, asserts.items_equal(expected_sample_names))
    pipeline.run()

class GetSampleIdsTest(unittest.TestCase):
  """Test cases for the ``SampleTableToDict`` transform."""

  def test_get_sample_ids(self):
    hash_dict = {'N01': 1, 'N02': 2, 'N03': 3, 'N04': 4}
    sample_names = ['N01', 'N02', 'N03', 'N04']
    expected_sample_ids = [1, 2, 3, 4]

    pipeline = TestPipeline()
    hash_dict_pc = (
        pipeline
        | 'CreateHashDict' >> Create(hash_dict)
        | combiners.ToDict())
    sample_ids = (
        pipeline
        | Create(sample_names)
        | 'GetSampleNames' >> GetSampleIds(AsSingleton(hash_dict_pc)))

    assert_that(sample_ids, asserts.items_equal(expected_sample_ids))
    pipeline.run()
