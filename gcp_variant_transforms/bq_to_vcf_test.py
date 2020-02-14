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

"""Tests for `bq_to_vcf` module."""

import collections
import unittest

from apache_beam.io import filesystems
from apache_beam.io.gcp.internal.clients import bigquery

from gcp_variant_transforms import bq_to_vcf
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.testing import bigquery_schema_util
from gcp_variant_transforms.testing import temp_dir


class BqToVcfTest(unittest.TestCase):
  """Test cases for the `bq_to_vcf` module."""

  def _create_mock_args(self, **args):
    return collections.namedtuple('MockArgs', args.keys())(*args.values())

  def test_write_vcf_data_header(self):
    lines = [
        '##fileformat=VCFv4.2\n',
        '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
        '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n',
        '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="GQ">\n',
        '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	\n']
    with temp_dir.TempDir() as tempdir:
      representative_header = tempdir.create_temp_file(lines=lines)
      file_path = filesystems.FileSystems.join(tempdir.get_path(),
                                               'data_header')
      bq_to_vcf._write_vcf_header_with_sample_names(
          ['Sample 1', 'Sample 2'],
          ['#CHROM', 'POS', 'ID', 'REF', 'ALT'],
          representative_header,
          file_path)
      expected_content = [
          '##fileformat=VCFv4.2\n',
          '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
          '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
          '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n',
          '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="GQ">\n',
          '#CHROM\tPOS\tID\tREF\tALT\tSample 1\tSample 2\n'
      ]
      with filesystems.FileSystems.open(file_path) as f:
        content = f.readlines()
        self.assertEqual(content, expected_content)

  def test_get_bigquery_query_no_region(self):
    args = self._create_mock_args(
        input_table='my_bucket:my_dataset.my_table',
        genomic_regions=None)
    schema = bigquery.TableSchema()
    schema.fields.append(bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.REFERENCE_NAME,
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='Reference name.'))
    self.assertEqual(bq_to_vcf._get_bigquery_query(args, schema),
                     'SELECT reference_name FROM '
                     '`my_bucket.my_dataset.my_table`')

  def test_get_bigquery_query_with_regions(self):
    args_1 = self._create_mock_args(
        input_table='my_bucket:my_dataset.my_table',
        genomic_regions=['c1:1,000-2,000', 'c2'])
    schema = bigquery.TableSchema()
    schema.fields.append(bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.REFERENCE_NAME,
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='Reference name.'))
    schema.fields.append(bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.START_POSITION,
        type=bigquery_util.TableFieldConstants.TYPE_INTEGER,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description=('Start position (0-based). Corresponds to the first base '
                     'of the string of reference bases.')))
    expected_query = (
        'SELECT reference_name, start_position FROM '
        '`my_bucket.my_dataset.my_table` WHERE '
        '(reference_name="c1" AND start_position>=1000 AND end_position<=2000) '
        'OR (reference_name="c2" AND start_position>=0 AND '
        'end_position<=9223372036854775807)'
    )
    self.assertEqual(bq_to_vcf._get_bigquery_query(args_1, schema),
                     expected_query)

  def test_get_query_columns(self):
    schema = bigquery.TableSchema()
    schema.fields.append(bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.REFERENCE_NAME,
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='Reference name.'))
    schema.fields.append(bigquery.TableFieldSchema(
        name='partition_date_please_ignore',
        type='Date',
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='Column required by BigQuery partitioning logic.'))
    expected_columns = [bigquery_util.ColumnKeyConstants.REFERENCE_NAME]
    self.assertEqual(bq_to_vcf._get_query_columns(schema), expected_columns)

  def test_get_annotation_names(self):
    schema_with_annotations = bigquery_schema_util.get_sample_table_schema(
        with_annotation_fields=True)

    self.assertEqual(
        bq_to_vcf._extract_annotation_names(schema_with_annotations),
        {'CSQ': ['Consequence', 'IMPACT']})
    schema_with_no_annotation = bigquery_schema_util.get_sample_table_schema()
    self.assertEqual(
        bq_to_vcf._extract_annotation_names(schema_with_no_annotation),
        {})

  def test_get_annotation_names_multiple_annotations(self):
    schema = bigquery.TableSchema()
    alternate_bases_record = bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.ALTERNATE_BASES,
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='One record for each alternate base (if any).')
    annotation_record_1 = bigquery.TableFieldSchema(
        name='CSQ_1',
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='desc')
    annotation_record_1.fields.append(bigquery.TableFieldSchema(
        name='allele',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='desc.'))
    annotation_record_1.fields.append(bigquery.TableFieldSchema(
        name='Consequence',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='desc.'))
    alternate_bases_record.fields.append(annotation_record_1)
    annotation_record_2 = bigquery.TableFieldSchema(
        name='CSQ_2',
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='desc')
    annotation_record_2.fields.append(bigquery.TableFieldSchema(
        name='allele',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='desc.'))
    annotation_record_2.fields.append(bigquery.TableFieldSchema(
        name='IMPACT',
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='desc.'))
    alternate_bases_record.fields.append(annotation_record_2)
    schema.fields.append(alternate_bases_record)
    self.assertEqual(
        bq_to_vcf._extract_annotation_names(schema),
        {'CSQ_1': ['allele', 'Consequence'], 'CSQ_2': ['allele', 'IMPACT']})
