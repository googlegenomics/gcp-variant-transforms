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

"""Util functions for accessing testdata."""

from __future__ import absolute_import

import os.path

from gcp_variant_transforms.libs import hashing_util

__all__ = ['get_full_file_path', 'get_full_dir']

def hash_name(sample_name, file_name=''):
  return hashing_util.generate_sample_id(sample_name, file_name)


def get_full_file_path(file_name):
  """Returns the full path of the specified ``file_name`` from ``data``."""
  return os.path.join(
      os.path.dirname(__file__), 'data', 'vcf', file_name)


def get_full_dir():
  """Returns the full path of the  ``data`` directory."""
  return os.path.join(os.path.dirname(__file__), 'data', 'vcf')


def get_sample_vcf_file_lines():
  return get_sample_vcf_header_lines() + get_sample_vcf_record_lines()


def get_sample_vcf_header_lines():
  return [
      '##fileformat=VCFv4.2\n',
      '##INFO=<ID=NS,Number=1,Type=Integer,Description="Number samples">\n',
      '##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">\n',
      '##INFO=<ID=HG,Number=G,Type=Integer,Description="IntInfo_G">\n',
      '##INFO=<ID=HR,Number=R,Type=String,Description="ChrInfo_R">\n',
      '##FILTER=<ID=MPCBT,Description="Mate pair count below 10">\n',
      '##ALT=<ID=INS:ME:MER,Description="Insertion of MER element">\n',
      '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n',
      '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="GQ">\n',
      '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1	SAMPLE2\n',
  ]


def get_sample_vcf_record_lines():
  return [
      '20	14370	.	G	A	29	PASS	AF=0.5	GT:GQ	0|0:48 1|0:48\n',
      '20	17330	.	T	A	3	q10	AF=0.017	GT:GQ	0|0:49	0|1:3\n',
      '20	1110696	.	A	G,T	67	PASS	AF=0.3,0.7	GT:GQ	1|2:21	2|1:2\n',
      '20	1230237	.	T	.	47	PASS	.	GT:GQ	0|0:54	0|0:48\n',
      '19	1234567	.	GTCT	G,GTACT	50	PASS	.	GT:GQ	0/1:35	0/2:17\n',
      '20	1234	rs123	C	A,T	50	PASS	AF=0.5,0.5	GT:GQ	0/0:48	1/0:20\n',
      '19	123	rs1234	GTC	.	40	q10;s50	NS=2	GT:GQ	1|0:48	0/1:.\n',
      '19	12	.	C	<SYMBOLIC>	49	q10	AF=0.5	GT:GQ	0|1:45 .:.\n'
  ]
