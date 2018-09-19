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

"""Provides reserved INFO and FORMAT fields based on VCF 4.3 spec ."""

from __future__ import absolute_import

from vcf import parser

_Format = parser._Format


def _get_field_count(value):
  return parser.field_counts[value]


def _create_info(field, number, field_type, description):
  return parser._Info(field, number, field_type, description, None, None)


INFO_FIELDS = {
    'AA': _create_info('AA', 1, 'String', 'Ancestral allele'),
    'AC': _create_info('AC', _get_field_count('A'), 'Integer',
                       'Allele count in genotypes, for each ALT allele, in the '
                       'same order as listed'),
    'AD': _create_info('AD', _get_field_count('R'), 'Integer',
                       'Total read depth for each allele'),
    'ADF': _create_info('ADF', _get_field_count('R'), 'Integer',
                        'Read depth for each allele on the forward strand'),
    'ADR': _create_info('ADR', _get_field_count('R'), 'Integer',
                        'Read depth for each allele on the reverse strand'),
    'AF': _create_info('AF', _get_field_count('A'), 'Float',
                       'Allele frequency for each ALT allele in the same order '
                       'as listed (estimated from primary data, not called '
                       'genotypes'),
    'AN': _create_info('AN', 1, 'Integer',
                       'Total number of alleles in called genotypes'),
    'BQ': _create_info('BQ', 1, 'Float', 'RMS base quality'),
    'CIGAR': _create_info('CIGAR', _get_field_count('A'), 'String',
                          'Cigar string describing how to align an alternate '
                          'allele to the reference allele'),
    'DB': _create_info('DB', 0, 'Flag', 'dbSNP membership'),
    'DP': _create_info('DP', 1, 'Integer', 'Combined depth across samples'),
    'END': _create_info('END', 1, 'Integer',
                        'End position (for use with symbolic alleles)'),
    'H2': _create_info('H2', 0, 'Flag', 'HapMap2 membership'),
    'H3': _create_info('H3', 0, 'Flag', 'HapMap3 membership'),
    'MQ': _create_info('MQ', 1, 'Integer', 'RMS mapping quality'),
    'MQ0': _create_info('MQ0', 1, 'Integer', 'Number of MAPQ == 0 reads'),
    'NS': _create_info('NS', 1, 'Integer', 'Number of samples with data'),
    'SB': _create_info('SB', 4, 'Integer', 'Strand bias'),
    'SOMATIC': _create_info('SOMATIC', 0, 'Flag',
                            'Somatic mutation (for cancer genomics)'),
    'VALIDATED': _create_info('VALIDATED', 0, 'Flag',
                              'Validated by follow-up experiment'),
    '1000G': _create_info('1000G', 0, 'Flag', '1000 Genomes membership')
}

FORMAT_FIELDS = {
    'AD': _Format('AD', _get_field_count('R'), 'Integer',
                  'Read depth for each allele'),
    'ADF': _Format('ADF', _get_field_count('R'), 'Integer',
                   'Read depth for each allele on the forward strand'),
    'ADR': _Format('ADR', _get_field_count('R'), 'Integer',
                   'Read depth for each allele on the reverse strand'),
    'DP': _Format('DP', 1, 'Integer', 'Read depth'),
    'EC': _Format('EC', _get_field_count('A'), 'Integer',
                  'Expected alternate allele counts'),
    'FT': _Format('FT', 1, 'String',
                  'Filter indicating if this genotype was ''called'''),
    'GL': _Format('GL', _get_field_count('G'), 'Float',
                  'Genotype likelihoods'),
    'GP': _Format('GP', _get_field_count('G'), 'Float',
                  'Genotype posterior probabilities'),
    'GQ': _Format('GQ', 1, 'Integer', 'Conditional genotype quality'),
    'GT': _Format('GT', 1, 'String', 'Genotype'),
    'HQ': _Format('HQ', 2, 'Integer', 'Haplotype quality'),
    'MQ': _Format('MQ', 1, 'Integer', 'RMS mapping quality'),
    'PL': _Format('PL', _get_field_count('G'), 'Integer',
                  'Phred-scaled genotype likelihoods rounded to the closest '
                  'integer'),
    'PQ': _Format('PQ', 1, 'Integer', 'Phasing quality'),
    'PS': _Format('PS', 1, 'Integer', 'Phase set')
}
