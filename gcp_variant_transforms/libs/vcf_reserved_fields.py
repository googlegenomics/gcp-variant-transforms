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

"""Provides reserved INFO and FORMAT fields based on VCF 4.3 spec.

See http://samtools.github.io/hts-specs/VCFv4.3.pdf for more details.
"""

from __future__ import absolute_import

import collections
from typing import Optional  # pylint: disable=unused-import

from vcf import parser

_ReservedDefinition = collections.namedtuple('ReservedDefinition',
                                             ['id', 'num', 'type', 'desc'])


def _get_field_count(value):
  # type: (str) -> Optional[int]
  return parser.field_counts[value]


INFO_FIELDS = {
    'AA': _ReservedDefinition('AA', 1, 'String', 'Ancestral allele'),
    'AC': _ReservedDefinition('AC', _get_field_count('A'), 'Integer',
                              'Allele count in genotypes, for each ALT allele, '
                              'in the same order as listed'),
    'AD': _ReservedDefinition('AD', _get_field_count('R'), 'Integer',
                              'Total read depth for each allele'),
    'ADF': _ReservedDefinition('ADF', _get_field_count('R'), 'Integer',
                               'Read depth for each allele on the forward '
                               'strand'),
    'ADR': _ReservedDefinition('ADR', _get_field_count('R'), 'Integer',
                               'Read depth for each allele on the reverse '
                               'strand'),
    'AF': _ReservedDefinition('AF', _get_field_count('A'), 'Float',
                              'Allele frequency for each ALT allele in the '
                              'same order as listed (estimated from primary '
                              'data, not called genotypes'),
    'AN': _ReservedDefinition('AN', 1, 'Integer',
                              'Total number of alleles in called genotypes'),
    'BQ': _ReservedDefinition('BQ', 1, 'Float', 'RMS base quality'),
    'CIGAR': _ReservedDefinition('CIGAR', _get_field_count('A'), 'String',
                                 'Cigar string describing how to align an '
                                 'alternate allele to the reference allele'),
    'DB': _ReservedDefinition('DB', 0, 'Flag', 'dbSNP membership'),
    'DP': _ReservedDefinition('DP', 1, 'Integer',
                              'Combined depth across samples'),
    'END': _ReservedDefinition('END', 1, 'Integer',
                               'End position (for use with symbolic alleles)'),
    'H2': _ReservedDefinition('H2', 0, 'Flag', 'HapMap2 membership'),
    'H3': _ReservedDefinition('H3', 0, 'Flag', 'HapMap3 membership'),
    'MQ': _ReservedDefinition('MQ', 1, 'Integer', 'RMS mapping quality'),
    'MQ0': _ReservedDefinition('MQ0', 1, 'Integer',
                               'Number of MAPQ == 0 reads'),
    'NS': _ReservedDefinition('NS', 1, 'Integer',
                              'Number of samples with data'),
    'SB': _ReservedDefinition('SB', 4, 'Integer', 'Strand bias'),
    'SOMATIC': _ReservedDefinition('SOMATIC', 0, 'Flag',
                                   'Somatic mutation (for cancer genomics)'),
    'VALIDATED': _ReservedDefinition('VALIDATED', 0, 'Flag',
                                     'Validated by follow-up experiment'),
    '1000G': _ReservedDefinition('1000G', 0, 'Flag', '1000 Genomes membership')
}

FORMAT_FIELDS = {
    'AD': _ReservedDefinition('AD', _get_field_count('R'), 'Integer',
                              'Read depth for each allele'),
    'ADF': _ReservedDefinition('ADF', _get_field_count('R'), 'Integer',
                               'Read depth for each allele on the forward '
                               'strand'),
    'ADR': _ReservedDefinition('ADR', _get_field_count('R'), 'Integer',
                               'Read depth for each allele on the reverse '
                               'strand'),
    'DP': _ReservedDefinition('DP', 1, 'Integer', 'Read depth'),
    'EC': _ReservedDefinition('EC', _get_field_count('A'), 'Integer',
                              'Expected alternate allele counts'),
    'FT': _ReservedDefinition('FT', 1, 'String',
                              'Filter indicating if this genotype was '
                              '''called'''),
    'GL': _ReservedDefinition('GL', _get_field_count('G'), 'Float',
                              'Genotype likelihoods'),
    'GP': _ReservedDefinition('GP', _get_field_count('G'), 'Float',
                              'Genotype posterior probabilities'),
    'GQ': _ReservedDefinition('GQ', 1, 'Integer',
                              'Conditional genotype quality'),
    'GT': _ReservedDefinition('GT', 1, 'String', 'Genotype'),
    'HQ': _ReservedDefinition('HQ', 2, 'Integer', 'Haplotype quality'),
    'MQ': _ReservedDefinition('MQ', 1, 'Integer', 'RMS mapping quality'),
    'PL': _ReservedDefinition('PL', _get_field_count('G'), 'Integer',
                              'Phred-scaled genotype likelihoods rounded to '
                              'the closest integer'),
    'PQ': _ReservedDefinition('PQ', 1, 'Integer', 'Phasing quality'),
    'PS': _ReservedDefinition('PS', 1, 'Integer', 'Phase set')
}
