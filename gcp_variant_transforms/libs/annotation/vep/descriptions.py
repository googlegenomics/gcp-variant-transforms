# Copyright 2018 Google Inc.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Names of VEP annotation fields and their corresponding descriptions.

Original descriptions are part of the VEP documentation and can be found at:
http://useast.ensembl.org/info/docs/tools/vep/vep_formats.html#output
"""

VEP_DESCRIPTIONS = {
    'AA_AF': ('Frequency of existing variant in NHLBI-ESP African American '
              'population'),
    'AF': 'Frequency of existing variant in 1000 Genomes',
    'AFR_AF': ('Frequency of existing variant in 1000 Genomes combined African '
               'population'),
    'ALLELE_NUM': ('Allele number from input; 0 is reference, 1 is first '
                   'alternate etc'),
    'AMBIGUITY': 'IUPAC allele ambiguity code',
    'AMR_AF': ('Frequency of existing variant in 1000 Genomes combined '
               'American population'),
    'APPRIS': ('Annotates alternatively spliced transcripts as primary or '
               'alternate based on a range of computational methods. NB: not '
               'available for GRCh37'),
    'ASN_AF': ('Frequency of existing variant in 1000 Genomes combined Asian '
               'population'),
    'Allele': 'The variant allele used to calculate the consequence',
    'Amino_acids': ('Reference and variant amino acids. Only given if the '
                    'variant affects the protein-coding sequence'),
    'BAM_EDIT': 'Indicates success or failure of edit using BAM file',
    'BIOTYPE': 'Biotype of transcript or regulatory feature',
    'CANONICAL': ('A flag indicating if the transcript is denoted as the '
                  'canonical transcript for this gene'),
    'CCDS': 'The CCDS identifer for this transcript, where applicable',
    'CDS_position': 'Relative position of base pair in coding sequence',
    'CELL_TYPE': ('List of cell types and classifications for regulatory '
                  'feature'),
    'CLIN_SIG': 'ClinVar clinical significance of the dbSNP variant',
    'Codons': 'The alternative codons with the variant base in upper case',
    'Consequence': 'Consequence type of this variant',
    'DISTANCE': 'Shortest distance from variant to transcript',
    'DOMAINS': 'The source and identifer of any overlapping protein domains',
    'EAS_AF': ('Frequency of existing variant in 1000 Genomes combined East '
               'Asian population'),
    'EA_AF': ('Frequency of existing variant in NHLBI-ESP European American '
              'population'),
    'ENSP': 'The Ensembl protein identifier of the affected transcript',
    'EUR_AF': ('Frequency of existing variant in 1000 Genomes combined '
               'European population'),
    'EXON': 'The exon number (out of total number)',
    'Existing_variation': 'Known identifier of existing variant',
    'Extra': ('This column contains extra information as key=value pairs '
              'separated by \';\'.'),
    'FLAGS': 'Transcript quality flags (cds_start_NF, cds_start_NF)',
    'FREQS': 'Frequencies of overlapping variants used in filtering',
    'Feature': 'Ensembl stable ID of feature',
    'Feature_type': ('Type of feature. Currently one of Transcript, '
                     'RegulatoryFeature, MotifFeature.'),
    'GENE_PHENO': ('Indicates if overlapped gene is associated with a '
                   'phenotype, disease or trait'),
    'GIVEN_REF': 'Reference allele from input',
    'Gene': 'Ensembl stable ID of affected gene',
    'HGNC_ID': 'HUGO Gene Nomenclature Committee approved symbol',
    'HGVS_OFFSET': ('Indicates by how many bases the HGVS notations for this '
                    'variant have been shifted'),
    'HGVSc': 'The HGVS coding sequence name',
    'HGVSg': 'The HGVS genomic sequence name',
    'HGVSp': 'The HGVS protein sequence name',
    'HIGH_INF_POS': ('A flag indicating if the variant falls in a high '
                     'information position of a transcription factor binding '
                     'profile (TFBP)'),
    'IMPACT': 'The impact modifier for the consequence type',
    'IND': 'Individual name',
    'INTRON': 'The intron number (out of total number)',
    'Location': 'In standard coordinate format (chr:start or chr:start-end)',
    'MAX_AF': ('Maximum observed allele frequency in 1000 Genomes, ESP and '
               'gnomAD'),
    'MAX_AF_POPS': ('Populations in which maximum allele frequency was '
                    'observed'),
    'MINIMISED': ('Alleles in this variant have been converted to minimal '
                  'representation before consequence calculation'),
    'MOTIF_NAME': ('The source and identifier of a transcription factor '
                   'binding profile aligned at this position'),
    'MOTIF_POS': 'The relative position of the variation in the aligned TFBP',
    'MOTIF_SCORE_CHANGE': ('The difference in motif score of the reference and '
                           'variant sequences for the TFBP'),
    'NEAREST': 'Identifier(s) of nearest transcription start site',
    'OVERLAP_BP': ('Number of base pairs overlapping with the corresponding '
                   'variation feature'),
    'OVERLAP_PC': ('Percentage of corresponding variation feature overlapped '
                   'by the given input'),
    'PHENO': ('Indicates if existing variant is associated with a phenotype, '
              'disease or trait; multiple values correspond to multiple values '
              'in the Existing_variation field'),
    'PICK': ('Indicates if this block of consequence data was picked by '
             '--flag_pick or --flag_pick_allele'),
    'PUBMED': 'Pubmed ID(s) of publications that cite existing variant',
    'PolyPhen': 'The PolyPhen prediction and/or score',
    'Protein_position': 'Relative position of amino acid in protein',
    'REFSEQ_MATCH': ('The RefSeq transcript match status; contains a number of '
                     'flags indicating whether this RefSeq transcript matches '
                     'the underlying reference sequence and/or an Ensembl '
                     'transcript. NB: not available for GRCh37.'),
    'SAS_AF': ('Frequency of existing variant in 1000 Genomes combined South '
               'Asian population'),
    'SIFT': ('The SIFT prediction and/or score, with both given as '
             'prediction(score)'),
    'SOMATIC': ('Somatic status of existing variant(s); multiple values '
                'correspond to multiple values in the Existing_variation '
                'field'),
    'STRAND': 'The DNA strand (1 or -1) on which the transcript/feature lies',
    'SV': 'IDs of overlapping structural variants',
    'SWISSPROT': 'Best match UniProtKB/Swiss-Prot accession of protein product',
    'SYMBOL': 'The gene symbol',
    'SYMBOL_SOURCE': 'The source of the gene symbol',
    'TREMBL': 'Best match UniProtKB/TrEMBL accession of protein product',
    'TSL': 'Transcript support level. NB: not available for GRCh37',
    'UNIPARC': 'Best match UniParc accession of protein product',
    'USED_REF': 'Reference allele as used to get consequences',
    'Uploaded_variation': 'Identifier of uploaded variant',
    'VARIANT_CLASS': 'Sequence Ontology variant class',
    'ZYG': 'Zygosity of individual genotype at this locus',
    'cDNA_position': 'Relative position of base pair in cDNA sequence',
    'gnomAD_AF': ('Frequency of existing variant in gnomAD exomes combined '
                  'population'),
    'gnomAD_AFR_AF': ('Frequency of existing variant in gnomAD exomes '
                      'African/American population'),
    'gnomAD_AMR_AF': ('Frequency of existing variant in gnomAD exomes American '
                      'population'),
    'gnomAD_ASJ_AF': ('Frequency of existing variant in gnomAD exomes '
                      'Ashkenazi Jewish population'),
    'gnomAD_EAS_AF': ('Frequency of existing variant in gnomAD exomes East '
                      'Asian population'),
    'gnomAD_FIN_AF': ('Frequency of existing variant in gnomAD exomes Finnish '
                      'population'),
    'gnomAD_NFE_AF': ('Frequency of existing variant in gnomAD exomes '
                      'Non-Finnish European population'),
    'gnomAD_OTH_AF': ('Frequency of existing variant in gnomAD exomes combined '
                      'other combined populations'),
    'gnomAD_SAS_AF': ('Frequency of existing variant in gnomAD exomes South '
                      'Asian population')
}
