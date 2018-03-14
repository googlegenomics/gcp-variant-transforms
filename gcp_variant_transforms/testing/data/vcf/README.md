This file summarizes the contents and the purpose for each files/folder within
current folder.

`valid-4.0.vcf`, `valid-4.0.vcf.gz`, `valid-4.0.vcf.bz2` are used to test
Variant Call Format version 4.0 files in the form of uncompressed, gzip and
bzip, respectively. For more details on the VCF format version specifications,
please refer to [VCF Specification](https://samtools.github.io/hts-specs/).

`valid-4.1-large.vcf`, `valid-4.1-large.vcf.gz` are used to test version 4.1
uncompressed, gzip VCF file, respectively.

`valid-4.2.vcf`, `valid-4.2.vcf.gz` are used to test version 4.2 uncompressed,
gzip VCF file, respectively.

`invalid-4.0-AF-field-removed.vcf` is created by removing `AF` field definition
from the meta-information based on `valid-4.0.vcf`. It is used to test `AF`
field can be parsed correctly given a representative_header_file containing
`AF`.

`invalid-4.0-POS-empty.vcf` is created based on `valid-4.0.vcf` by removing the
POS value for the first entry. It is used to test when `allow_malformed_records`
is enabled, failed VCF record reads will not raise errors and the BigQuery table
can still be generated.

The folder `merge` is created to test the merge options. Three .vcf files are
created. `merge1.vcf` contains two samples, while `merge2.vcf` and `merge3.vcf`
contain one other sample, respectively. When MERGE_TO_CALLS is selected, the
variant call with `POS = 14370` is meant to merge across three files, while the
call with `POS = 1234567` is designed to be merged for `merge1.vcf` and
`merge2.vcf`.
