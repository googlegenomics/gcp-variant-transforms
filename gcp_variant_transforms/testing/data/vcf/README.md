This file summarizes the contents and the purpose for each files/folder within
current folder.

`valid-4.0.vcf`, `valid-4.0.vcf.gz`, `valid-4.0.vcf.bz2` are used to test
version 4.0 uncompressed, gzip, bzip VCF file, respectively.

`valid-4.1-large.vcf`, `valid-4.1-large.vcf.gz` are used to test version 4.1
uncompressed, gzip VCF file, respectively.

`valid-4.2.vcf`, `valid-4.2.vcf.gz` are used to test version 4.2 uncompressed,
gzip VCF file, respectively.

`invalid-4.0-AF-field-removed.vcf` is created by removing `AF` field definition
from the meta-information based on `valid-4.0.vcf`. It is used to test `AF`
field can be parsed correctly given a representative_header_file contains `AF`.

`invalid-4.0-POS-empty.vcf` is created based on `valid-4.0.vcf` by removing the
POS value for the first entry. It is used to test when `allow_malformed_records`
is enabled, failed VCF record reads will not raise errors and the BQ can still
generate correctly.

The folder `merge` is created to test the merge options. Three .vcf files are
created. `merge1.vcf` contains two samples, while `merge2.vcf` and `merge3.vcf`
contain one other sample, respectively. When MERGE_TO_CALLS is selected, the
variant call where `POS = 14370` is meant to merge across three files, while the
call where `POS = 1234567` is designed to be merged for `merge1.vcf` and
`merge2.vcf`.
