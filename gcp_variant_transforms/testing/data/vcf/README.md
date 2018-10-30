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

`invalid-4.2-AF-mismatch.vcf` is created on `valid-4.2.vcf` by changing the
value of `AF` field in `20:17330` to have too many values (two instead of one)
and the value in `20:1234567` to have too few values (one instead of two).
By default, the job will fail as the cardinalities do not match, but it
should be successful when `allow_malformed_records` is enabled.

The folder `merge` is created to test the merge options. Three .vcf files are
created. `merge1.vcf` contains two samples, while `merge2.vcf` and `merge3.vcf`
contain one other sample, respectively. When MERGE_TO_CALLS is selected, the
variant call with `POS = 14370` is meant to merge across three files, while the
call with `POS = 1234567` is designed to be merged for `merge1.vcf` and
`merge2.vcf`.

`infer-header-fields.vcf` is used to test the case where some INFO and FORMAT
field are not defined in VCF header. Also, it contains conflicting header
definition and field values. To be more specific, `AF` from INFO is defined as
`Number=A, Type=Integer`, but in the second variant record it only provides one
value for two alternates, and it is a float value. For the FORMAT, `DP` is
defined as `Type=Integer`, but one value of DP field is float in the last
variant record.

`incompatible-field-value.vcf` is used to test the case where values provided
for the fields does not match the definition. Checkout fields `NS` and `DB`.

The folder `preprocessor` is created to test vcf to bq preprocessor. The
conflicting header definitions (`ID=GQ`), missing header definitions (`FT`), and
malformed records (last variant) are created intentionally. To be more specific,
`invalid-4.0.vcf` is a similar copy of `valid-4.0.vcf` with the following
modifications:
1. The type of field `ID=GQ` is changed from `Integer` to `Float`.
2. The penultimate variant has replaced `HQ` with `FT` in the FORMAT, which is
not defined in the header.
3. The `POS` value is removed from the last variant.

The folder `bq_to_vcf` is created to test BigQuery to VCF pipeline. In
`expected_output` folder, it saves the expected contents for some of the
integration tests included in
`gcp_variant_transforms/testing/integration/bq_to_vcf_tests`.
