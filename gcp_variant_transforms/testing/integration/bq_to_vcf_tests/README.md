This file summarizes the BigQuery to VCF pipeline integration tests and the
purpose for each file within the current folder.

All input tables in the data set
`gcp-variant-transforms-test.bq_to_vcf_integration_tests` are loaded using
Variant Transforms.

Table `4_0` is created by loading the file
"gs://gcp-variant-transforms-testfiles/small_tests/valid-4.0.vcf". The test case
`4_0.json` is used to test the basic functionalities, in which the meta
information is inferred from the BigQuery schema.
`4_0_with_representative_header_file.json` is used to test the option
`--representative_header_file`, which loads all meta information from
`representative_header_file`. The test case `4_0_option_customized_export.json`
is used to test the options `--genomic_regions` and `--call_names`.

Table `4_2` is created by loading the file
"gs://gcp-variant-transforms-testfiles/small_tests/valid-4.2.vcf". Note that in
the BigQuery schema, FORMAT `GL` is defined as `INTEGER`, which is conflict with
the reserved definition (`Type=FLOAT`) in [VCF 4.3 Spec](
http://samtools.github.io/hts-specs/VCFv4.3.pdf). Moreover, this table
contains unicode `BÑD`. The test case `4_2_option_allow_incompatible_schema` is
used to test the option `--allow_incompatible_schema`.

Table `platinum_NA12877_hg38_10K_lines` is created by loading the file
"gs://gcp-variant-transforms-testfiles/small_tests/platinum_NA12877_hg38_10K_lines_manual_vep_orig_output.vcf",
with `--annotation_fields` specified as `CSQ`. The test case
`platinum_option_number_of_bases_per_shard.json` is used to test the option
`--number_of_bases_per_shard`, as well as the exporting of annotations.

Table `merge_option_move_to_calls` is created by loading files
"gs://gcp-variant-transforms-testfiles/small_tests/merge/*.vcf" with
`variant_merge_strategy=MOVE_TO_CALLS`. The test case
`densify_samples.vcf` is used to test the densify process for BigQuery table
with missing variants for some of the calls.
