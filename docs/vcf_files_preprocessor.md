# VCF files preprocessor

The VCF files preprocessor is used for validating the datasets such that the
inconsistencies can be easily identified. It can be used as a standalone
validator to check the validity of the VCF files, or as a helper tool for
[VCF to BigQuery pipeline](/README.md/#loading-vcf-files-to-bigquery). The VCF
to BigQuery loading process may fail for ill-defined datasets, which can be
resolved by applying
[conflicts resolving strategies](multiple_files.md#field-compatibility).
This tool provides insights on what conflicts are there in the VCF files and the
resolutions that will be applied when loading the data into BigQuery. Meanwhile,
it also makes the manual corrections easier if the proposed resolutions are not
satisfactory.

This tool generates a report that includes three types of inconsistencies:
* Conflicting header definitions: It is common to have different definitions
  for the same field in different VCF files. For this type of inconsistency, the
  report lists all conflicting definitions, the corresponding file paths (up to
  5), and the suggested resolutions.
* Malformed Records (Optional): There can be malformed records that cannot be
  parsed correctly by the VCF parser. In this case, the file paths and the
  malformed variant records are included in the report for reference.
* Inferred headers (Optional): Occasionally, some fields may be used directly
  without a definition in the VCF files, or though a definition is provided, the
  field value does not match the field definition. This tool can infer the type
  and num for such fields and provides them in the report. For the latter case,
  two kinds of the mismatches are handled:
  1. The defined field type is `Integer`, but the provided value is float.
     Correct the type to be `Float`.
  2. Defined num is `A`, but the provided values do not have the same
     cardinality as the alternate bases. Infer the num to be `None`.

## Running VCF files to BigQuery Preprocessor

Similar to running the
[VCF to BigQuery pipeline](/README.md/#loading-vcf-files-to-bigquery), the
preprocessor can also be run using docker or directly from the source.

### Using docker

First, set up the preprocessor pipeline configurations shown below and save it
as `vcf_to_bigquery_preprocess.yaml`. The parameters that you need to replace
are:

* `my_project`: This is your project name that contains the BigQuery dataset.
* `gs://my_bucket/vcffiles/*.vcf`: A location in Google Cloud Storage where the
  VCF file are stored. You may specify a single file or provide a pattern to
  analyze multiple files at once.
* `report_path`: This can be any path in Google Cloud Storage that your project
  has write access to. It is used to store the report from the preprocessor
  tool.
* `resolved_headers_path`: Optional. This can be any path in Google Cloud
  Storage that your project has write access to. It is used to store the
  resolved headers which can be further used as a representative header (via
  `--representative_header_file`) for the
  [VCF to BigQuery pipeline](/README.md/#loading-vcf-files-to-bigquery).
* `gs://my_bucket/staging` and `gs://my_bucket/temp`: These can be any folder in
  Google Cloud Storage that your project has write access to. These are used to
  store temporary files needed for running the pipeline.

`report_all_conflicts` is optional. By default, the preprocessor reports the inconsistent
header definitions across multiple VCF files. By setting this parameter to true,
it also checks the undefined headers and the malformed records.

```yaml
name: vcf-to-bigquery-preprocess-pipeline
docker:
  imageName: gcr.io/gcp-variant-transforms/gcp-variant-transforms
  cmd: |
    ./opt/gcp_variant_transforms/bin/vcf_to_bq_preprocess \
      --project my_project \
      --input_pattern gs://my_bucket/vcffiles/*.vcf \
      --report_path gs://my_bucket/preprocess/report.tsv \
      --resolved_headers_path gs://my_bucket/preprocess/headers.vcf \
      --report_all_conflicts true \
      --staging_location gs://my_bucket/staging \
      --temp_location gs://my_bucket/temp \
      --job_name vcf-to-bigquery-preprocess \
      --runner DataflowRunner
```

Next, run the following command to launch the pipeline. Replace `my_project`
with your project name, `gs://my_bucket/temp/runner_logs` with a Cloud Storage
folder to store the logs from the pipeline.

```bash
gcloud alpha genomics pipelines run \
    --project my_project \
    --pipeline-file vcf_to_bigquery_preprocess.yaml \
    --logging gs://my_bucket/temp/runner_logs \
    --zones us-west1-b
```

### Running from github

In addition to using the docker image, you may run the pipeline directly from
source.

Example command for DirectRunner:

```bash
python -m gcp_variant_transforms.vcf_to_bq_preprocess \
  --input_pattern gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf \
  --report_path gs://my_bucket/preprocess/report.tsv
  --resolved_headers_path gs://my_bucket/preprocess/headers.vcf \
  --report_all_conflicts true
```

Example command for DataflowRunner:

```bash
python -m gcp_variant_transforms.vcf_to_bq_preprocess \
  --input_pattern gs://my_bucket/vcffiles/*.vcf \
  --report_path gs://my_bucket/preprocess/report.tsv \
  --resolved_headers_path gs://my_bucket/preprocess/headers.vcf \
  --report_all_conflicts true \
  --project my_project \
  --staging_location gs://my_bucket/staging \
  --temp_location gs://my_bucket/temp \
  --job_name vcf-to-bigquery-preprocess \
  --setup_file ./setup.py \
  --runner DataflowRunner
```

### Example

The following is the conflicts report generated by running the preprocessor on
the data set
[1000 Genomes](https://cloud.google.com/genomics/docs/public-datasets/1000-genomes).

Header Conflicts

| ID  | Category |  Conflicts          |   File Paths |   Proposed Resolution |
|-----|----------|---------------------|--------------|-----------------------|
|GL|FORMAT|num=None type=Float|gs://genomics-public-data/1000-genomes/vcf/ALL.chr13.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf|num=None type=Float|
| | | |gs://genomics-public-data/1000-genomes/vcf/ALL.chr17.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf| |
| | | |gs://genomics-public-data/1000-genomes/vcf/ALL.chr21.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf| |
| | | |gs://genomics-public-data/1000-genomes/vcf/ALL.chr8.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf| |
| | | |gs://genomics-public-data/1000-genomes/vcf/ALL.chrX.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf| |
| | |num=3 type=Float|gs://genomics-public-data/1000-genomes/vcf/ALL.wgs.integrated_phase1_v3.20101123.snps_indels_sv.sites.vcf||
|GQ|FORMAT|num=1 type=Float|gs://genomics-public-data/1000-genomes/vcf/ALL.chrY.phase1_samtools_si.20101123.snps.low_coverage.genotypes.vcf|num=1 type=Float|
| | |num=1 type=Integer |gs://genomics-public-data/1000-genomes/vcf/ALL.chrY.phase1_samtools_si.20101123.snps.low_coverage.genotypes.vcf| |

Inferred Headers

| ID  | Category | Proposed Resolution |
|-----|----------|---------------------|
| FT  | FORMAT   | num=1 type=String   |

No Malformed Records Found.
