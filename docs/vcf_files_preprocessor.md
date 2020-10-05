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
  2. Defined num is `A` (i.e. one value for each alternate base), but the
     provided values do not have the same cardinality as the alternate bases.
     Infer the num to be unknown (i.e. `.`).

## Running VCF files to BigQuery Preprocessor

Similar to running the
[VCF to BigQuery pipeline](/README.md/#loading-vcf-files-to-bigquery), the
preprocessor can also be run using docker or directly from the source.

### Using docker

Run the script below and replace the following parameters:

* Dataflow's [required inputs](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#configuring-pipelineoptions-for-execution-on-the-cloud-dataflow-service):
  * `GOOGLE_CLOUD_PROJECT`: This is your project ID where the job should run.
  * `GOOGLE_CLOUD_REGION`: You must choose a geographic region for Cloud Dataflow
  to process your data, for example: `us-west1`. For more information please refer to
  [Setting Regions](docs/setting_region.md).
  * `GOOGLE_CLOUD_LOCATION`: You may choose a geographic location for Cloud Life
  Sciences API to orchestrate job from. This is not where the data will be processed,
  but where some operation metadata will be stored. This can be the same or different from
  the region chosen for Cloud Dataflow. If this is not set, the metadata will be stored in
  us-central1. See the list of [Currently Available Locations](https://cloud.google.com/life-sciences/docs/concepts/locations).
  * `TEMP_LOCATION`: This can be any folder in Google Cloud Storage that your
  project has write access to. It's used to store temporary files and logs
  from the pipeline.
* `INPUT_PATTERN`: A location in Google Cloud Storage where the
  VCF file are stored. You may specify a single file or provide a pattern to
  analyze multiple files at once.
* `REPORT_PATH`: This can be any path in Google Cloud Storage that your project
  has write access to. It is used to store the report from the preprocessor
  tool.
* `RESOLVED_HEADERS_PATH`: Optional. This can be any path in Google Cloud
  Storage that your project has write access to. It is used to store the
  resolved headers which can be further used as a representative header (via
  `--representative_header_file`) for the
  [VCF to BigQuery pipeline](/README.md/#loading-vcf-files-to-bigquery).

`report_all_conflicts` is optional. By default, the preprocessor reports the
inconsistent header definitions across multiple VCF files. By setting this
parameter to true, it also checks the undefined headers and the malformed
records.

```bash
#!/bin/bash
# Parameters to replace:
GOOGLE_CLOUD_PROJECT=GOOGLE_CLOUD_PROJECT
GOOGLE_CLOUD_REGION=GOOGLE_CLOUD_REGION
GOOGLE_CLOUD_LOCATION=GOOGLE_CLOUD_LOCATION
TEMP_LOCATION=gs://BUCKET/temp
INPUT_PATTERN=gs://BUCKET/*.vcf
REPORT_PATH=gs://BUCKET/report.tsv
RESOLVED_HEADERS_PATH=gs://BUCKET/resolved_headers.vcf

COMMAND="vcf_to_bq_preprocess \
  --input_pattern ${INPUT_PATTERN} \
  --report_path ${REPORT_PATH} \
  --job_name vcf-to-bigquery-preprocess \
  --resolved_headers_path ${RESOLVED_HEADERS_PATH} \
  --report_all_conflicts true \
  --runner DataflowRunner"

docker run -v ~/.config:/root/.config \
  gcr.io/cloud-lifesciences/gcp-variant-transforms \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --location "${GOOGLE_CLOUD_LOCATION}" \
  --region "${GOOGLE_CLOUD_REGION}" \
  --temp_location "${TEMP_LOCATION}" \
  "${COMMAND}"
```

### Running from github

In addition to using the docker image, you may run the pipeline directly from
source.

Example command for DirectRunner:

```bash
python3 -m gcp_variant_transforms.vcf_to_bq_preprocess \
  --input_pattern gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf \
  --report_path gs://BUCKET/report.tsv
  --job_name vcf-to-bigquery-preprocess-direct-runner \
  --resolved_headers_path gs://BUCKET/resolved_headers.vcf \
  --report_all_conflicts true \
  --temp_location "${TEMP_LOCATION}"
```

Example command for DataflowRunner:

```bash
python3 -m gcp_variant_transforms.vcf_to_bq_preprocess \
  --input_pattern gs://BUCKET/*.vcf \
  --report_path gs://BUCKET/report.tsv \
  --job_name vcf-to-bigquery-preprocess \
  --resolved_headers_path gs://BUCKET/resolved_headers.vcf \
  --report_all_conflicts true \
  --setup_file ./setup.py \
  --runner DataflowRunner \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --region "${GOOGLE_CLOUD_REGION}" \
  --temp_location "${TEMP_LOCATION}"
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
