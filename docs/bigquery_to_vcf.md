# BigQuery to VCF

The BigQuery to VCF pipeline is used for loading BigQuery table to one VCF. This
pipeline reads the variants from BigQuery table, groups a collection of variants
within a contiguous region of the genome, sorts them, and then writes to one VCF
shard. At the end, it consolidates VCF header and VCF shards into one.

## Running BQ to VCF

Similar to running the
[VCF to BigQuery pipeline](/README.md/#loading-vcf-files-to-bigquery), the
BigQuery to VCF pipeline can also be run using docker or directly from the
source. 

### Using docker

Run the script below and replace the following parameters:

* `GOOGLE_CLOUD_PROJECT`: This is your Google Cloud project ID where the job
  should run.
* `INPUT_TABLE`: BigQuery table that will be loaded to VCF. It must be in the
  format of GOOGLE_CLOUD_PROJECT:DATASET.TABLE.
* `OUTPUT_FILE`: The full path of the VCF file to store the result. If run
  with Dataflow, a path in Google Cloud Storage that your project has write
  access to is required.
* `TEMP_LOCATION`: This can be any folder in Google Cloud Storage that your
  project has write access to. It's used to store temporary files and logs
  from the pipeline.

```bash
#!/bin/bash
# Parameters to replace:
GOOGLE_CLOUD_PROJECT=GOOGLE_CLOUD_PROJECT
INPUT_TABLE=GOOGLE_CLOUD_PROJECT:DATASET.TABLE
OUTPUT_FILE=gs://BUCKET/loaded_file.vcf
TEMP_LOCATION=gs://BUCKET/temp

COMMAND="/opt/gcp_variant_transforms/bin/bq_to_vcf \
  --project ${GOOGLE_CLOUD_PROJECT} \
  --input_table ${INPUT_TABLE} \
  --output_file ${OUTPUT_FILE} \
  --temp_location ${TEMP_LOCATION} \
  --job_name bq-to-vcf \
  --runner DataflowRunner"
gcloud alpha genomics pipelines run \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --logging "${TEMP_LOCATION}/runner_logs_$(date +%Y%m%d_%H%M%S).log" \
  --zones us-west1-b \
  --service-account-scopes https://www.googleapis.com/auth/cloud-platform \
  --docker-image gcr.io/gcp-variant-transforms/gcp-variant-transforms \
  --command-line "${COMMAND}"
```

Besides, the following optional flags are provided:
* `--representative_header_file`: If provided, meta-information from the
  provided file will be added into the `output_file`. Otherwise, the
  meta-information is inferred from the BigQuery schema. To best recover the
  VCF, it is recommended to provide this file.
* `--number_of_bases_per_shard`:  The maximum number of base pairs per
  chromosome to include in a single VCF file (one shard). A shard is a
  collection of data within a contiguous region of the genome. By default,
  number_of_bases_per_shard = 1,000,000. With the default setting, the variants
  on the same chromosome with start position 1 to 999,999 are written to the
  same shard, and the variants on the same chromosome with start position
  1,000,000 to 1,999,999 are written to another shard, etc. This parameter will
  have an impact on memory requirements since the data in a single shard must be
  sorted.
* `--genomic_regions`: A list of genomic regions (separated by a space) to load
  from BigQuery. The format of each genomic region should be
  CHROMOSOME:START_POSITION-END_POSITION or CHROMOSOME if the full chromosome is
  requested. Only variants matching at least one of these regions will be
  loaded. If this parameter is not specified, all variants will be kept.
* `--call_names`: A list of call names (separated by a space). Only variants for
  these calls will be loaded from BigQuery. If this parameter is not specified,
  all calls will be kept.
* `--allow_incompatible_schema`: If `representative_header_file` is not
  provided, the meta-information is inferred from the BigQuery schema. There are
  some reserved fields based on
  [VCF 4.3 spec](http://samtools.github.io/hts-specs/VCFv4.3.pdf). If the
  inferred definition is not the same as the reserved definition, an error will
  raise and the pipeline fails. By setting this para to be true, the
  incompatibilities between BigQuery schema and the reserved fields will not
  raise errors. Instead, the VCF meta information are inferred by the schema
  without validation.

### Running from github

In addition to using the docker image, you may run the pipeline directly from
source.

Example command for DirectRunner:

```bash
python -m gcp_variant_transforms.bq_to_vcf \
  --input_table bigquery-public-data:human_genome_variants.1000_genomes_phase_3_variants_20150220 \
  --output_file gs://BUCKET/loaded_file.vcf \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --genomic_regions 1:124852-124853 \
  --call_names HG00099 HG00105
```

Example command for DataflowRunner:

```bash
python -m gcp_variant_transforms.bq_to_vcf \
  --input_table bigquery-public-data:human_genome_variants.1000_genomes_phase_3_variants_20150220 \
  --output_file gs://BUCKET/loaded_file.vcf \
  --representative_header_file gs://BUCKET/representative_header_file.vcf \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --temp_location gs://BUCKET/temp \
  --genomic_regions 1:124852-124853 \
  --call_names HG00099 HG00105 \
  --job_name bq-to-vcf \
  --setup_file ./setup.py \
  --runner DataflowRunner
```
