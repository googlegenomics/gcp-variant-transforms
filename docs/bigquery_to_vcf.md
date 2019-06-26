# BigQuery to VCF

The BigQuery to VCF pipeline is used for loading a BigQuery table to one VCF
file. You may also customize the pipeline to load a subset of samples and/or
genomic regions.

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
* `OUTPUT_FILE`: The full path of the output VCF file. This can be a local path
  if you use `DirectRunner` (for very small VCF files) but must be a path in
  Google Cloud Storage if using `DataflowRunner`.
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

COMMAND="bq_to_vcf \
  --input_table ${INPUT_TABLE} \
  --output_file ${OUTPUT_FILE} \
  --temp_location ${TEMP_LOCATION} \
  --job_name bq-to-vcf \
  --runner DataflowRunner"

docker run -v ~/.config:/root/.config \
  gcr.io/cloud-lifesciences/gcp-variant-transforms \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --zones us-west1-b \
  "${COMMAND}"
```

In addition, the following optional flags can be specified in the `COMMAND`
argument in the above script:
* `--representative_header_file`: If provided, meta-information from the
  provided file (e.g., INFO, FORMAT, FILTER, etc) will be added into the
  `output_file`. Otherwise, the meta-information is inferred from the BigQuery
  schema on a "best effort" basis (e.g., any repeated INFO field will have
  `Number=.`). It is recommended to provide this file to specify the most
  accurate and complete meta-information in the VCF file.
* `--genomic_regions`: A list of genomic regions (separated by a space) to load
  from BigQuery. The format of each genomic region should be
  REFERENCE_NAME:START_POSITION-END_POSITION or REFERENCE_NAME if the full
  chromosome is requested. Only variants matching at least one of these regions
  will be loaded. For example, `--genomic_regions chr1 chr2:1000-2000` will load
  all variants in `chr1` and all variants in `chr2` with `start_position` in
  `[1000,2000)` from BigQuery. If this flag is not specified, all variants will
  be loaded.
* `--call_names`: A list of call names (separated by a space). Only variants for
  these calls will be loaded from BigQuery. If this parameter is not specified,
  all calls will be loaded.
* `--allow_incompatible_schema`: If `representative_header_file` is not
  provided, the meta-information is inferred from the BigQuery schema. There are
  some reserved fields based on
  [VCF 4.3 spec](http://samtools.github.io/hts-specs/VCFv4.3.pdf). If the
  inferred definition is not the same as the reserved definition, an error will
  be raised and the pipeline will fail. By setting this flag to true, the
  incompatibilities between BigQuery schema and the reserved fields will not
  raise errors. Instead, the VCF meta-information are inferred from the schema
  without validation.
* `--preserve_call_names_order`: By default, call names in the output VCF file
  are generated in ascending order. If set to true, the order of call names will
  be the same as the BigQuery table, but it requires all extracted variants to
  have the same call name ordering (usually true for tables from single VCF file
  import).
* `--number_of_bases_per_shard`: The maximum number of base pairs per
  chromosome to include in a shard. A shard is a collection of data within a
  contiguous region of the genome that can be efficiently sorted in memory.
  This flag is set to 1,000,000 by default, which should work for most datasets.
  You may change this flag if you have a dataset that is very dense and variants
  in each shard cannot be sorted in memory.

The pipeline can be optimized depending on the size of the output VCF file:
* For small VCF files (e.g. a few megabytes), you may use
  `--runner DirectRunner` to speed up the pipeline as it avoids creating a
  Dataflow pipeline.
* For large VCF files (e.g. >100GB), using
  [Cloud Dataflow Shuffle](https://cloud.google.com/dataflow/service/dataflow-service-desc#cloud-dataflow-shuffle)
  can speed up the pipeline, by adding the parameter
  `--experiments shuffle_mode=service` to the `COMMAND` argument.
* For medium sized VCF files, you can use the above script as is.

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
