# GCP Variant Transforms

[![Build Status](https://travis-ci.org/googlegenomics/gcp-variant-transforms.svg?branch=master)](https://travis-ci.org/googlegenomics/gcp-variant-transforms)
[![Coverage
Status](https://coveralls.io/repos/github/googlegenomics/gcp-variant-transforms/badge.svg)](https://coveralls.io/github/googlegenomics/gcp-variant-transforms)

## Overview

This is a tool for transforming and processing
[VCF](https://samtools.github.io/hts-specs/VCFv4.3.pdf) files in a scalable
manner based on [Apache Beam](https://beam.apache.org/) using 
[Dataflow](https://cloud.google.com/dataflow/) on Google Cloud Platform.

It can be used to directly load VCF files to
[BigQuery](https://cloud.google.com/bigquery/) supporting hundreds of thousands
of files, millions of samples, and billions of records. Additionally, it
provides a preprocess functionality to validate the VCF files such that the
inconsistencies can be easily identified.

Please see
[this presentation](https://docs.google.com/presentation/d/1mIjtfAPlojEBa30fZAcene7GRPr9LYo3GRgtQIQMbRY)
for a high level overview of BigQuery and how to effectively use Variant
Transforms and BigQuery. Read the blog post
[How Color uses the new Variant Transforms tool for breakthrough clinical data science with BigQuery](https://cloud.google.com/blog/big-data/2018/03/how-color-uses-the-new-variant-transforms-tool-for-breakthrough-clinical-data-science-with-bigquery).
Also see the blog post
[Accelerating Mayo Clinic’s data platform with BigQuery and Variant Transforms](https://cloud.google.com/blog/products/data-analytics/genome-data-analytics-with-google-cloud).
The [gnomAD sample query notebook](sample_queries) has additional queries
you can run to practice querying variant data in BigQuery.

### Prerequisites

1.  Follow the [getting started](https://cloud.google.com/genomics/docs/how-tos/getting-started)
    instructions on the Google Cloud page.
1.  Enable the [Genomics, Compute Engine, Cloud Storage, and Dataflow APIs](https://console.cloud.google.com/flows/enableapi?apiid=genomics,storage_component,storage_api,compute_component,dataflow)
1.  Create a new BigQuery dataset by visiting the
    [BigQuery web UI](https://bigquery.cloud.google.com/), clicking on the
    down arrow icon next to your project name in the navigation, and clicking on
    _Create new dataset_.

## Loading VCF files to BigQuery

### Using docker

The easiest way to run the VCF to BigQuery pipeline is to use the
[docker](https://www.docker.com/) image, as it has the binaries and all
dependencies pre-installed. Please ensure you have the latest `gcloud` tool by
running `gcloud components update` (more details [here](https://cloud.google.com/sdk/gcloud/reference/components/update)).

Use the following command to get the latest version of Variant Transforms.
```bash
docker pull gcr.io/cloud-lifesciences/gcp-variant-transforms
```

Run the script below and replace the following parameters:

* Dataflow's [required inputs](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#configuring-pipelineoptions-for-execution-on-the-cloud-dataflow-service):
  * `GOOGLE_CLOUD_PROJECT`: This is your project ID that contains the BigQuery
  dataset.
  * `GOOGLE_CLOUD_REGION`: You must choose a geographic region for Cloud Dataflow
  to process your data, for example: `us-west1`. For more information please refer to
  [Setting Regions](docs/setting_region.md).
  * `TEMP_LOCATION`: This can be any folder in Google Cloud Storage that your
  project has write access to. It's used to store temporary files and logs
  from the pipeline.
* `INPUT_PATTERN`: A location in Google Cloud Storage where the
  VCF file are stored. You may specify a single file or provide a pattern to
  load multiple files at once. Please refer to the
  [Variant Merging](docs/variant_merging.md) documentation if you want
  to merge samples across files. The pipeline supports gzip, bzip, and
  uncompressed VCF formats. However, it runs slower for compressed files as they
  cannot be sharded.
* `OUTPUT_TABLE`: The full path to a BigQuery table to store the output.

```bash
#!/bin/bash
# Parameters to replace:
GOOGLE_CLOUD_PROJECT=GOOGLE_CLOUD_PROJECT
GOOGLE_CLOUD_REGION=GOOGLE_CLOUD_REGION
TEMP_LOCATION=gs://BUCKET/temp
INPUT_PATTERN=gs://BUCKET/*.vcf
OUTPUT_TABLE=GOOGLE_CLOUD_PROJECT:BIGQUERY_DATASET.BIGQUERY_TABLE

COMMAND="vcf_to_bq \
  --input_pattern ${INPUT_PATTERN} \
  --output_table ${OUTPUT_TABLE} \
  --job_name vcf-to-bigquery \
  --runner DataflowRunner"

docker run -v ~/.config:/root/.config \
  gcr.io/cloud-lifesciences/gcp-variant-transforms \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --region "${GOOGLE_CLOUD_REGION}" \
  --temp_location "${TEMP_LOCATION}" \
  "${COMMAND}"
```
`--project`, `--region`, and `--temp_location` are required inputs. You must set all of them, unless your project and region default values
are set in your local `gcloud` configuration. You may set the default project
and region using the following commands:
```bash
gcloud config set project GOOGLE_CLOUD_PROJECT
gcloud config set compute/region REGION
```

The underlying pipeline uses
[Cloud Dataflow](https://cloud.google.com/dataflow/). You can navigate to the
[Dataflow Console](https://console.cloud.google.com/dataflow), to see more
detailed view of the pipeline (e.g. number of records being processed, number of
workers, more detailed error logs).

### Running from github

In addition to using the docker image, you may run the pipeline directly from
source. First install git, python, pip, and virtualenv:

```bash
sudo apt-get install -y git python3-pip python3-venv python3.7-venv python-dev build-essential
```

Note that python 3.8 is not yet supported, so ensure you are using Python 3.7.

Run virtualenv, clone the repo, and install pip packages:

```bash
python3 -m venv venv3
source venv3/bin/activate
git clone https://github.com/googlegenomics/gcp-variant-transforms.git
cd gcp-variant-transforms
python -m pip install --upgrade pip
python -m pip install --upgrade wheel
python -m pip install --upgrade .
```

You may use the
[DirectRunner](https://beam.apache.org/documentation/runners/direct/)
(aka local runner) for small (e.g. 10,000 records) files or
[DataflowRunner](https://beam.apache.org/documentation/runners/dataflow/)
for larger files. Files should be stored on Google Cloud Storage if using
Dataflow, but may be stored locally for DirectRunner.

Example command for DirectRunner:

```bash
python -m gcp_variant_transforms.vcf_to_bq \
  --input_pattern gcp_variant_transforms/testing/data/vcf/valid-4.0.vcf \
  --output_table GOOGLE_CLOUD_PROJECT:BIGQUERY_DATASET.BIGQUERY_TABLE \
  --job_name vcf-to-bigquery-direct-runner \
  --temp_location "${TEMP_LOCATION}"
```

Example command for DataflowRunner:

```bash
python -m gcp_variant_transforms.vcf_to_bq \
  --input_pattern gs://BUCKET/*.vcf \
  --output_table GOOGLE_CLOUD_PROJECT:BIGQUERY_DATASET.BIGQUERY_TABLE \
  --job_name vcf-to-bigquery \
  --setup_file ./setup.py \
  --runner DataflowRunner \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --region "${GOOGLE_CLOUD_REGION}" \
  --temp_location "${TEMP_LOCATION}"
```

## Running VCF files preprocessor

The VCF files preprocessor is used for validating the datasets such that the
inconsistencies can be easily identified. It can be used as a standalone
validator to check the validity of the VCF files, or as a helper tool for
[VCF to BigQuery pipeline](#loading-vcf-files-to-bigquery). Please refer to
[VCF files preprocessor](docs/vcf_files_preprocessor.md) for more details.


## Running BigQuery to VCF

The BigQuery to VCF pipeline is used to export variants in BigQuery to one VCF file.
Please refer to [BigQuery to VCF pipeline](docs/bigquery_to_vcf.md) for more
details.


## Additional topics

* [Understanding the BigQuery Variants Table
  Schema](https://cloud.google.com/life-sciences/docs/how-tos/bigquery-variants-schema)
* [Loading multiple files](docs/multiple_files.md)
* [Variant merging](docs/variant_merging.md)
* [Handling large inputs](docs/large_inputs.md)
* [Appending data to existing tables](docs/data_append.md)
* [Variant Annotation](docs/variant_annotation.md)
* [Sharding](docs/sharding.md)
* [Flattening the BigQuery table](docs/flattening_table.md)
* [Troubleshooting](docs/troubleshooting.md)

## Development

* [Development Guide](docs/development_guide.md)
* [Release process](docs/release.md)
 
