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
Transforms and BigQuery. Please also read the
[blog post](https://cloud.google.com/blog/big-data/2018/03/how-color-uses-the-new-variant-transforms-tool-for-breakthrough-clinical-data-science-with-bigquery)
about how a GCP customer used Variant Transforms for breakthrough clinical
data science with BigQuery.

### Prerequisites

1.  Setup a [Google Cloud account](https://cloud.google.com/) and
    [create a project](https://cloud.google.com/resource-manager/docs/creating-managing-projects).
1.  [Sign up and install the Google Cloud SDK](https://cloud.google.com/genomics/install-genomics-tools)
1.  Enable the [Genomics, Compute Engine, Cloud Storage, and Dataflow APIs](https://console.cloud.google.com/flows/enableapi?apiid=genomics,storage_component,storage_api,compute_component,dataflow)
1.  Open the [billing](https://console.cloud.google.com/project/_/settings) page
    for the project you have selected or created, and click _Enable billing_.
1.  Create a new BigQuery dataset by visiting the
    [BigQuery web UI](https://bigquery.cloud.google.com/), clicking on the
    down arrow icon next to your project name in the navigation, and clicking on
    _Create new dataset_.

## Loading VCF files to BigQuery  <a name="vcf_to_bq"></a>

### Using docker

The easiest way to run the VCF to BigQuery pipeline is to use the
[docker](https://www.docker.com/) image and run it with the
[Google Genomics Pipelines API](https://cloud-dot-devsite.googleplex.com/genomics/pipelines)
as it has the binaries and all dependencies pre-installed.

First, set up the pipeline configurations shown below and save it as
`vcf_to_bigquery.yaml`. The parameters that you need to replace are:

* `my_project`: This is your project name that contains the BigQuery dataset.
* `gs://my_bucket/vcffiles/*.vcf`: A location in Google Cloud Storage where the
  VCF file are stored. You may specify a single file or provide a pattern to
  load multiple files at once. Please refer to the
  [Variant Merging](docs/variant_merging.md) documentation if you want
  to merge samples across files. The pipeline supports gzip, bzip, and
  uncompressed VCF formats. However, it runs slower for compressed files as they
  cannot be sharded.
* `my_bigquery_dataset`: Your BigQuery dataset to store the output.
* `my_bigquery_table`: This can be any ID you like (e.g. vcf_test).
* `gs://my_bucket/staging` and `gs://my_bucket/temp`: These can be any folder in
  Google Cloud Storage that your project has write access to. These are used to
  store temporary files needed for running the pipeline.

```yaml
name: vcf-to-bigquery-pipeline
docker:
  imageName: gcr.io/gcp-variant-transforms/gcp-variant-transforms
  cmd: |
    ./opt/gcp_variant_transforms/bin/vcf_to_bq \
      --project my_project \
      --input_pattern gs://my_bucket/vcffiles/*.vcf \
      --output_table my_project:my_bigquery_dataset.my_bigquery_table \
      --staging_location gs://my_bucket/staging \
      --temp_location gs://my_bucket/temp \
      --job_name vcf-to-bigquery \
      --runner DataflowRunner
```

Next, run the following command to launch the pipeline. Replace `my_project`
with your project name, `gs://my_bucket/temp/runner_logs` with a Cloud Storage
folder to store the logs from the pipeline.

```bash
gcloud alpha genomics pipelines run \
    --project my_project \
    --pipeline-file vcf_to_bigquery.yaml \
    --logging gs://my_bucket/temp/runner_logs \
    --zones us-west1-b \
    --service-account-scopes https://www.googleapis.com/auth/bigquery
```

Please note the operation ID returned by the above script. You can track the
status of your operation by running:

```bash
gcloud alpha genomics operations describe <operation-id>
```

The returned data will have `done: true` when the operation is done.
A detailed description of the Operation resource can be found in the
[API documentation](https://cloud.google.com/genomics/reference/rest/v1/operations)

The underlying pipeline uses
[Cloud Dataflow](https://cloud.google.com/dataflow/). You can navigate to the
[Dataflow Console](https://console.cloud.google.com/dataflow), to see more
detailed view of the pipeline (e.g. number of records being processed, number of
workers, more detailed error logs).

### Running from github

In addition to using the docker image, you may run the pipeline directly from
source. First install git, python, pip, and virtualenv:

```bash
sudo apt-get install -y git python-pip python-dev build-essential
sudo pip install --upgrade pip
sudo pip install --upgrade virtualenv
```

Run virtualenv, clone the repo, and install pip packages:

```bash
virtualenv venv
source venv/bin/activate
git clone https://github.com/googlegenomics/gcp-variant-transforms.git
cd gcp-variant-transforms
pip install --upgrade .
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
  --output_table projectname:bigquerydataset.tablename
```

Example command for DataflowRunner:

```bash
python -m gcp_variant_transforms.vcf_to_bq \
  --input_pattern gs://my_bucket/vcffiles/*.vcf \
  --output_table my_project:my_bigquery_dataset.my_bigquery_table \
  --project my_project \
  --staging_location gs://my_bucket/staging \
  --temp_location gs://my_bucket/temp \
  --job_name vcf-to-bigquery \
  --setup_file ./setup.py \
  --runner DataflowRunner
```


## Running VCF files preprocessor

The VCF files preprocessor is used for validating the datasets such that the
inconsistencies can be easily identified. It can be used as an individual
validator to check the validity of the information inside the VCF files, or as a
helper tool for [VCF to BigQuery pipeline](#vcf_to_bq). Please refer to
[VCF files preprocessor](docs/vcf_files_preprocessor.md) for more details.


## Running jobs in a particular region/zone

You may need to constrain Cloud Dataflow job processing to a specific geographic
region in support of your project’s security and compliance needs. See
[Running in particular zone/region doc](docs/running_in_particular_zone_region.md).


## Additional topics

* [Understanding the BigQuery Variants Table Schema](docs/bigquery_schema.md)
* [Loading multiple files](docs/multiple_files.md)
* [Variant merging](docs/variant_merging.md)
* [Handling large inputs](docs/large_inputs.md)
* [Appending data to existing tables](docs/data_append.md)
* [Troubleshooting](docs/troubleshooting.md)

## Development

* [Development Guide](docs/development_guide.md)
* [Release process](docs/release.md)
 
