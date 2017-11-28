# GCP Variant Transforms

## Overview

This is a tool for transforming and processing VCF files in a scalable manner
based on [Apache Beam](https://beam.apache.org/). The Python implementation
supports running pipelines using
[Dataflow](https://cloud.google.com/dataflow/) on Google Cloud Platform.

### Pre-requisites

Install Python, pip, and virtualenv:

```bash
sudo apt-get install python-pip python-dev build-essential
sudo pip install --upgrade pip
sudo pip install --upgrade virtualenv
```

Run virtualenv and install pip packages:

```bash
virtualenv venv
source venv/bin/activate
git clone git@github.com:googlegenomics/gcp-variant-transforms.git
cd gcp-variant-transforms
pip install --upgrade .
```

Setup a [Google Cloud account](https://cloud.google.com/) and
[create a project](https://cloud.google.com/resource-manager/docs/creating-managing-projects),
if you don't have one already.

## Pipelines

### VCF to BigQuery

Use `vcf_to_bq.py` to load
[VCF](https://samtools.github.io/hts-specs/VCFv4.3.pdf) files
directly to [BigQuery](https://cloud.google.com/bigquery/).

You may use the
[DirectRunner](https://beam.apache.org/documentation/runners/direct/)
(aka local runner) for small (e.g. 10,000 records) files or
[DataflowRunner](https://beam.apache.org/documentation/runners/dataflow/)
for larger files. Files should be stored on Google Cloud Storage if using
Dataflow, but may be stored locally for DirectRunner. You may load multiple
files by providing a pattern (e.g. `gs://bucket/path/*.vcf`).
Samples across files may be merged according to a merging strategy (e.g.
through specifying `--variant_merge_strategy=MOVE_TO_CALLS`). Please refer to
the documentation in `vcf_to_bq.py` for additional details on merging.

The pipeline supports gzip, bzip, and uncompressed VCF formats. However,
it runs slower for compressed files as they cannot be sharded.

Note that the pipeline requires valid and complete headers for all VCF files
being processed.

Example command for DirectRunner:

```bash
python -m gcp_variant_transforms.vcf_to_bq \
  --input_pattern gcp_variant_transforms/testing/testdata/valid-4.0.vcf \
  --output_table projectname:bigquerydataset.tablename
```

Example command for DataflowRunner:

```bash
python -m gcp_variant_transforms.vcf_to_bq \
  --input_pattern gs://bucket/vcfs/vcffile.vcf \
  --output_table projectname:bigquerydataset.tablename  \
  --project projectname \
  --staging_location gs://bucket/staging \
  --temp_location gs://bucket/temp \
  --job_name vcf-to-bq \
  --setup_file ./setup.py \
  --runner DataflowRunner
```

#### Running with large inputs

You may adjust the number of workers that Dataflow uses in the pipeline by
adding the `--max_num_workers` argument. To request a specific number of
workers, use the `--num_workers` argument. You may need to request additional
quota for using a large number of workers. Please see
https://cloud.google.com/compute/quotas for details.

If you want to load a large number of files (e.g. more than 50), then you
should specify `--representative_header_file` to point to a file
that has representative INFO and FORMAT header fields among the files
being imported (i.e. a merged view of all INFO and FORMAT fields).
In cases where all files have the same header fields, then this can point
to any individual VCF file in the batch being loaded.

## Docker

A docker image is also available at
`gcr.io/gcp-variant-transforms/gcp-variant-transforms` with pre-built binaries
and dependencies.

The easiest way to use the docker image in Google Compute Engine is to
create an instance based on
[Container-Optimized OS](https://cloud.google.com/container-optimized-os/docs/):

```bash
gcloud beta compute instances create gcp-variant-transforms \
  --project $PROJECT_NAME \
  --scopes bigquery,storage-full,compute-rw,cloud-platform,userinfo-email \
  --machine-type n1-standard-1 \
  --image cos-stable-61-9765-66-0 \
  --image-project cos-cloud \
  --zone us-east1-d
```

Next, SSH into the VM and run the following command to grant credentials
to the docker image:

```bash
docker-credential-gcr configure-docker
```

You can now run the pipeline using the `docker run` command. For instance,
to run the VCF to BigQuery pipeline using Dataflow, run:

```bash
docker run gcr.io/gcp-variant-transforms/gcp-variant-transforms:latest \
  ./opt/gcp_variant_transforms/bin/vcf_to_bq \
  --input_pattern gs://bucket/vcfs/vcffile.vcf \
  --output_table projectname:bigquerydataset.tablename  \
  --project projectname \
  --staging_location gs://bucket/staging \
  --temp_location gs://bucket/temp \
  --job_name vcf-to-bq \
  --runner DataflowRunner
```

## Development
[Development Guide](docs/development_guide.md)
