# GCP Variant Transforms based on Apache Beam

## Overview

This is a tool for transforming and processing VCF files in a scalable manner
based on Apache Beam. The Python implementation supports running pipelines
using [Dataflow](https://cloud.google.com/dataflow/) on Google Cloud Platform.

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
pip install apache-beam[gcp] pyvcf
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

## Development

### Testing

To run all tests:

```bash
python setup.py test
```

To run a specific test:

```bash
python setup.py test -s <module>.<test class>.<test method>
```
