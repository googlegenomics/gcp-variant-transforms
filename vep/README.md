# Annotating input files with VEP

This directory includes tools and utilities for running
[Ensembl's Variant Effect Predictor](
https://ensembl.org/info/docs/tools/vep/index.html) (VEP) on input VCF files
of [Variant Transforms](../README.md).

## Overview

With tools provided in this directory, one can:
* Create a docker image of VEP.
* Download and package VEP's database (a.k.a.
[cache](https://.ensembl.org/info/docs/tools/vep/script/vep_cache.html)) for
different species, reference sequences and versions of VEP.
* Run VEP on VCF input files and create output VCF files that are annotated.

Note that, this is a useful standalone tool for running VEP in the cloud but the
main goal is to be able to run VEP as a preprocessor through Variant Transforms
and then import the annotated variants into BigQuery with proper handling of
annotations.

## How to create and push VEP docker images

Inside this directory, run:

`docker build . -t [IMAGE_TAG]`

This will download the source from
[VEP GitHub repo](https://github.com/Ensembl/ensembl-vep) and build VEP from
that source. By default, it uses version 91 of VEP. This can be changed by
`ENSEMBL_RELEASE` build argument, e.g.,

`docker build . -t [IMAGE_TAG] --build-arg ENSEMBL_RELEASE=90`

Let's say we want to push this image to the
[Container Registry](https://cloud.google.com/container-registry/) of `my-project`
on Google Cloud, se we can pick `[IMAGE_TAG]` as `gcr.io/my-project/vep_91`.
Then push this image by:

`gcloud docker -- push gcr.io/my-project/vep_91`

**TODO**: Add `cloudbuild.yaml` files for both easy push and integration test.

## How to download and package VEP databases

Choose a local directory with enough space (e.g., ~20GB for homo_sapiens) to
download and integrate different pieces of the VEP database or cache files.
Then from within that directory run the
[`build_vep_cache.sh`](build_vep_cache.sh) script. By default this script
creates the database for human (homo_sapiens), referenec sequence `GRCh38`,
and release 91 of VEP. These values can be overwritten by the following
environment variables (note you should use the same VEP release
that you used for creating VEP docker image above):

* `VEP_SPECIES`
* `GENOME_ASSEMBLY`
* `ENSEMBL_RELEASE`

## How to run VEP on GCP

There is the helper script [`run_vep.sh`](run_vep.sh) that is added to the VEP
docker image and can be used to run VEP. One way of running it on
Google Cloud Platform (GCP) is through the [Pipelines API](
https://cloud.google.com/genomics/v1alpha2/pipelines-api-command-line). For a
sample `yaml` job description check
[`sample_pipeline.yaml`](sample_pipeline.yaml).
Here is a sample `gcloud` command that uses that file:

```
gcloud alpha genomics pipelines run \
  --project my-project \
  --pipeline-file sample_pipeline.yaml \
  --logging gs://my_bucket/logs \
  --inputs VCF_INFO_FILED=CSQ_RERUN
```

Note the `vep_cache_homo_sapiens_GRCh38_91.tar.gz` file that is referenced in
the sample `yaml` file, is the output file that you get from the above database
creation step.

The [`run_vep.sh`](run_vep.sh) script relies on several environment variables
that can be set to change the default behaviour. In the above example
`VCF_INFO_FILED` is changed to `CSQ_RERUN` (the default is `CSQ_VT`).

This is the full list of supported environment variables:

* `SPECIES`: default is `homo_sapiens`
* `GENOME_ASSEMBLY`: default is `GRCh38`
* `NUM_FORKS`: The value to be set for
[`--fork` option of VEP](
http://.ensembl.org/info/docs/tools/vep/script/vep_options.html#opt_fork).
default is 1.
* `OTHER_VEP_OPTS`: Other options to be set for the VEP invocation, default is
[`--everything`](
http://ensembl.org/info/docs/tools/vep/script/vep_options.html#opt_everything)
* `VCF_INFO_FILED`: The name of the info field to be used for annotations,
default is `CSQ_VT`. See
[`--vcf_info_field`](
http://ensembl.org/info/docs/tools/vep/script/vep_options.html#opt_vcf_info_field)

The following environment variables have to be set and point to valid storage
locations:

* `VEP_CACHE`: Where the tar.gz file, created in the above database creation
step, is located.
* `INPUT_FILE`: Note this can be either a VCF file or a compressed VCF file
(`.gz` or `.bgz`). If it is a compressed file, the `run_vep.sh` script will
decompress it before sending it to VEP.
* `OUTPUT_VCF`: The name of the output file which is always a VCF file.
