# Handling large inputs

The Variant Transforms pipeline can process hunderds of thousands of files,
millions of samples, and billions of records. Please consider adjusting the
default settings as described below when processing any dataset that is more
than a few gigabytes or contains more than 50,000 files (even if the individual
files are small).

In addition, it is recommended to run the pipeline with
`--optimize_for_large_inputs` to process large datasets more efficiently.

## Numer of workers

The default maximum number of workers is 15. You can adjust this by adding
`--max_num_workers <num workers>` to the pipeline args.
The actual number of workers will be dynamically adjusted based on Dataflow's
autoscaling algorithm. If you prefer to not use the autoscaling algorithm, you
may set `--num_workers <num workers>` to use the exact
number of workers.

### Adjusting quota

To increase the number of workers, you need to adjust the Compute Engine quota.
In particular, you need at least `num_workers + 1` IPs, `num_workers + 1` CPUs,
and `num_workers * 250GB` of disk in the zone you are running the pipeline.
Please see https://cloud.google.com/compute/quotas for details on how to
adjust your quota.

Note: the `+1` in the above figures is to account for the additional VM used as
the "pipeline orchastrator" when using the
[pipelines API with the docker image](../README.md#using-docker).

## Machine type

By default, Dataflow uses the `n1-standard-1` machine, which has 1 vCPU and
3.75GB of RAM. It is recommanded to use a larger machine type for large
datasets. You may change the machine type by adding
`--worker_machine_type <machine type>` to the pipeline args. Please see
https://cloud.google.com/compute/pricing#predefined_machine_types for a list of
available machine types.

Please note that you may need to adjust your quota to accomoddate the additional
number of CPUs.

## Disk size

By default, each worker has 250GB of disk. The aggregate amount of disk space
from all workers must be at least as large as the uncompressed size of all VCF
files being processed. However, to accomoddate for intermediate stages of the
pipeline and also to account for the additional overhead introduced by the
transforms (e.g. the sample name is repeated in every record in the BigQuery
output rather than just being specified once as in the VCF header), you
typically need 3 to 4 times the total size of the raw VCF files. You may change
the disk size of every worker by adding `--disk_size_gb <disk size>` to the
pipeline args.

## Pipeline resource estimator tool

We are planning on adding a "resource estimator" tool to provide recommandations
on the pipeline resource settings based on the input data.
[Issue #67](https://github.com/googlegenomics/gcp-variant-transforms/issues/67)
tracks this feature.

