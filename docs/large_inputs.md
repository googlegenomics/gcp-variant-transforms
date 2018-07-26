# Handling large inputs

The Variant Transforms pipeline can process hunderds of thousands of files,
millions of samples, and billions of records. There are a few settings that
may need to be adjusted depending on the size of the input files. Each of these
settings are explained in the sections below.

Default settings:

```
/opt/gcp_variant_transforms/bin/vcf_to_bq ... \
  --optimize_for_large_inputs <default false> \
  --max_num_workers <default is automatically determined> \
  --worker_machine_type <default n1-standard-1> \
  --disk_size_gb <default 250> \
  --worker_disk_type <default PD> \
  --num_bigquery_write_shards <default 1> \
  --partition_config_path <default None> \
```

### Important notes

#### Running preprocessor/validator tool

Since processing large inputs can take a long time and can be costly, we highly
recommend running the [preprocessor/validator tool](vcf_files_preprocessor.md)
prior to loading the full VCF to BigQuery pipeline to find out about any
invalid/inconsistent records. This can avoid failures due to invalid records
and can save time/cost. Depending on the quality of the input files, you may
consider running with `--report_all_conflicts` to get the full report (it takes
longer, but is more accurate and is highly recommended when you're not sure
about the quality of the input files).

#### Adjusting quota

Compute Engine enforces quota on maximum amount of resources that can be used
at any time for variety of reasons. Please see
https://cloud.google.com/compute/quotas for more details. As a result, you may
need to adjust the quota to satisfy the job requirements.

The main Compute Engine quotas to be adjusted are:
* `In-use IP addresses`: One per worker.
* `CPUs`: At least one per worker. More if larger machine type is used.
* `Persistent Disk Standard (GB)`: At least 250GB per worker. More if larger
  disk is used.
* `Persistent Disk SSD (GB)`: Only needed if `--worker_disk_type` is set to SSD.
  Required quota size is the same as `Persistent Disk Standard`.

Note that the value assigned to these quotas will be the upper limit of
available resources for your job. For example, if the quota for
`In-use IP addresses` is 20, but you try to run with `--max_num_workers 20`,
your job will be running with at most 10 workers because that's all your GCP
project is allowed to use.

### `--optimize_for_large_inputs`

This flag should be set to true when loading more than 50,000 files and/or
[merging](variant_merging.md) is enabled for a large number (>3 billion)
of variants. This flag optimizes the Dataflow pipeline for large inputs, which
can save significant cost/time, but the additional overhead may hurt cost/time
for small inputs.

### `--max_num_workers`

By default, Dataflow uses its autoscaling algorithm to adjust the number of
workers assigned to each job (limited by the Compute Engine quota). You may
adjust the maximum number of workers using `--max_num_workers`. You may also use
`--num_workers` to specify the initial number of workers to assign to the job.

### `--worker_machine_type`

By default, Dataflow uses the `n1-standard-1` machine, which has 1 vCPU and
3.75GB of RAM. You may need to request a larger machine for large datasets.
Please see https://cloud.google.com/compute/pricing#predefined_machine_types
for a list of available machine types.

We have observed that Dataflow performs more efficiently when running
with a large number of small machines rather than a small number of large
machines (e.g. 200 `n1-standard-4` workers instead of 25 `n1-standard-32`
workers). This is due to disk/network IOPS (input/output operations per second)
being limited for each machine especially if [merging](variant_merging.md) is
enabled.

Using a large number of workers may not always be possible due to disk and
IP quotas. As a result, we recommend using SSDs (see
[`--worker_disk_type`](#--worker_disk_type)) when choosing a large (
`n1-standard-16` or larger) machine, which yields higher disk IOPS and can avoid
idle CPU cycles. Note that disk is significantly cheaper than CPU, so always try
to optimize for high CPU utilization rather than disk usage.

### `--disk_size_gb`

By default, each worker has 250GB of disk. The aggregate amount of disk space
from all workers must be at least as large as the uncompressed size of all VCF
files being processed. However, to accomoddate for intermediate stages of the
pipeline and also to account for the additional overhead introduced by the
transforms (e.g. the sample name is repeated in every record in the BigQuery
output rather than just being specified once as in the VCF header), you
typically need 3 to 4 times the total size of the raw VCF files.

In addition, if [merging](variant_merging.md) or
[--num_bigquery_write_shards](#--num_bigquery_write_shards) is enabled, you may
need more disk per worker (e.g. 500GB) as the same variants need to be
aggregated together on one machine.

### `--worker_disk_type`

SSDs provide significantly more IOPS than standard persistent disks, but are
more expensive. However, when choosing a large machine (e.g. `n1-standard-16`),
they can reduce cost as they can avoid idle CPU cycles due to disk IOPS
limitations.

As a result, we recommend using SSDs if [merging](variant_merge.md) or
[--num_bigquery_write_shards](#--num_bigquery_write_shards) is enabled: these
operations require "shuffling" the data (i.e. redistributing the data among
workers), which require significant disk I/O.

Set
`--worker_disk_type compute.googleapis.com/projects//zones//diskTypes/pd-ssd`
to use SSDs.

### `--num_bigquery_write_shards`

Currently, the write operation to BigQuery in Dataflow is performed as a
postprocessing step after the main transforms are done. As a workaround for
BigQuery write limitations (more details
[here](https://github.com/googlegenomics/gcp-variant-transforms/issues/199)),
we have added "sharding" when writing to BigQuery. This makes the data load
to BigQuery significantly faster as it parallelizes the process and enables
loading large (>5TB) data to BigQuery at once.

As a result, we recommend setting `--num_bigquery_write_shards 20` when loading
any data that has more than 1 billion rows (after merging) or 1TB of final
output. You may use a smaller number of write shards (e.g. 5) when using
[partitioned output](#--partition_config_path) as each partition also acts as a
"shard". Note that using a larger value (e.g. 50) can cause BigQuery write to
fail as there is a maximum limit on the number of concurrent writes per table.

### `--partition_config_path`

Partitioning the output can save significant query costs once the data is in
BigQuery. It can also optimize the cost/time of the pipeline (e.g. it natively
shards the BigQuery output per partition and merging can also occur per
partition).

As a result, we recommend setting the partition config for very large data
where possible. Please see the [documentation](partitioning.md) for more
details.

