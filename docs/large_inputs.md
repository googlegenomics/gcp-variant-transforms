# Handling large inputs

The Variant Transforms pipeline can process hundreds of thousands of files,
millions of samples, and billions of records. There are a few settings that 
may need to be adjusted depending on the size of the input files. Each of 
these settings are explained in the sections below.

Example usage:

```
/opt/gcp_variant_transforms/bin/vcf_to_bq ... \
  --max_num_workers <default is automatically determined> \
  --worker_machine_type <default n1-standard-1> \
  --disk_size_gb <default 250> \
  --worker_disk_type <default PD> \
  --keep_intermediate_avro_files \
  --sharding_config_path <default gcp_variant_transforms/data/
      sharding_configs/homo_sapiens_default.yaml> \
```

#### `--max_num_workers`

By default, Dataflow uses its autoscaling algorithm to adjust the number of 
workers assigned to each job (limited by your Compute Engine quota).
You may adjust the maximum number of workers using `--max_num_workers`.
You may also use `--num_workers` to specify the initial number of workers 
to assign to the job.

#### `--worker_machine_type`

By default, Dataflow uses the `n1-standard-1` machine, which has 1 vCPU 
and 3.75GB of RAM. You may need to request a larger machine for large 
datasets. Please see [this page](https://cloud.google.com/compute/pricing#predefined_machine_types)
for a list of available machine types.

We have observed that Dataflow performs more efficiently when running
with a large number of small machines rather than a small number of
large machines (e.g. 200 `n1-standard-4` workers instead of 25 
`n1-standard-32` workers). This is due to disk/network IOPS
(input/output operations per second) being limited for each machine,
especially if [merging](variant_merging.md) is enabled.

Using a large number of workers may not always be possible due to disk 
and IP quotas. As a result, we recommend using SSDs
(see [`--worker_disk_type`](#--worker_disk_type))
when choosing a large (`n1-standard-16` or larger) machine,
which yields higher disk IOPS and can avoid idle CPU cycles.
Note that disk is significantly cheaper than CPU, so always try to optimize
for high CPU utilization rather than disk usage.

#### `--disk_size_gb`

By default, each worker has 250GB of disk. The aggregate amount of
disk space from all workers must be at least as large as the uncompressed
size of all VCF files being processed. However, to accommodate for
intermediate stages of the pipeline and also to account for the additional
overhead introduced by the transforms (e.g. the sample name is repeated
in every record in the BigQuery output rather than just being specified
once as in the VCF header).

In addition, if [merging](variant_merging.md) is enabled, you may 
need more disk space per worker (e.g. 500GB), as the same variants need
to be aggregated together on one machine.

#### `--worker_disk_type`
SSDs provide significantly more IOPS than standard persistent disks,
but are more expensive. However, when choosing a large machine 
(e.g. `n1-standard-16`), they can reduce cost as they can avoid idle
CPU cycles due to disk IOPS limitations.

As a result, we recommend using SSDs if [merging](variant_merge.md) is enabled: these
operations require "shuffling" the data (i.e. redistributing the data
among workers), which require significant disk I/O. Add the following flag to use SSDs:

```
--worker_disk_type compute.googleapis.com/projects//zones//diskTypes/pd-ssd
```

### Adjusting Quotas and Limits 

Compute Engine enforces quota on the maximum amount of resources that can
be used at any time, please see
[this page](https://cloud.google.com/compute/quotas)
for more details. As a result, you may need to adjust your quota to satisfy
the job requirements. The flags mentioned above will not be effective if you
do not have enough quota. In other words, Dataflow autoscaling will not be
able to raise the number of workers to reach the target number if you don't
have enough quota for one of the required resources. One way to confirm this
is to check the *current usage* of your quotas. The following image shows a
situation where `Persistent Disk SSD` in `us-central1` region has reached its
maximum value:

![quotas](images/console_quotas.png)

To resolve situations like this, increase the following Compute Engine quotas:

* `In-use IP addresses`: One per worker. If you set `--use_public_ips false`
then Dataflow workers use private IP addresses for all communication. 
* `CPUs`: At least one per worker. More if a larger machine type is used.
* `Persistent Disk Standard (GB)`: At least 250GB per worker. More if a larger
  disk is used.
* `Persistent Disk SSD (GB)`: Only needed if `--worker_disk_type` is set to SSD.
  Required quota size is the same as `Persistent Disk Standard`.

For more information please refer to
[Dataflow quotas guidelines](https://cloud.google.com/dataflow/quotas#compute-engine-quotas). 
Values assigned to these quotas are the upper limit of available resources for
your job. For example, if the quota for `In-use IP addresses` is 10, but you try
to run with `--max_num_workers 20`, your job will be running with at most 10
workers because that's all your GCP project is allowed to use.

Please note you need to set quotas for the region that your Dataflow pipeline
is running. For more information related to regions please refer to our 
[region documentation](setting_region.md).   

## Other options to consider 

### Running preprocessor/validator tool

Because processing large inputs can take a long time and can be costly,
we highly recommend running the
[preprocessor/validator tool](vcf_files_preprocessor.md)
prior to loading the full VCF to BigQuery pipeline to check for any
invalid/inconsistent records. Doing so can avoid failures due to invalid
records and can save you time and money. Depending on the quality of the
input files, you may consider running with `--report_all_conflicts` to
get the full report. Running with this flag will take longer, but it is
more accurate and is highly recommended when you're not sure about the
quality of the input files.

### Sharding

Sharding the output significantly reduces the query costs once the data
is queried in BigQuery. It also optimizes the cost and time of the pipeline.
As a result, we enforce sharding for all runs of Variant Transforms,
please see the [documentation](sharding.md) for more details.


For very large inputs, you can use `--sharding_config_path` to only
process and import a small region of genomes into BigQuery. For example,
the following sharding config file produces an output table that only
contains variants of chromosome 1 in the range of `[1000000, 2000000]`:

```
-  output_table:
     table_name_suffix: "chr1_1M_2M"
     regions:
       - "chr1:1,000,000-2,000,000"
       - "1"
     partition_range_end: 2,000,000
```

### Saving AVRO files

If you are processing large inputs, you can set the
`--keep_intermediate_avro_files` as a safety measure to ensure that the
result of your Dataflow pipeline is stored in Google Cloud Storage in
case something goes wrong while the AVRO files are copied into BigQuery.
Doing so will not increase your compute cost, because most of the cost
of running Variant Transforms is due to resources used in the Dataflow
pipeline, and loading AVRO files to BigQuery is free. Storing the
intermediate AVRO files avoids wasting the output of Dataflow. For more
information about this flag, please refer to the 
[importing VCF files](vcf_to_bigquery.md) docs.

The downside of this approach is the extra cost of storing AVRO files in a
Google Cloud Storage bucket. To avoid this cost, we recommend deleting
the AVRO files after they have been loaded into BigQuery. If your import
job failed and you need help with loading AVRO files into BigQuery,
please let us know by
[submitting an issue](https://github.com/googlegenomics/gcp-variant-transforms/issues).

