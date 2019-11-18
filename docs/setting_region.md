# Setting GCP region

## What to consider

Google Cloud Platform services are available in [many
locations](https://cloud.google.com/about/locations/) across the globe.
You can minimize network latency and network transport costs by running your
Dataflow job in the same region as its input bucket, output dataset, and
temporary directory are located. More specifically, in order to run Variant
Transforms most efficiently you should make sure all the following resources
are located in the same region:
* Your source bucket set by  `--input_pattern` flag.
* Your pipeline's temporary location set by `--temp_location` flag.
* Your output BigQuery dataset set by `--output_table` flag.
* Your Dataflow pipeline set by `--region` flag.

## Running jobs in a particular region
Dataflow API [requires](https://beam.apache.org/blog/2019/08/22/beam-2.15.0.html)
setting a [GCP
region](https://cloud.google.com/compute/docs/regions-zones/#available) via
`--region` flag to run. In addition to this requirment you might also
choose to run Variant Transforms in a specific region following your project’s
security and compliance requirements. For example, in order
to restrict your processing job to Europe, update the region as follows:

```bash
# Dataflow API.
COMMAND="/opt/gcp_variant_transforms/bin/vcf_to_bq ...

docker run gcr.io/cloud-lifesciences/gcp-variant-transforms \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --region "${GOOGLE_CLOUD_REGION}" \
  "${COMMAND}"
```

Note that values of `--project` and `--region` flags will be automatically
passed as `COMMAND` args in [`piplines_runner.sh`](docker/pipelines_runner.sh).
Alternatively, you can set your default region using the following command:

```bash
gcloud config set compute/region "europe-west1"
```

In this case you do not need to set the `--region` flag any more. For more
information please refer to this [cloud SDK page](https://cloud.google.com/sdk/gcloud/reference/config/set).

If you are running Variant Transforms from GitHub, you just need to specify
region for Dataflow API as below.

```bash
python -m gcp_variant_transforms.vcf_to_bq ... \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --region "${GOOGLE_CLOUD_REGION}" \
```

## Setting Google Cloud Storage bucket region

You can choose your [GCS bucket's region](https://cloud.google.com/storage/docs/locations)
when you are [creating it](https://cloud.google.com/storage/docs/creating-buckets#storage-create-bucket-console).
When you create a bucket, you [permanently
define](https://cloud.google.com/storage/docs/moving-buckets#storage-create-bucket-console)
its name, its geographic location, and the project it is part of. For an existing bucket, you can check
[its information](https://cloud.google.com/storage/docs/getting-bucket-information) to find out 
about its geographic location.

## Setting BigQuery dataset region 

You can choose the region for the BigQuery dataset at dataset creation time.

![BigQuery dataset region](images/bigquery_dataset_region.png)

