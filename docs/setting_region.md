# Setting GCP region

## Running jobs in a particular region

For this, you need to specify region in Dataflow API (as well as
the Pipelines API if you are using Docker). For example, in order to restrict
job processing to Europe, update the region as follows:

```bash
# Dataflow API.
COMMAND="/opt/gcp_variant_transforms/bin/vcf_to_bq ... \
  --region europe-west1"

# Pipelines API.
gcloud alpha genomics pipelines run ... \
    --region europe-west1
```

If running from GitHub, you just need to specify region for Dataflow
API as below.

```bash
python -m gcp_variant_transforms.vcf_to_bq ... \
  --region europe-west1
```

Alternatively, you can set your default region using the following command.

```bash
gcloud config set compute/region "europe-west1"
```
In this case you do not need to set the `--region` flag any more. For more
information please refer to this [cloud SDK page](https://cloud.google.com/sdk/gcloud/reference/config/set).

## Setting BigQuery dataset region 

You can choose the region for the BigQuery dataset at dataset creation time.

![BigQuery dataset region](images/bigquery_dataset_region.png)

