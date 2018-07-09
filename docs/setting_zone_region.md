# Setting zone/region

## Running jobs in a particular zone/region

For this, you need to specify zone and region in Dataflow API (as well as
the Pipelines API if you are using Docker). For example, in order to restrict
job processing to Europe, update the zone and region as follows:

```bash
# Dataflow API.
COMMAND="/opt/gcp_variant_transforms/bin/vcf_to_bq ... \
  --region europe-west1 \
  --zone europe-west1-b"

# Pipelines API.
gcloud alpha genomics pipelines run ... \
    --zones europe-west1-b
```

If running from GitHub, you just need to specify zone and region for Dataflow
API as below.

```bash
python -m gcp_variant_transforms.vcf_to_bq ... \
  --region europe-west1 \
  --zone europe-west1-b
```

## Setting BigQuery dataset region 

You can choose the region for the BigQuery dataset at dataset creation time.

![BigQuery dataset region](images/bigquery_dataset_region.png)

