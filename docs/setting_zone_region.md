# Setting zone/region

## Running jobs in a particular zone/region

For this, you need to specify zone and region in Dataflow API (as well as
the Pipelines API if you are using Docker). For example, in order to restrict
job processing to Europe, first update `vcf_to_bigquery.yaml` to use zone and
region in Europe:

```yaml
name: vcf-to-bigquery-pipeline
docker:
  imageName: gcr.io/gcp-variant-transforms/gcp-variant-transforms
  cmd: |
    ./opt/gcp_variant_transforms/bin/vcf_to_bq ...\
      --region europe-west1 \
      --zone europe-west1-b

```

Then specify zone in the Pipelines API:

```bash
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

