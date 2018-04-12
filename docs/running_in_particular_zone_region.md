# Running jobs in a particular region/zone

For this, you need to specify zone and region in Dataflow API (as well as
Pipelines API if you are using Docker). For example, in order to restrict
job processing to Europe. First update `vcf_to_bigquery.yaml` to use zone and
region in Europe

```yaml
name: vcf-to-bigquery-pipeline
docker:
  imageName: gcr.io/gcp-variant-transforms/gcp-variant-transforms
  cmd: |
    ./opt/gcp_variant_transforms/bin/vcf_to_bq ...\
      --region europe-west1 \
      --zone europe-west1-b

```

Then specify zone and region in Pipelines API.

```bash
gcloud alpha genomics pipelines run ... \
    --regions europe-west1 \
    --zones europe-west1-b
```

If running from github, you just need to specify zone and region for Dataflow
API as below.

```bash
python -m gcp_variant_transforms.vcf_to_bq ... \
  --region europe-west1 \
  --zone europe-west1-b
```
