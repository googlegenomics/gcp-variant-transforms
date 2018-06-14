# Running jobs in a particular VPC & Subnetwork

To run in a particular VPC and subnetwork you have to make 3 changes. Convert the vcf_to_bigquery.yaml file to the v2alpha1 api, pass the network and subnetwork to the vcf_to_bq command, and set the network, and subnetwork on the virtual machine.

See the example vcf_to_bigquery.yaml below:

```yaml
actions:
  - name: vcf-to-bigquery
   # Provide the docker image to run
    imageUri: gcr.io/gcp-variant-transforms/gcp-variant-transforms
    #set the entrypoint
    entrypoint: /opt/gcp_variant_transforms/bin/vcf_to_bq
    #provide the flags and values
    commands:
      - --network 
      - < network name >
      - --subnetwork
      - /regions/<region>/subnetworks/<subnet name>
resources:
  projectid: <project id>
  #define the zones the pipeline instances will run in 
  zones: 
    - <zone>
  virtualMachine:
    machineType: n1-standard-1
    network: <network name>
    subnetwork: <subnetwork name>
```


Then call the pipelines api

```bash
gcloud alpha genomics pipelines run ... 
```

If running from github, you just need to specify network and subnetwork for Dataflow
API as below.

```bash
python -m gcp_variant_transforms.vcf_to_bq ... \
  --network < network name > \
  --subnetwork /regions/<region>/subnetworks/<subnet name>
```

If you are using automatic subnetworks you don't need to provide the subnetwork name.
