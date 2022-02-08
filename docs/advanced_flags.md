# Advanced Flags

## Custom Networks

Variant Transforms supports custom networks. This can be used to start the processing VMs in a specific subnetwork of your Google Cloud project as opposed to the default network.

Specify a subnetwork by using the `--subnetwork` flag and provide the name of the subnetwork as follows: `--subnetwork my-subnet`. Just use the name of the subnet, not the full path.

Example:
```bash
COMMAND="/opt/gcp_variant_transforms/bin/vcf_to_bq ...

docker run gcr.io/cloud-lifesciences/gcp-variant-transforms \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --subnetwork my-subnet \
  ...
  "${COMMAND}"
```


## Removing External IPs
Variant Transforms allows disabling the use of external IP addresses with the
`--use_public_ips` flag. If not specified, this defaults to true, so to restrict the
use of external IP addresses, use `--use_public_ips false`. Note that without external
IP addresses, VMs can only send packets to other internal IP addresses. To allow these
VMs to connect to the external IP addresses used by Google APIs and services, you can
[enable Private Google Access](https://cloud.google.com/vpc/docs/configure-private-google-access)
on the subnet.

Example:
```bash
COMMAND="/opt/gcp_variant_transforms/bin/vcf_to_bq ...

docker run gcr.io/cloud-lifesciences/gcp-variant-transforms \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --use_public_ips false \
  ...
  "${COMMAND}"
```

## Custom Dataflow Runner Image
By default Variant Transforms uses a custom docker image to run the pipeline in: `gcr.io/cloud-lifesciences/variant-transforms-custom-runner:latest`.
This image contains all the necessary python/linux dependencies needed to run variant transforms so that they are not downloaded from the internet when the pipeline starts.

You can override which container is used by passing a `--sdk_container_image` as in the following example:

```bash
COMMAND="/opt/gcp_variant_transforms/bin/vcf_to_bq ...

docker run gcr.io/cloud-lifesciences/gcp-variant-transforms \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --sdk_container_image gcr.io/path/to/my/container\
  ...
  "${COMMAND}"
```

## Custom Service Accounts
By default the dataflow workers will use the [default compute service account](https://cloud.google.com/compute/docs/access/service-accounts#default_service_account). You can override which service account to use with the `--service_account` flag as in the following example:

```bash
COMMAND="/opt/gcp_variant_transforms/bin/vcf_to_bq ...

docker run gcr.io/cloud-lifesciences/gcp-variant-transforms \
  --project "${GOOGLE_CLOUD_PROJECT}" \
  --service_account my-cool-dataflow-worker@<PROJECT_ID>.iam.gserviceaccount.com\
  ...
  "${COMMAND}"
```

**Other Service Account Notes:**
- The [Life Sciences Service Account is not changable](https://cloud.google.com/life-sciences/docs/troubleshooting#missing_service_account)
- The [Dataflow Admin Service Account is not changable](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#service_account)