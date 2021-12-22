#!/bin/bash
set -euo pipefail

# Copyright 2019 Google Inc.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#################################################
# Parses arguments and does some sanity checking.
# Arguments:
#   It is expected that this is called with $@ of the main script.
#################################################
function parse_args {
  # getopt command is only for checking arguments.
  getopt -o '' -l project:,temp_location:,docker_image:,custom_runner_image:,region:,subnetwork:,use_public_ips:,service_account:,location: -- "$@"
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --project)
        google_cloud_project="$2"
        ;;

      --region)
        region="$2"
        ;;

      --location)
        location="$2"
        ;;

      --temp_location)
        temp_location="$2"
        ;;

      --docker_image)
        vt_docker_image="$2"
        ;;

      --custom_runner_image)
        custom_runner_image="$2"
        ;;

      --subnetwork)
        subnetwork="$2"
        ;;

      --use_public_ips)
        use_public_ips="$2"
        ;;

      --service_account)
        service_account="$2"
        ;;

      *)
        command="$@"
        break
        ;;
    esac
    shift 2
  done
}

function main {
  if [[ $1 == /opt/gcp_variant_transforms/bin/* ]]; then
    exec $@
  fi
  parse_args "$@"

  # If missing, we will try to find the default values.
  google_cloud_project="${google_cloud_project:-$(gcloud config get-value project)}"
  region="${region:-$(gcloud config get-value compute/region)}"
  vt_docker_image="${vt_docker_image:-gcr.io/cloud-lifesciences/gcp-variant-transforms}"
  custom_runner_image="${custom_runner_image:-gcr.io/cloud-lifesciences/variant-transforms-custom-runner:latest}"

  location="${location:-}"
  temp_location="${temp_location:-}"
  subnetwork="${subnetwork:-}"
  use_public_ips="${use_public_ips:-}"
  service_account="${service_account:-}"

  if [[ -z "${google_cloud_project}" ]]; then
    echo "Please set the google cloud project using flag --project PROJECT."
    echo "Or set default project in your local client configuration using gcloud config set project PROJECT."
    exit 1
  fi

  if [[ -z "${region}" ]]; then
    echo "Please set the region using flags --region."
    echo "Or set default region in your local client configuration using gcloud config set compute/region REGION."
    exit 1
  fi

  if [[ -z "${temp_location}" ]]; then
    echo "Please set the temp_location using flag --temp_location."
    exit 1
  fi

  if [[ ! -v command ]]; then
    echo "Please specify a command to run Variant Transforms."
    exit 1
  fi

  # Build Dataflow required args based on `docker run ...` inputs.
  df_required_args="--project ${google_cloud_project} --region ${region} --temp_location ${temp_location}"

  # Build up optional args for pipelines-tools and Dataflow, if they are provided.
  pt_optional_args=""
  df_optional_args=""

  if [[ ! -z "${custom_runner_image}" ]]; then
    echo "Adding --sdk_container_image ${custom_runner_image} to optional_args"
    df_optional_args="${df_optional_args} --experiments=use_runner_v2 --sdk_container_image=${custom_runner_image}"
  fi

  if [[ ! -z "${subnetwork}" ]]; then
    echo "Adding --subnetwork ${subnetwork} to optional_args"
    pt_optional_args="${pt_optional_args} --subnetwork projects/${google_cloud_project}/regions/${region}/subnetworks/${subnetwork}"
    df_optional_args="${df_optional_args} --subnetwork https://www.googleapis.com/compute/v1/projects/${google_cloud_project}/regions/${region}/subnetworks/${subnetwork}"
  fi

  if [[ ! -z "${use_public_ips}" && "${use_public_ips}" == "false" ]]; then
    echo "Adding --private-address and --no_use_public_ips to optional_args"
    pt_optional_args="${pt_optional_args} --private-address"
    df_optional_args="${df_optional_args} --no_use_public_ips"
  fi

  if [[ ! -z "${service_account}" ]]; then
    echo "Adding --service-account ${service_account} to optional_args"
    pt_optional_args="${pt_optional_args} --service-account ${service_account}"
    df_optional_args="${df_optional_args} --service_account_email ${service_account}"
  fi

  # Optional location for Life Sciences API (default us-central1), see currently available
  # locations here: https://cloud.google.com/life-sciences/docs/concepts/locations
  l_s_location=""
  if [[ ! -z "${location}" ]]; then
    echo "Adding --location ${location} to Life Sciences API invocation command."
    l_s_location="--location ${location}"
  fi

  pipelines --project "${google_cloud_project}" ${l_s_location} run \
    --command "/opt/gcp_variant_transforms/bin/${command} ${df_required_args} ${df_optional_args}" \
    --output "${temp_location}"/runner_logs_$(date +%Y%m%d_%H%M%S).log \
    --output-interval 200s \
    --wait \
    --scopes "https://www.googleapis.com/auth/cloud-platform" \
    --regions "${region}" \
    --image "${vt_docker_image}" \
    --machine-type "g1-small" \
    --pvm-attempts 0 \
    --attempts 1 \
    --disk-size 10 \
    --boot-disk-size 100 \
    ${pt_optional_args}
}

main "$@"
