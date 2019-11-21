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
  getopt -o '' -l project:,temp_location:,docker_image:,region: -- "$@"
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --project)
        google_cloud_project="$2"
        ;;

      --temp_location)
        temp_location="$2"
        ;;

      --docker_image)
        vt_docker_image="$2"
        ;;

      --region)
        region="$2"
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

  google_cloud_project="${google_cloud_project:-$(gcloud config get-value project)}"
  vt_docker_image="${vt_docker_image:-gcr.io/cloud-lifesciences/gcp-variant-transforms:${COMMIT_SHA}}"
  region="${region:-$(gcloud config get-value compute/region)}"
  temp_location="${temp_location:-''}"

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

  if [[ ! -v command ]]; then
    echo "Please specify a command to run Variant Transforms."
    exit 1
  fi

  pipelines --project "${google_cloud_project}" run \
    --command "/opt/gcp_variant_transforms/bin/${command} --project ${google_cloud_project} --region ${region}" \
    --output "${temp_location}"/runner_logs_$(date +%Y%m%d_%H%M%S).log \
    --wait \
    --scopes "https://www.googleapis.com/auth/cloud-platform" \
    --regions "${region}" \
    --image "${vt_docker_image}" \
    --pvm-attempts 0 \
    --attempts 1 \
    --disk-size 10
}

main "$@"
