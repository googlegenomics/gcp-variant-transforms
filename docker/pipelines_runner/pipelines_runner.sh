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

PROJECT_OPT="--project"
TEMP_LOCATION_OPT="--temp_location"
DOCKER_IMAGE_OPT="--docker_image"
ZONES_OPT="--zones"
COMMAND_OPT="--command"
SERVICE_ACCOUNT_SCOPES="https://www.googleapis.com/auth/cloud-platform"

#################################################
# Parses arguments and does some sanity checking.
# Arguments:
#   It is expected that this is called with $@ of the main script.
#################################################
function parse_args {
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      ${PROJECT_OPT})
        if [[ "$#" == 1 ]]; then
          echo "ERROR: No project provided after $1!"
          exit 1
        fi
        google_cloud_project="$2"
        ;;

      ${TEMP_LOCATION_OPT})
        if [[ "$#" == 1 ]]; then
          echo "ERROR: No temp location provided after $1!"
          exit 1
        fi
        temp_location="$2"
        ;;

      ${DOCKER_IMAGE_OPT})
        if [[ "$#" == 1 ]]; then
          echo "ERROR: No docker image provided after $1!"
          exit 1
        fi
        vt_docker_image="$2"
        ;;

      ${ZONES_OPT})
        if [[ "$#" == 1 ]]; then
          echo "ERROR: No zones provided after $1!"
          exit 1
        fi
        zones="$2"
        ;;

      ${COMMAND_OPT})
        if [[ "$#" == 1 ]]; then
          echo "ERROR: No command provided after $1!"
          exit 1
        fi
        command="$2"
        ;;

      *)
        echo "Unrecognized flags $@."
        exit 1
        ;;
    esac
    shift 2
  done
}

function main {
  parse_args "$@"
  google_cloud_project="${google_cloud_project:-$(gcloud config get-value project)}"
  vt_docker_image="${vt_docker_image:-gcr.io/gcp-variant-transforms/gcp-variant-transforms}"
  zones="${zones:-$(gcloud config get-value compute/zone)}"
  temp_location="${temp_location:-''}"

  if [[ -z "${google_cloud_project}" ]]; then
    echo "Please set the google cloud project using flag --project PROJECT."
    echo "Or set default project in your local client configuration using gcloud config set project PROJECT."
    exit 1
  fi

  if [[ -z "${zones}" ]]; then
    echo "Please set the zones using flags --zones."
    echo "Or set default zone in your local client configuration using gcloud config set compute/zone ZONE."
    exit 1
  fi

  if [[ ! -v command ]]; then
    echo "Please specify the command."
    exit 1
  fi

  gcloud alpha genomics pipelines run \
    --project "${google_cloud_project}" \
    --logging "${temp_location}"/runner_logs_$(date +%Y%m%d_%H%M%S).log \
    --service-account-scopes  "${SERVICE_ACCOUNT_SCOPES}" \
    --zones "${zones}" \
    --docker-image "${vt_docker_image}" \
    --command-line "/opt/gcp_variant_transforms/bin/${command} --project ${google_cloud_project}"
}

main "$@"
