#!/bin/bash

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

readonly google_cloud_project="${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project)}"
readonly temp_location="${TEMP_LOCATION}"
readonly service_account_scopes="${SERVICE_ACCOUNT_SCOPE:-https://www.googleapis.com/auth/cloud-platform}"
readonly vt_docker_image="${DOCKER_IMAGE:-gcr.io/gcp-variant-transforms/gcp-variant-transforms}"
readonly zones="${ZONES:-us-west1-b}"
readonly pipelines_flags="${PIPELINES_FLAGS}"

if [[ -z "${google_cloud_project}" ]]; then
  echo "Please set the google cloud project through the environment var GOOGLE_CLOUD_PROJECT or use gcloud config set project myProject."
  exit 1
fi

script_to_run="$1"
script_to_run_args="${@:2}"

supported_funcs=('vcf_to_bq' 'bq_to_vcf' 'vcf_to_bq_preprocessor')

if echo "${supported_funcs[@]}" | grep -q -w "${script_to_run}"; then
  echo "Running ${script_to_run} starting at $(date)."
else
  echo "${script_to_run} is not supported, please choose one of the following: ${supported_funcs[@]}."
  exit 1
fi

COMMAND="/opt/gcp_variant_transforms/bin/${script_to_run} \
         ${script_to_run_args} \
         --project ${google_cloud_project}"

gcloud alpha genomics pipelines run ${pipelines_flags} \
  --project "${google_cloud_project}" \
  --logging "${temp_location}"/runner_logs_$(date +%Y%m%d_%H%M%S).log \
  --service-account-scopes  "${service_account_scopes}" \
  --zones "${zones}" \
  --docker-image "${vt_docker_image}" \
  --command-line "${COMMAND}"
