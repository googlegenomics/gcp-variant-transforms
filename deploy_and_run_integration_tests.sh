#!/bin/bash
set -euo pipefail

# Copyright 2018 Google Inc.  All Rights Reserved.
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
#
# This script builds a Docker image from the current state of the local code,
# pushes the image to a test registry (project 'gcp-variant-transforms-test')
# and runs integration tests against that image.
#
# To run this test successfully:
# - The user's gcloud credentials should be set; follow the steps at:
#   https://cloud.google.com/genomics/install-genomics-tools
#
# - The user should have access to the 'gcp-variant-transforms-test' project.

PROJECT="gcp-variant-transforms-test"
GS_DIR="integration_test_runs"
GREEN="\e[32m"
RED="\e[31m"
TEST_ARGUMENTS=""
KEEP_IMAGE_OPT="--keep_image"
IMAGE_TAG_OPT="--image_tag"
SKIP_BUILD_OPT="--skip_build"
image_tag=""
keep_image=""
skip_build=""

#################################################
# Prints a given message with a color.
# Arguments:
#   $1: The message
#   $2: The text for the color, e.g., "\e[32m" for green.
#################################################
color_print() {
  echo -n -e "$2"  # Sets the color to the given color.
  echo "$1"
  echo -n -e "\e[0m"  # Resets the color to no color.
}

#################################################
# Prints the usage.
#################################################
usage() {
  echo "Usage: $0 [${KEEP_IMAGE_OPT}] [${IMAGE_TAG_OPT} image_tag] "
  echo "    [${SKIP_BUILD_OPT}] [... test script options ...]"
  echo "  With ${KEEP_IMAGE_OPT} the tested image is not deleted at the end."
  echo "  If ${IMAGE_TAG_OPT} is not set, a new image with the current time"
  echo "    tag is created and used in tests, otherwise the given tag is used."
  echo "  ${SKIP_BUILD_OPT} skips the build step so it has to be used with "
  echo "    a valid tag_name passed through {$IMAGE_TAG_OPT}."
  echo "  This script should be run from the root of source tree."
}

#################################################
# Makes sure that we are in the root of the source tree and cloudbuild.yaml
# file exists.
#################################################
check_dir() {
  dir_ok="$(pwd | sed -e 's/.*gcp-variant-transforms$/MATCHED/')"
  if [[ "${dir_ok}" != "MATCHED" ]]; then
    usage
    color_print "ERROR: Not run from the gcp-variant-transforms root!" \
        "${RED}"
    exit 1
  fi
  # The condition with 'true' is to work around 'set -e'.
  build_file="$(ls cloudbuild.yaml)" || true
  if [[ ${build_file} != "cloudbuild.yaml" ]]; then
    usage
    color_print "ERROR: Cannot find 'cloudbuild.yaml'!" "${RED}"
    exit 1
  fi
}

#################################################
# Parses arguments and does some sanity checking. Any non-recognized argument is
# added to ${TEST_ARGUMENTS} to be passed to the test script later.
# Arguments:
#   It is expected that this is called with $@ of the main script.
#################################################
parse_args() {
  while [[ $# -gt 0 ]]; do
    if [[ "$1" = "${KEEP_IMAGE_OPT}" ]]; then
      keep_image="yes"  # can be any non-empty string
      shift
    elif [[ "$1" = "${IMAGE_TAG_OPT}" ]]; then
      shift
      if [[ $# == 0 ]]; then
        usage
        color_print "ERROR: No tag provided after ${IMAGE_TAG_OPT}!" "${RED}"
        exit 1
      fi
      image_tag="$1"
      color_print "Using custom image tag: ${image_tag}" "${GREEN}"
      shift
    elif [[ "$1" = "${SKIP_BUILD_OPT}" ]]; then
      skip_build="yes"  # can be any non-empty string
      shift
    else
      TEST_ARGUMENTS="${TEST_ARGUMENTS} $1"
      shift
    fi
  done
  if [[ ( ! -z "${skip_build}" ) && ( -z "${image_tag}" ) ]]; then
    color_print "ERROR: ${SKIP_BUILD_OPT} with no ${IMAGE_TAG_OPT}" "${RED}"
    exit 1
  fi
}

#################################################
# Deactivates virtualenv, removes its directory, and deletes the image.
#################################################
clean_up() {
  color_print "Removing integration test environment ${temp_dir}" "${GREEN}"
  deactivate
  rm -rf "${temp_dir}"
  if [[ -z "${keep_image}" ]]; then
    # TODO(bashir2): Find a way to mark these images as temporary such that they are
    # garbage collected automatically if the test fails before this line.
    color_print "Deleting the test image from the registry" "${GREEN}"
    gcloud container images delete --quiet "${full_image_name}"
  else
    color_print "Keeping the test image ${image_tag}" "${GREEN}"
  fi
}

#################################################
# Main
#################################################
check_dir
parse_args $@
color_print "Arguments to be passed to the test script: " "${GREEN}"
color_print "${TEST_ARGUMENTS}" "${GREEN}"

if [[ -z "${image_tag}" ]]; then
  time_tag="$(date +%F-%H-%M-%S)"
  image_tag="test_${time_tag}"
fi
full_image_name="gcr.io/${PROJECT}/gcp-variant-transforms:${image_tag}"

if [[ -z "${skip_build}" ]]; then
  color_print "Building the Docker image with tag ${image_tag}" "${GREEN}"
  # TODO(bashir2): This will pick and include all directories in the image,
  # including local build and library dirs that do not need to be included.
  # Update this to include only the required files/directories.
  gcloud container builds submit --config "${build_file}" \
      --project "${PROJECT}" \
      --substitutions _CUSTOM_TAG_NAME="${image_tag}" .
fi

# Running integration tests in a temporary virtualenv
temp_dir="$(mktemp -d)"
color_print "Setting up integration test environment in ${temp_dir}" "${GREEN}"
# Since we have no prompt we need to disable prompt changing in virtualenv.
export VIRTUAL_ENV_DISABLE_PROMPT="something"
virtualenv "${temp_dir}"
source ${temp_dir}/bin/activate;
trap clean_up EXIT
pip install --upgrade .[int_test]
color_print "Running integration tests against ${full_image_name}" "${GREEN}"
python gcp_variant_transforms/testing/integration/run_tests.py \
    --project "${PROJECT}" \
    --staging_location "gs://${GS_DIR}/staging" \
    --temp_location "gs://${GS_DIR}/temp" \
    --logging_location "gs://${GS_DIR}/temp/logs" \
    --image "${full_image_name}" ${TEST_ARGUMENTS}

error_code="$?"
if [[ ${error_code} == 0 ]]; then
  color_print "Success!" "${GREEN}"
else
  color_print "FAILED!" "${RED}"
fi


