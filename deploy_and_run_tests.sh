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
# and runs integration tests against that image. By default, only integration
# tests for vcf_to_bq pipeline will run. To run the preprocessor integration
# tests, use --run_preprocessor_tests options.
#
# To run this test successfully:
# - The user's gcloud credentials should be set; follow the steps at:
#   https://cloud.google.com/genomics/install-genomics-tools
#
# - The user should have access to the 'gcp-variant-transforms-test' project.

GREEN="\e[32m"
RED="\e[31m"
TEST_ARGUMENTS=""
GS_DIR_OPT="--gs_dir"
KEEP_IMAGE_OPT="--keep_image"
IMAGE_TAG_OPT="--image_tag"
PROJECT_OPT="--project"
RUN_UNIT_TEST_OPT="--run_unit_tests"
RUN_PREPROCESSOR_TEST_OPT="--run_preprocessor_tests"
SKIP_BUILD_OPT="--skip_build"
image_tag=""
keep_image=""
skip_build=""
gs_dir="integration_test_runs"  # default GS dir to store logs, etc.
project="gcp-variant-transforms-test"  # default project to use
run_unit_tests=""  # By default do not run unit-tests.
run_preprocessor_tests=""  # By default skip preprocessor integration tests.

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
  echo "Usage: $0  [${GS_DIR_OPT} gs_dir] [${KEEP_IMAGE_OPT}]"
  echo "    [${IMAGE_TAG_OPT} image_tag] [${PROJECT_OPT} project_name] "
  echo "    [${RUN_UNIT_TEST_OPT}] [${SKIP_BUILD_OPT}] [... test script options ...]"
  echo "  ${GS_DIR_OPT} can be used to change the default GS bucket used for"
  echo "    test run artifacts like logs, staging, etc. This has to be set if"
  echo "    the default project is not used."
  echo "  With ${KEEP_IMAGE_OPT} the tested image is not deleted at the end."
  echo "  If ${IMAGE_TAG_OPT} is not set, a new image with the current time"
  echo "    tag is created and used in tests, otherwise the given tag is used."
  echo "  ${PROJECT_OPT} sets the cloud project. This project is used to push "
  echo "    the image, create BigQuery tables, run Genomics pipelines etc."
  echo "    Default is gcp-variant-transforms-test."
  echo "  ${RUN_UNIT_TEST_OPT} runs the unit-tests before integration tests."
  echo "  ${RUN_PREPROCESSOR_TEST_OPT} runs the preprocessor integration tests."
  echo "  ${SKIP_BUILD_OPT} skips the build step so it has to be used with "
  echo "    a valid tag_name passed through {$IMAGE_TAG_OPT}."
  echo "  This script should be run from the root of source tree."
}

#################################################
# Makes sure that we are in the root of the source tree and cloudbuild.yaml
# file exists.
#################################################
check_dir() {
  local test_script="gcp_variant_transforms/testing/integration/run_vcf_to_bq_tests.py"
  ls_script_file="$(ls ${test_script})" || true
  dir_ok="$(echo "${ls_script_file}" | sed -e 's/.*run_vcf_to_bq_tests.py$/MATCHED/')"
  if [[ "${dir_ok}" != "MATCHED" ]]; then
    usage
    color_print "ERROR: Cannot find ${test_script}" "${RED}"
    color_print "Seems not run from the root of the source tree!" "${RED}"
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
    elif [[ "$1" = "${GS_DIR_OPT}" ]]; then
      shift
      if [[ $# == 0 ]]; then
        usage
        color_print "ERROR: No name provided after ${GS_DIR_OPT}!" "${RED}"
        exit 1
      fi
      gs_dir="$1"
      color_print "Using GS directory: ${gs_dir}" "${GREEN}"
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
    elif [[ "$1" = "${PROJECT_OPT}" ]]; then
      shift
      if [[ $# == 0 ]]; then
        usage
        color_print "ERROR: No name provided after ${PROJECT_OPT}!" "${RED}"
        exit 1
      fi
      project="$1"
      color_print "Using project: ${project}" "${GREEN}"
      shift
    elif [[ "$1" = "${RUN_UNIT_TEST_OPT}" ]]; then
      run_unit_tests="yes"  # can be any non-empty string
      shift
    elif [[ "$1" = "${RUN_PREPROCESSOR_TEST_OPT}" ]]; then
      run_preprocessor_tests="yes"  # can be any non-empty string
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
full_image_name="gcr.io/${project}/gcp-variant-transforms:${image_tag}"

if [[ -z "${skip_build}" ]]; then
  color_print "Building the Docker image with tag ${image_tag}" "${GREEN}"
  # TODO(bashir2): This will pick and include all directories in the image,
  # including local build and library dirs that do not need to be included.
  # Update this to include only the required files/directories.
  gcloud container builds submit --config "${build_file}" \
      --project "${project}" \
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
if [[ -n "${run_unit_tests}" ]]; then
  pip install --upgrade .
  python setup.py test
fi
pip install --upgrade .[int_test]

color_print "Running integration tests against ${full_image_name}" "${GREEN}"
python gcp_variant_transforms/testing/integration/run_vcf_to_bq_tests.py \
    --project "${project}" \
    --staging_location "gs://${gs_dir}/staging" \
    --temp_location "gs://${gs_dir}/temp" \
    --logging_location "gs://${gs_dir}/temp/logs" \
    --image "${full_image_name}" ${TEST_ARGUMENTS} &
pid_vcf_to_bq="$!"
if [[ -n "${run_preprocessor_tests}" ]]; then
  python gcp_variant_transforms/testing/integration/run_preprocessor_tests.py \
      --project "${project}" \
      --staging_location "gs://${gs_dir}/staging" \
      --temp_location "gs://${gs_dir}/temp" \
      --logging_location "gs://${gs_dir}/temp/logs" \
      --image "${full_image_name}" &
fi
# `pid_preprocess` could be the same as `pid_vcf_to_bq` if preprocessor tests
# are not run.
pid_preprocess="$!"
if wait "${pid_vcf_to_bq}" && wait "${pid_preprocess}"; then
  color_print "$0 succeeded!" "${GREEN}"
else
  exit 1
fi
