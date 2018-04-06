#!/bin/bash

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
# This script is intended to be used in the Docker image built for VEP.
# Note that the arguments are passed through environment variables and to log
# useful error messages when they are not properly set, we always check their
# existence the first time they are being accessed.
#
# The only enviroment variables that have to be set are (others are optional):
#
# VEP_CACHE: The path of the VEP cache which is a single .tar.gz file.
# INPUT_FILE: The path of the input file (might be a VCF or a compressed VCF).
# OUTPUT_VCF: The path of the output file which is always a VCF file.
#
# For the full list of supported environment variables and their documentation
# check README.md.
# Capital letter variables refer to environment variables that can be set from
# outside. Internal variables have small letters.

set -euo pipefail

readonly species="${SPECIES:-homo_sapiens}"
readonly assembly="${GENOME_ASSEMBLY:-GRCh38}"
readonly fork_opt="--fork ${NUM_FORKS:-1}"
readonly other_vep_opts="${OTHER_VEP_OPTS:---everything}"
readonly annotation_field_name="${VCF_INFO_FILED:-CSQ_VT}"

if [[ ! -r "${VEP_CACHE:?VEP_CACHE is not set!}" ]]; then
  echo "ERRPR: Cannot read ${VEP_CACHE}"
  exit 1
fi

# Check INPUT_FILE is set and is a readable file.
if [[ ! -r "${INPUT_FILE:?INPUT_FILE is not set!}" ]]; then
  echo "ERRPR: Cannot read ${INPUT_FILE}"
  exit 1
fi

# TODO(bashir2): Add support for multiple input and output files.

echo "Checking input file at $(date)"
ls -l "${INPUT_FILE}"

# Make sure output file does not exist and can be written.
if [[ -e ${OUTPUT_VCF:?OUTPUT_VCF is not set!} ]]; then
  echo "ERROR: ${OUTPUT_VCF} already exits!"
  exit 1
fi
touch ${OUTPUT_VCF}
rm ${OUTPUT_VCF}

readonly vep_cache_dir="$(dirname ${VEP_CACHE})"
readonly vep_cache_file="$(basename ${VEP_CACHE})"
pushd ${vep_cache_dir}
echo "Decompressing the cache file ${vep_cache_file} started at $(date)"
tar xzvf "${vep_cache_file}"
if [[ ! -d "${species}" ]]; then
  echo "Cannot find directory ${species} after decompressing ${vep_cache_file}!"
  exit 1
fi
popd

readonly vep_command="./vep -i ${INPUT_FILE} -o ${OUTPUT_VCF} \
  --dir ${vep_cache_dir} --offline --species ${species} --assembly ${assembly} \
  --vcf --allele_number --vcf_info_field ${annotation_field_name} ${fork_opt} \
  ${other_vep_opts}"
echo "VEP command is: ${vep_command}"

echo "Running vep started at $(date)"
# The next line should not be quoted since we want word splitting to happen.
${vep_command}
