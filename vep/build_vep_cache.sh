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
# This is a script for downloading VEP cache files, decompressing and placing
# them in the appropriate directory structure that is expected by VEP script.
# At the end, the whole structure is compressed to generate a single tar.gz
# file that can be used in run_vep.sh invocations.
#
# This script creates a 'vep_cache' sub-directory and does every other file
# operations and downloads inside that directory. The final cache file will be
# stored in that directory as well.
#
# Capital letter variables refer to environment variables that can be set from
# outside. Internal variables have small letters. All environment variables
# have a default value as well to set up cache for homo_sapiens with reference
# GRCh38 and release 91 of VEP.
#
# More details on cache files can be found here:
# https://ensembl.org/info/docs/tools/vep/script/vep_cache.html

set -euo pipefail

readonly release="${ENSEMBL_RELEASE:-91}"
readonly species="${VEP_SPECIES:-homo_sapiens}"
readonly assembly="${GENOME_ASSEMBLY:-GRCh38}"
readonly work_dir="vep_cache"

mkdir -p "${work_dir}"
pushd "${work_dir}"
readonly cache_file="${species}_vep_${release}_${assembly}.tar.gz"
readonly ftp_base="ftp://ftp.ensembl.org/pub/release-${release}"
readonly remote_cache="${ftp_base}/variation/VEP/${cache_file}"
echo "Downloading ${remote_cache} ..."
curl -O "${remote_cache}"

# The fasta file name depends on the species and assembly but not the version.
# Also the first letter of the file is capital while it is small for the actual
# cache file (above). For example: "Homo_sapiens.GRCh38.dna.toplevel.fa.gz"
readonly fasta_file="${species^?}.${assembly}.dna.toplevel.fa.gz"
readonly remote_fasta="${ftp_base}/fasta/homo_sapiens/dna_index/${fasta_file}"
echo "Downloading ${remote_fasta} and its index files ..."
curl -O "${remote_fasta}"
curl -O "${remote_fasta}.fai"
curl -O "${remote_fasta}.gzi"

echo "Decompressing cache files ..."
tar xzf "${cache_file}"

echo "Moving fasta files to the cache structure ..."
mv ${fasta_file}* "${species}/${release}_${assembly}"

echo "Creating single tar.gz file for the whole cache ..."
readonly output_cache="vep_cache_${species}_${assembly}_${release}.tar.gz"
tar czf "${output_cache}" "${species}"
if [[ -r "${output_cache}" ]]; then
  echo "Cleaning up ..."
  rm -rf "${species}"
  rm -f "${cache_file}"
fi
popd

if [[ -r "${work_dir}/${output_cache}" ]]; then
  echo "Successfully created cache file at ${work_dir}/${output_cache}"
else
  echo "ERROR: Something went wrong when creating ${work_dir}/${output_cache} !"
fi

# TODO(bashir2): Experiment with the convert_cache.pl script of VEP and measure performance
# improvements. If the change is significant then this script has to run convert_cache.pl too.

