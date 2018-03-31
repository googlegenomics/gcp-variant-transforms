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
# This is branched from batch/build_annotator/Dockerfile.vep file of the
# verilylifesciences/variant-annotation GitHub repo.
#
# Example:
#
# docker build . --build-arg ENSEMBL_RELEASE=91 --tag vep_91
#
# To run vep through containers created by this file, the VEP cache has to be
# downloaded separately and made available through command line arguments.
# The script for doing so is build_vep_cache.sh, see README.md for details.

# Start from this container so that gcloud and all its dependencies are already
# available.
FROM gcr.io/cloud-builders/gcloud

ARG ENSEMBL_RELEASE=91
ARG VEP_BASE=/opt/variant_effect_predictor

RUN apt-get -y update && apt-get install -y \
    build-essential \
    git \
    libarchive-zip-perl \
    libdbd-mysql-perl \
    libdbi-perl \
    libfile-copy-recursive-perl \
    libhts1 \
    libjson-perl \
    libmodule-build-perl \
    tabix \
    unzip \
    zlib1g-dev

# Install VEP per the instructions at:
# http://www.ensembl.org/info/docs/tools/vep/script/vep_download.html#installer
RUN git clone https://github.com/Ensembl/ensembl-vep.git ${VEP_BASE}

WORKDIR ${VEP_BASE}

RUN git checkout release/${ENSEMBL_RELEASE}

RUN perl INSTALL.pl \
    --AUTO a \
    --NO_UPDATE

ADD run_vep.sh ${VEP_BASE}/run_vep.sh

ENTRYPOINT ["bash"]
