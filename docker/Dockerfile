# Copyright 2017 Google Inc.  All Rights Reserved.
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

# To build a new docker image, run the following from the root source dir:
# $ docker build . -f docker/Dockerfile -t $IMAGE_NAME

FROM golang:latest

RUN go get -ldflags '-extldflags "-fno-PIC -static"' -buildmode pie -tags 'osusergo netgo static_build' github.com/googlegenomics/pipelines-tools/pipelines

COPY --from=apache/beam_python3.8_sdk:2.34.0 /opt/apache/beam /opt/apache/beam


FROM google/cloud-sdk:slim
COPY --from=0 /go/bin/pipelines /usr/bin

ARG commit_sha
ENV COMMIT_SHA=${commit_sha}

RUN mkdir -p /opt/gcp_variant_transforms/bin && mkdir -p /opt/gcp_variant_transforms/src
ADD / /opt/gcp_variant_transforms/src/

# Needed for installing mmh3 (one of the required packages in setup.py).
RUN apt install -y g++

# Install Pysam dependencies. These dependencies are only required because we
# have a monolithic binary - they primarily have to be installed on the workers.
RUN apt-get update && apt-get install -y \
    autoconf \
    automake \
    gcc \
    libbz2-dev \
    libcurl4-openssl-dev \
    liblzma-dev \
    libssl-dev \
    make \
    perl \
    zlib1g-dev \
    python3-pip \
    python3-venv

# Install dependencies.
RUN python3 -m venv /opt/gcp_variant_transforms/venv3 && \
    sed -i 's/$1/${1:-}/' /opt/gcp_variant_transforms/venv3/bin/activate && \
    . /opt/gcp_variant_transforms/venv3/bin/activate && \
    cd /opt/gcp_variant_transforms/src && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade wheel && \
    python3 -m pip install --upgrade .

RUN printf '#!/bin/bash\n%s\n%s' \
      ". /opt/gcp_variant_transforms/venv3/bin/activate && cd /opt/gcp_variant_transforms/src" \
      'python -m gcp_variant_transforms.vcf_to_bq --setup_file ./setup.py "$@"' > \
      /opt/gcp_variant_transforms/bin/vcf_to_bq && \
    chmod +x /opt/gcp_variant_transforms/bin/vcf_to_bq

RUN printf '#!/bin/bash\n%s\n%s' \
      ". /opt/gcp_variant_transforms/venv3/bin/activate && cd /opt/gcp_variant_transforms/src" \
      'python -m gcp_variant_transforms.vcf_to_bq_preprocess --setup_file ./setup.py "$@"' > \
      /opt/gcp_variant_transforms/bin/vcf_to_bq_preprocess && \
    chmod +x /opt/gcp_variant_transforms/bin/vcf_to_bq_preprocess

RUN printf '#!/bin/bash\n%s\n%s' \
      ". /opt/gcp_variant_transforms/venv3/bin/activate && cd /opt/gcp_variant_transforms/src" \
      'python -m gcp_variant_transforms.bq_to_vcf --setup_file ./setup.py "$@"' > \
      /opt/gcp_variant_transforms/bin/bq_to_vcf && \
    chmod +x /opt/gcp_variant_transforms/bin/bq_to_vcf

ENTRYPOINT ["/opt/gcp_variant_transforms/src/docker/pipelines_runner.sh"]
