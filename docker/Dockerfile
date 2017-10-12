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

FROM google/cloud-sdk

# TODO(arostami): Figure out why 'setup.py install' does't work.
RUN pip install apache_beam[gcp] pyvcf

RUN mkdir -p /opt/variant_processing/bin && mkdir -p /opt/variant_processing/src
ADD / /opt/variant_processing/src/
RUN echo 'python /opt/variant_processing/src/vcf_to_bq.py "$@"' > \
      /opt/variant_processing/bin/vcf_to_bq && \
    chmod +x /opt/variant_processing/bin/vcf_to_bq
