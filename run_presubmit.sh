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

# A helper script for ensuring all checks pass before submitting any change.

echo ========== Running unit tests.
if [[ -z `which coverage` ]];then
  echo "coverage is not installed. Installing ..."
  python -m pip install coverage
fi
# coverage run --source=gcp_variant_transforms setup.py test
# Add individual files and packages as they are migrated to Python 3. Once all
# of the files are migrated, delete this code and uncomment the above line.
coverage run --source=gcp_variant_transforms setup.py test -s setup.py
coverage run --source=gcp_variant_transforms setup.py test -s \
  gcp_variant_transforms.beam_io
coverage run --source=gcp_variant_transforms setup.py test -s \
  gcp_variant_transforms.libs
coverage run --source=gcp_variant_transforms setup.py test -s \
  gcp_variant_transforms.transforms

echo ========== Running pylint.
if [[ -z `which pylint` ]];then
  echo "pylint is not installed. Installing ..."
  python -m pip install pylint
fi
# python -m pylint gcp_variant_transforms
# Add individual files and packages as they are migrated to Python 3. Once all
# of the files are migrated, delete this code and uncomment the above line.
python -m pylint setup.py
python -m pylint gcp_variant_transforms/beam_io
python -m pylint gcp_variant_transforms/libs
python -m pylint gcp_variant_transforms/transforms
