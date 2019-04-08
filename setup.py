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

"""Beam pipelines for processing variants based on VCF files."""

import setuptools

REQUIRED_PACKAGES = [
    'cython>=0.28.1',
    # TODO(bashir2): Drop the <=2.4 condition once the build is fixed with 2.5.
    'apache-beam[gcp]>=2.3,<=2.4',
    # Note that adding 'google-api-python-client>=1.6' causes some dependency
    # mismatch issues. This is fatal if using 'setup.py install', but works on
    # 'pip install .' as it ignores conflicting versions. See Issue #71.
    'google-api-python-client>=1.6',
    'intervaltree>=2.1.0,<2.2.0',
    'pyvcf<0.7.0',
    'google-nucleus==0.2.0',
    # Nucleus needs uptodate protocol buffer compiler (protoc).
    'protobuf>=3.6.1',
    'mmh3<2.6',
    # Need to explicitly install v<=1.2.0. apache-beam requires
    # google-cloud-pubsub 0.26.0, which relies on google-cloud-core<0.26dev,
    # >=0.25.0. google-cloud-storage also has requirements on google-cloud-core,
    # and version 1.2.0 resolves the dependency conflicts.
    'google-cloud-storage<=1.2.0'
]

INTEGRATION_TEST_REQUIREMENTS = [
    # Need to explicitly install v>0.25 as the BigQuery python API has changed.
    'google-cloud-bigquery>0.25'
]

REQUIRED_SETUP_PACKAGES = [
    'nose>=1.0',
]

setuptools.setup(
    name='gcp_variant_transforms',
    version='0.7.0',
    description=('Tool for transforming and processing VCF files in a '
                 'scalable manner based on Apache Beam'),
    author='Google',
    license='Apache 2.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers for the list
    # of values.
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: System :: Distributed Computing',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],

    setup_requires=REQUIRED_SETUP_PACKAGES,
    install_requires=REQUIRED_PACKAGES,
    extras_require={
        'int_test': INTEGRATION_TEST_REQUIREMENTS,
    },
    test_suite='nose.collector',
    packages=setuptools.find_packages(),
    package_data={
        'gcp_variant_transforms': ['gcp_variant_transforms/testing/testdata/*']
    },
)
