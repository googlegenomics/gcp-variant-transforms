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
    'apache-beam[gcp]>=2.0',
    'pyvcf<0.7.0',
    ]

REQUIRED_SETUP_PACKAGES = [
    'nose>=1.0',
    ]

setuptools.setup(
    name='gcp_variant_transforms',
    version='0.0.0',
    description=('Tool for transforming and processing VCF files in a '
                 'scalable manner based on Apache Beam'),
    author='Google',
    license='Apache 2.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers for the list
    # of values.
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
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
    test_suite='nose.collector',
    packages=setuptools.find_packages(),
    package_data={
        'gcp_variant_transforms': ['gcp_variant_transforms/testing/testdata/*']
    },
)
