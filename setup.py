"""Beam pipelines for processing variants based on VCF files."""

import setuptools

REQUIRED_PACKAGES = [
    'apache-beam[gcp]>=2.0',
    'pyvcf',
    ]

REQUIRED_SETUP_PACKAGES = [
    'nose>=1.0',
    ]

setuptools.setup(
    name='variant_processing',
    version='0.0.0',
    description='Variant processing tools based on Apache Beam.',
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
    package_data={'variant_processing': ['testing/testdata/*']},
)
