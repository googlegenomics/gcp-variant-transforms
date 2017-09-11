"""Beam pipelines for processing variants based on VCF files."""

import setuptools

REQUIRED_PACKAGES = [
    'apache-beam[gcp]>=2.0',
    'pyvcf',
    ]

REQUIRED_SETUP_PACKAGES = [
    'nose>=1.0',
    ]

# TODO(arostami): Add more details (e.g. package keywords).
setuptools.setup(
    name='variant_processing',
    version='0.0.0',
    description='Variant processing tools.',
    setup_requires=REQUIRED_SETUP_PACKAGES,
    install_requires=REQUIRED_PACKAGES,
    test_suite='nose.collector',
    packages=setuptools.find_packages(),
    package_data={'variant_processing': ['testing/testdata/*']},
)
