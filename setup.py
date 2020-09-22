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

import subprocess
from distutils.command.build import build as _build

import os
import time
import setuptools

PYSAM_DEPENDENCY_COMMANDS = [
    ['apt-get', 'update'],
    ['apt-get', '-y', 'install', 'autoconf', 'automake', 'gcc', 'libbz2-dev',
     'libcurl4-openssl-dev', 'liblzma-dev', 'libssl-dev', 'make', 'perl',
     'zlib1g-dev']
]

PYSAM_INSTALLATION_COMMAND = ['python3', '-m', 'pip', 'install', 'pysam<0.16.0']

REQUIRED_PACKAGES = [
    'cython>=0.28.1',
    'apache-beam[gcp]==2.24.0',
    # Note that adding 'google-api-python-client>=1.6' causes some dependency
    # mismatch issues. This is fatal if using 'setup.py install', but works on
    # 'pip install .' as it ignores conflicting versions. See Issue #71.
    'google-api-python-client>=1.6,<1.7.12',
    'intervaltree>=2.1.0,<2.2.0',
    'mmh3<2.6',
    'google-cloud-storage',
    'pyfarmhash',
    'pyyaml'
]

REQUIRED_SETUP_PACKAGES = [
    'nose>=1.0',
]

class CustomCommands(setuptools.Command):
  """A setuptools Command class able to run arbitrary commands."""

  def initialize_options(self):
    pass

  def finalize_options(self):
    pass

  def RunCustomCommand(self, command_list):
    print('Running command: %s' % command_list)
    try:
      subprocess.call(command_list)
    except Exception as e:
      raise RuntimeError(
          'Command %s failed with error: %s' % (command_list, e)) from e

  def do_install(self):
    try:
      # For superuser UID is 0, so attempt to install pysam's C dependencies.
      if not os.getuid():
        for command in PYSAM_DEPENDENCY_COMMANDS:
          self.RunCustomCommand(command)
      self.RunCustomCommand(PYSAM_INSTALLATION_COMMAND)

    except RuntimeError as e:
      raise RuntimeError(
          'PySam installation has failed. Make sure you have the ' + \
          'following packages installed: autoconf automake gcc libbz2-dev ' + \
          'liblzma-dev libcurl4-openssl-dev libssl-dev make perl ' + \
          'zlib1g-dev') from e

  def run(self):

    # When running vcf_to_bq under with Dataflow cost optimizations:
    #   --flexrs_goal=COST_OPTIMIZED \
    #   --runner DataflowRunner
    #
    # It was observed that new workers would fail with:
    #   <snip>
    #   File "/usr/local/lib/python2.7/site-packages/pysam/__init__.py",
    #     line 5, in <module> from pysam.libchtslib import *
    #   ImportError: No module named libchtslib
    #
    # Root cause for the failure has not been determined, but with the
    # following retries, the problem has no longer been observed:

    for _ in range(10):
      try:
        self.do_install()

        # pylint: disable=import-outside-toplevel, unused-import
        from pysam import libchtslib
      except ImportError:
        time.sleep(10)

    # Try one more time before exiting; would rather fail here than downstream.
    # pylint: disable=import-outside-toplevel, unused-import
    from pysam import libchtslib

class build(_build):  # pylint: disable=invalid-name
  """A build command class that will be invoked during package install.

  The package built using the current setup.py will be staged and later
  installed in the worker using `pip install package'. This class will be
  instantiated during install for this specific scenario and will trigger
  running the custom commands specified.
  """
  sub_commands = _build.sub_commands + [('CustomCommands', None)]

setuptools.setup(
    name='gcp_variant_transforms',
    version='0.9.0',
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
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],

    setup_requires=REQUIRED_SETUP_PACKAGES,
    install_requires=REQUIRED_PACKAGES,
    test_suite='nose.collector',
    packages=setuptools.find_packages(),
    package_data={
        'gcp_variant_transforms': ['gcp_variant_transforms/testing/testdata/*']
    },

    cmdclass={
        # Command class instantiated and run during pip install scenarios.
        'build': build,
        'CustomCommands': CustomCommands,
    },
)
