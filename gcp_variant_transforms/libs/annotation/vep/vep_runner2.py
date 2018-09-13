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

# TODO(jessime) Rename to `vep_runner.py`. I've left the original file in place
# for now so it's easy to see what functionality it had and can quickly copy
# useful components. In the future, there is likely no reason to keep both
# files.

import os
import sys
import uuid
import requests
import apache_beam as beam
import random
import string
import traceback
import logging

from gcp_variant_transforms.libs import vcf_header_parser
from gcp_variant_transforms.libs.annotation import annotation_parser
from gcp_variant_transforms.beam_io import vcfio


# TODO(jessime) If this mapping exists elsewhere, import it instead.
TYPE_MAP = {'string': str,
            'float': float,
            'integer': int}

def join_lines(key_values):
  """Create single string from grouped vcf lines

  Args:
    key_values: Single key from KeyGenFn and all grouped values. Values
        are (ProcessedVariant, vcf_line) pairs.

  Returns:
    variants: ProcessedVariants
    joined_vcf_lines: Single string of multiple variants to be sent to server
        for annotation.
  """
  # type: (Tuple[UUID, List[Tuple[ProcessedVariant, str]]]) -> List[ProcessedVariant, str]

  # TODO (jessime) This was working fine in my prototype code.
  # I don't know why values have gotten wrapped into an `_UnwindowedValue`, but
  # Allie has run into this, and it should be fine to just cast to list.
  # values = kv[1]._windowed_values
  values = list(key_values[1])
  variants = [v[0] for v in values]
  joined_vcf_lines = '\n'.join(v[1] for v in values)
  return variants, joined_vcf_lines


def start_vep_server():
  """"""
  # TODO(jessime) Figure out how to automatically start the server.
  # Equally important, and maybe more challenging:
  # figure out how to detect when the server is up and running.
  # I currently do both of these manually. Starting the server is done by:
  # running `gcloud app deploy --project gcp-variant-transforms-test` from
  # within the directory containing app.yaml. Checking for a server that's
  # ready (it needs time to build the VEP cache) is done by running
  # `python vep_client_examples.py`, which sends a GET request to the server.
  # The server should respond 'Healthy.'.
  # Automating launching the server should be similar to the current
  # implementation of launching annotation VMs through the pipelines API. This
  # could be done near the beginning of the VT pipeline. Later, `Client` can
  # periodically ping the server until it gets a 200 response, and then start
  # sending variant data.
  pass


# TODO(jessime) This function may not be necessary. I added it while debugging
# what was likely a separate issue. The json response is loaded as unicode.
# This function converts all (nested) unicode into `str`. It's taken from here:
# https://stackoverflow.com/questions/956867/how-to-get-string-objects-instead-of-unicode-from-json
def _byteify(data, ignore_dicts=False):
  if isinstance(data, unicode):
    return data.encode('utf-8')
  if isinstance(data, list):
    return [_byteify(item, ignore_dicts=True) for item in data]
  if isinstance(data, dict) and not ignore_dicts:
    return {
      _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
      for key, value in data.iteritems()
    }
  return data


def get_headers(url):
  """GET headers from annotation server.

  Args:
    url: Location of annotation server.

  Returns:
    headers: Properly ordered list of annotation header information. e.g.
      [{'name': 'Gene', 'type': 'string', 'desc': 'Ensembl stable ID...'},
       {'name': 'ALLELE_NUM', 'type': 'integer', 'desc': 'allele num...'}]
  """
  # type: (str) -> List[Dict[str, str]]
  # TODO(jessime) This might be better as a classmethod of Client
  response = requests.get(os.path.join(url, 'headers'))
  headers = _byteify(response.json(object_hook=_byteify), ignore_dicts=True)
  return headers


class KeyGenFn(beam.DoFn):

  def __init__(self, chunk_size=1000):
    """Generates key/value pairs so there are no more than n values per key.

    Args:
      chunk_size: Number of values to pair with a given key before generating a
          new key.
      counter: Number of values paired with current key.
      key: Unique id. Initialized with `self.start_bundle` to avoid beam errors.
    """
    # type: (int, int) -> None
    self.chunk_size = chunk_size
    self.counter = 0

  def start_bundle(self):
    # type: () -> None
    self.key = uuid.uuid4()

  def process(self, element):
    # type: (Tuple[ProcessedVariant, str]) -> Tuple[str, Tuple[ProcessedVariant, str]]
    self.counter += 1
    if self.counter == self.chunk_size:
      self.counter = 0
      self.key = uuid.uuid4()
    yield self.key, element


class Client(beam.DoFn):
  """Handles communications with the annotation server.

  This class is responsible for sending data to, and processing responses from
  a service which annotates variants sent to it.
  """
  def __init__(self, url, annotation_headers, vep_info_field):
    # type (str, List[Dict[str, str]], str) -> None
    """

    Args:
      url: Location of annotation server.
      headers: Properly ordered list of annotation header information. e.g.
        [{'name': 'Gene', 'type': 'string', 'desc': 'Ensembl stable ID...'},
         {'name': 'ALLELE_NUM', 'type': 'integer', 'desc': 'allele num...'}]
      vep_info_field: Name of the new INFO field for annotations.
    """
    self.url = url
    self.annotation_headers = annotation_headers
    self.vep_info_field = vep_info_field

  def add_annotation_to_variant(self, variant, annotation):
    """Insert annotation data into ProcessedVariant's alternate_data_list"""
    # TODO(jessime) Adding this data would probably be cleaner if I figure out a
    # way to do this during the original creation of the `ProcessedVariant`.

    # TODO(jessime) The code in this comment is not currently relevant, because
    # `--allele_num` is forced on in VEP. So we can use that to find the correct
    # index in the alt_list. If it becomes an option to turn that off, then we
    # need some other way of calculating the right index.
    # allele_index_in_alt_data = {}
    # for i, alternate_base_data in enumerate(variant.alternate_data_list):
    #   allele_index_in_alt_data[alternate_base_data.alternate_bases] = i
    # alt_list = [a.alternate_bases for a in variant._alternate_datas]
    # parser = annotation_parser.Parser()

    for single_anno in annotation['data']:
      names = [header['name'] for header in self.annotation_headers]
      assert len(names) == len(single_anno)
      named_anno = {}
      incorrect_types = []
      for header, value in zip(self.annotation_headers, single_anno):
        # TODO(jessime) What should be done in the `allele` case?
        if value == '-' and header != 'Allele':
          value = None
        if value is not None:
          try:
            value = TYPE_MAP[header['type']](value)
          except ValueError:
            incorrect_types.append((header['name'], header['type'], value))
        named_anno[header['name']] = value
      if incorrect_types:
        raise ValueError('{}\nhad incorrect types.'.format(incorrect_types))
      # -1 since the first allele num is the reference
      annotation_alt_index = named_anno['ALLELE_NUM'] - 1
      try:
        alternate_base_data = variant.alternate_data_list[annotation_alt_index]
      except:
        variable_str = ('annotation_alt_index: {}; len(alternate_data_list): '
                        '{}; variant start: {}; allele_num: {}').format(
            annotation_alt_index, len(variant.alternate_data_list),
            variant.start, named_anno['ALLELE_NUM'])
        logging.warning(variable_str)
        alternate_base_data = variant.alternate_data_list[0]
      if self.vep_info_field in alternate_base_data.info:
        alternate_base_data.info[self.vep_info_field].append(named_anno)
      else:
        alternate_base_data.annotation_field_names.add(self.vep_info_field)
        alternate_base_data.info[self.vep_info_field] = [named_anno]
    return variant

  def process(self, variants_with_vcf_chunk):
    try:
      variants = variants_with_vcf_chunk[0]
      vcf_chunk = variants_with_vcf_chunk[1]
      response = requests.post(self.url, data={'variants': vcf_chunk})
      try:
        json_response = response.json()
        if json_response['stderr']:
          raise ValueError(json_response['stderr'])
        annotations = json_response['stdout']
      except ValueError:
        # TODO(jessime) This should be more robust. We should look at the
        # text and decide what to do based on the text. For example:
        # if we get the 'wait 30s error, we should do that and try again.
        # If it the request size was too large, we can subdivide the string
        # and send multiple smaller requests. If it's a VEP error, then we
        # can actually abort. Or something like this.
        raise ValueError(response.text)
      # TODO(jessime) This assumes a 1-1 correspondence between the variants and
      # the annotations. What happens if we're missing an annotation, for
      # example?
      for variant, annotation in zip(variants, annotations):
        yield self.add_annotation_to_variant(variant, annotation)
    # NOTE(jessime) This hack lets me see the full traceback, instead of just
    # the last line given by beam.
    except:
      error = traceback.format_exc()
      logging.error(error)
      assert False


# TODO(jessime) `VariantToVepFn` is a half attempt at creating VEP formatted
# strings to pass to the server instead of sending whole vcf formatted lines,
# which contain a lot of unnecessary information.
# The format of VEPs default input can be found here:
# http://useast.ensembl.org/info/docs/tools/vep/vep_formats.html#input

# class VariantToVepFn(beam.DoFn):
#
#   def process(self, variant):
#     try:
#       reference = variant.reference_name.lstrip('chr')
#       start = str(variant.start)
#       end = str(variant.end - 1)
#       strand = '+'
#       for alt in variant.alternate_bases:
#         allele = '/'.join([variant.reference_bases, alt])
#         variant_vep_data = [reference, start, end, allele, strand]
#         if variant.names:
#           variant_vep_data.append(variant.names[0])
#         variant_vep_str = '\t'.join(variant_vep_data)
#         yield variant_vep_str
#     except:
#       print traceback.format_exc()
#       assert False


class PairVariantsLines(beam.DoFn):
  """Creates pairs of ProcessedVariants and their corresponding vcf strings"""

  def __init__(self):
    self._coder = vcfio._ToVcfRecordCoder()

  def process(self, processed_variant):
    # type: (ProcessedVariant) -> Tuple(ProcessedVariant, str)
    vcf_str = self._coder.encode(processed_variant._variant).strip('\n')
    yield processed_variant, vcf_str


class VepRunner(beam.PTransform):
  """Transform for annotating variants.

  Randomly groups variants into chunks, sending batches of vcf lines to the
  annotation server for annotation. Annotations are then stored with their
  corresponding variant.
  """
  def __init__(self, url, annotation_headers, vep_info_field):
    # type (str, List[Dict[str, str]], str) -> None
    """

    Args:
      url: Location of annotation server.
      headers: Properly ordered list of annotation header information. e.g.
        [{'name': 'Gene', 'type': 'string', 'desc': 'Ensembl stable ID...'},
         {'name': 'ALLELE_NUM', 'type': 'integer', 'desc': 'allele num...'}]
      vep_info_field: Name of the new INFO field for annotations.
    """
    self.url = url
    self.annotation_headers = annotation_headers
    self.vep_info_field = vep_info_field

  def expand(self, pcoll):
    variants = (pcoll | 'GenerateKeys' >> beam.ParDo(KeyGenFn())
                      | beam.GroupByKey()
                      | beam.Map(join_lines)
                      | beam.ParDo(Client(self.url,
                                          self.annotation_headers,
                                          self.vep_info_field)))
    return variants


