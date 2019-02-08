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

"""Processes raw variant information using header information.

Note that for creating instances of the data objects in this module, there is a
factory function create_processed_variant. Other than that function, these
objects should be used as non-mutable in other scopes, hence all mutating
functions are "private".
"""

from __future__ import absolute_import

import enum
import logging

from typing import Any, Dict, List, Set  # pylint: disable=unused-import

import vcf

from apache_beam.io.gcp.internal.clients import bigquery

from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.beam_io import vcf_header_io
from gcp_variant_transforms.libs import metrics_util
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import bigquery_sanitizer
from gcp_variant_transforms.libs import infer_headers_util
from gcp_variant_transforms.libs.annotation import annotation_parser
from gcp_variant_transforms.libs.annotation.vep import descriptions

_FIELD_COUNT_ALTERNATE_ALLELE = 'A'

# An alias for the header key constants to make referencing easier.
_HeaderKeyConstants = vcf_header_io.VcfParserHeaderKeyConstants
_BigQuerySchemaSanitizer = bigquery_sanitizer.SchemaSanitizer

# Map for casting values with VcfHeaderFieldTypeConstants to Python types.
VCF_TYPE_TO_PY = {vcf_header_io.VcfHeaderFieldTypeConstants.STRING: str,
                  vcf_header_io.VcfHeaderFieldTypeConstants.FLOAT: float,
                  vcf_header_io.VcfHeaderFieldTypeConstants.INTEGER: int}

# Counter names
class _CounterEnum(enum.Enum):
  VARIANT = 'variant_counter'
  ALTERNATE_ALLELE_INFO_MISMATCH = 'alternate_allele_info_mismatch_counter'
  ANNOTATION_ALT_MATCH = 'annotation_alt_match_counter'
  ANNOTATION_ALT_MINIMAL_AMBIGUOUS = 'annotation_alt_minimal_ambiguous_counter'
  ANNOTATION_ALT_MISMATCH = 'annotation_alt_mismatch_counter'
  ALLELE_NUM_MISSING = 'allele_num_missing'
  ALLELE_NUM_INCORRECT = 'allele_num_incorrect'


class ProcessedVariant(object):
  """A wrapper around the ``Variant`` class with extra functionality.

  Given header file information, this can parse INFO fields that need to be
  split and attached to alternates. This is not inherited from
  :class:``vcfio.Variant`` as an encapsulation layer and to prefer composition.
  """

  def __init__(self, variant):
    # type: (vcfio.Variant) -> None
    if not isinstance(variant, vcfio.Variant):
      raise ValueError('Expected an instance of vcfio.Variant.')
    self._variant = variant
    self._non_alt_info = {}  # type: Dict[str, Any]
    self._alternate_datas = []  # type: List[AlternateBaseData]
    for a in variant.alternate_bases:
      self._alternate_datas.append(AlternateBaseData(a))

  def __repr__(self):
    return ', '.join(
        [str(s) for s in [
            self._variant,
            self._non_alt_info,
            self._alternate_datas]])

  def __eq__(self, other):
    return (isinstance(other, ProcessedVariant) and
            vars(self) == vars(other))

  @property
  def reference_name(self):
    # type: () -> str
    return self._variant.reference_name

  @property
  def start(self):
    # type: () -> int
    return self._variant.start

  @property
  def end(self):
    # type: () -> int
    return self._variant.end

  @property
  def reference_bases(self):
    # type: () -> str
    return self._variant.reference_bases

  @property
  def names(self):
    # type: () -> List[str]
    return self._variant.names

  @property
  def quality(self):
    # type: () -> float
    return self._variant.quality

  @property
  def filters(self):
    # type: () -> List[str]
    return self._variant.filters

  @property
  def calls(self):
    # type: () -> List[vcfio.VariantCall]
    return self._variant.calls

  @property
  def non_alt_info(self):
    # type: () -> Dict[str, Any]
    """Returns the INFO fields that are not alternate base specific.

    The type of the values in the map is specified in the VCF header. The values
    are copied from the `vcfio.VariantIfno.data` fields of the input variants.
    """
    return self._non_alt_info

  @property
  def alternate_data_list(self):
    # type: () -> List[AlternateBaseData]
    return self._alternate_datas


class AlternateBaseData(object):
  """This is to keep all information for a single alternate-bases."""

  def __init__(self, alt_bases):
    # type: (str) -> None
    """
    Args:
      alt_bases(str): The alternate bases string for this instance.
    """
    self._alt_bases = alt_bases
    # Note that `_info` also holds the split annotation fields. For those
    # fields, the value in the `_info` dict has a list of dicts itself.
    self._info = {}  # type: Dict[str, Any]
    self._annotation_field_names = set()  # type: Set[str]

  def __repr__(self):
    return ', '.join([str(self._alt_bases), str(self._info)])

  def __eq__(self, other):
    return (isinstance(other, AlternateBaseData) and
            self._alt_bases == other._alt_bases and
            self._info == other._info)

  @property
  def alternate_bases(self):
    # type: () -> str
    return self._alt_bases

  @property
  def info(self):
    # type: () -> Dict[str, Any]
    return self._info

  @property
  def annotation_field_names(self):
    # type: () -> Set[str]
    return self._annotation_field_names


class ProcessedVariantFactory(object):
  """Factory class for creating `ProcessedVariant` instances.

  This is the only right way for creating ProcessedVariants in production code.
  It uses the header information to process INFO fields and split them between
  alternates if needed. In the process, it does some header sanity checking too.
  """
  def __init__(
      self,
      header_fields,  # type: vcf_header_io.VcfHeader
      split_alternate_allele_info_fields=True,  # type: bool
      allow_alternate_allele_info_mismatch=False,  # type: bool
      annotation_fields=None,  # type: List[str]
      use_allele_num=False,  # type: bool
      minimal_match=False,  # type: bool
      infer_annotation_types=False,  # type: bool
      counter_factory=None  # type: metrics_util.CounterFactoryInterface
  ):
    # type: (...) -> None
    """Sets the internal state of the factory class.

    Args:
      header_fields: Header information used for parsing and splitting INFO
        fields of the variant.
      split_alternate_allele_info_fields: If True, splits fields with
        `field_count='A'` (i.e., one value for each alternate) among alternates.
      allow_alternate_allele_info_mismatch: By default (when False), an error
        will be raised for INFO fields with `field_count='A'` (i.e. one value
        for each alternate base) that do not have the same cardinality as
        alternate bases. If True, an error will not be raised and excess values
        will be dropped or insufficient values will be set to null. Only
        applicable if `split_alternate_allele_info_fields` is True.
      annotation_fields: If provided, this is the list of INFO field names that
        store variant annotations. The format of how annotations are stored and
        their names are extracted from header_fields.
      use_allele_num: If True, then "ALLELE_NUM" annotation is used to determine
        the index of the ALT that corresponds to an annotation set.
      minimal_match: If True, then the --minimal mode of VEP is simulated for
        annotation ALT matching.
      infer_annotation_types: If True, then warnings will be provided if header
        fields fail to contain Info type lines for annotation fields
      counter_factory: If provided, it will be used to record counters (e.g. the
        number of variants processed).
    """
    self._header_fields = header_fields
    self._split_alternate_allele_info_fields = (
        split_alternate_allele_info_fields)
    self._allow_alternate_allele_info_mismatch = (
        allow_alternate_allele_info_mismatch)
    self._annotation_field_set = set(annotation_fields or [])
    cfactory = counter_factory or metrics_util.NoOpCounterFactory()
    self._variant_counter = cfactory.create_counter(
        _CounterEnum.VARIANT.value)
    self._alternate_allele_info_mismatche_counter = cfactory.create_counter(
        _CounterEnum.ALTERNATE_ALLELE_INFO_MISMATCH.value)
    self._annotation_processor = _AnnotationProcessor(
        annotation_fields, self._header_fields, cfactory, use_allele_num,
        minimal_match, infer_annotation_types)
    self._minimal_match = minimal_match
    self._infer_annotation_types = infer_annotation_types

  def create_processed_variant(self, variant):
    # type: (vcfio.Variant) -> ProcessedVariant
    """The main factory method for creating ProcessedVariants.

    Args:
      variant (:class:`vcfio.Variant`): The raw variant information.
    """
    proc_var = ProcessedVariant(variant)
    self._variant_counter.inc()
    for key, variant_info_data in variant.info.iteritems():
      if key in self._annotation_field_set:
        self._annotation_processor.add_annotation_data(
            proc_var, key, variant_info_data)
      elif self._is_per_alt_info_field(key):
        self._add_per_alt_info(proc_var, key, variant_info_data)
      else:
        proc_var._non_alt_info[key] = variant_info_data
    return proc_var

  def _add_per_alt_info(self, proc_var, field_name, variant_info_data):
    # type: (ProcessedVariant, str, vcfio.VariantInfo) -> None
    num_variant_infos = len(variant_info_data)
    num_alternate_bases = len(proc_var._alternate_datas)
    if num_variant_infos != num_alternate_bases:
      error_message = (
          'Per alternate INFO field "{}" does not have same cardinality as '
          'number of alternates: {} vs {} in variant: "{}"'.format(
              field_name, num_variant_infos, num_alternate_bases, proc_var))
      self._alternate_allele_info_mismatche_counter.inc()
      if self._allow_alternate_allele_info_mismatch:
        logging.warning(error_message)
      else:
        raise ValueError(error_message)
    for alt_index in range(min(num_variant_infos, num_alternate_bases)):
      proc_var._alternate_datas[alt_index]._info[field_name] = (
          variant_info_data[alt_index])

  def create_alt_bases_field_schema(self):
    # type: () -> bigquery.TableFieldSchema
    """Returns the alternate_bases record compatible with this factory.

    Depending on how this class is set up to split INFO fields among alternate
    bases, this function produces a compatible alternate_bases record and
    returns it which can be added to a bigquery schema by the caller.
    """
    alternate_bases_record = bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.ALTERNATE_BASES,
        type=bigquery_util.TableFieldConstants.TYPE_RECORD,
        mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
        description='One record for each alternate base (if any).')
    alternate_bases_record.fields.append(bigquery.TableFieldSchema(
        name=bigquery_util.ColumnKeyConstants.ALTERNATE_BASES_ALT,
        type=bigquery_util.TableFieldConstants.TYPE_STRING,
        mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
        description='Alternate base.'))
    if self._split_alternate_allele_info_fields:
      for key, field in self._header_fields.infos.iteritems():
        if (field[_HeaderKeyConstants.NUM] ==
            vcf.parser.field_counts[_FIELD_COUNT_ALTERNATE_ALLELE]):
          alternate_bases_record.fields.append(bigquery.TableFieldSchema(
              name=_BigQuerySchemaSanitizer.get_sanitized_field_name(key),
              type=bigquery_util.get_bigquery_type_from_vcf_type(
                  field[_HeaderKeyConstants.TYPE]),
              mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
              description=_BigQuerySchemaSanitizer.get_sanitized_string(
                  field[_HeaderKeyConstants.DESC])))

    for annot_field in self._annotation_field_set:
      if annot_field not in self._header_fields.infos:
        raise ValueError('Annotation field {} not found'.format(annot_field))
      annotation_descs = descriptions.VEP_DESCRIPTIONS
      annotation_record = bigquery.TableFieldSchema(
          name=_BigQuerySchemaSanitizer.get_sanitized_field_name(annot_field),
          type=bigquery_util.TableFieldConstants.TYPE_RECORD,
          mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
          description='List of {} annotations for this alternate.'.format(
              annot_field))
      annotation_record.fields.append(bigquery.TableFieldSchema(
          name=annotation_parser.ANNOTATION_ALT,
          type=bigquery_util.TableFieldConstants.TYPE_STRING,
          mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
          description='The ALT part of the annotation field.'))
      annotation_names_keys = self._gen_annotation_name_key_pairs(annot_field)
      for annotation_name, type_key in annotation_names_keys:
        if type_key in self._header_fields.infos:
          vcf_type = self._header_fields.infos[type_key][
              vcf_header_io.VcfParserHeaderKeyConstants.TYPE]
        else:
          vcf_type = vcf_header_io.VcfHeaderFieldTypeConstants.STRING
          if self._infer_annotation_types:
            logging.warning(('Annotation field %s has no corresponding header '
                             'field with id %s to specify type. Using type %s '
                             'instead.'), annotation_name, type_key, vcf_type)
        annotation_record.fields.append(bigquery.TableFieldSchema(
            name=_BigQuerySchemaSanitizer.get_sanitized_field_name(
                annotation_name),
            type=bigquery_util.get_bigquery_type_from_vcf_type(vcf_type),
            mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
            description=annotation_descs.get(annotation_name, '')))
      alternate_bases_record.fields.append(annotation_record)
    return alternate_bases_record

  def _gen_annotation_name_key_pairs(self, annot_field):
    #  type: (str) -> (str, str)
    annotation_names = annotation_parser.extract_annotation_names(
        self._header_fields.infos[annot_field][_HeaderKeyConstants.DESC])
    for name in annotation_names:
      type_key = infer_headers_util.get_inferred_annotation_type_header_key(
          annot_field, name)
      yield name, type_key

  def gen_annotation_info_type_keys(self):
    #  type: () -> str
    """Generates all possible key IDs for annotation type info fields.

    Yields:
      type_key: IDs for info fields added during inferring annotation types. For
        example, if annotations fields are ('CSQ', 'CSQ_VT'), and names are
        ['Gene', 'Impact'], this will yield ('CSQ_Gene_TYPE', 'CSQ_Impact_TYPE',
        'CSQ_VT_Gene_TYPE', 'CSQ_VT_Impact_TYPE').
    """
    for annot_field in self._annotation_field_set:
      for _, type_key in self._gen_annotation_name_key_pairs(annot_field):
        yield type_key

  def info_is_in_alt_bases(self, info_field_name):
    # type: (str) -> bool
    if info_field_name not in self._header_fields.infos:
      raise ValueError('INFO field {} not found'.format(info_field_name))
    is_per_alt_info = self._is_per_alt_info_field(info_field_name)
    is_annotation = info_field_name in self._annotation_field_set
    return is_per_alt_info or is_annotation

  def _is_per_alt_info_field(self, info_field_name):
    # type: (str) -> bool
    """Returns true iff `info_field_name` is defined as having Number=A."""
    return (
        self._split_alternate_allele_info_fields and
        info_field_name in self._header_fields.infos and
        self._header_fields.infos[info_field_name][_HeaderKeyConstants.NUM] == (
            vcf.parser.field_counts[_FIELD_COUNT_ALTERNATE_ALLELE]))


class _AnnotationProcessor(object):
  """This is for handling all annotation related logic for variants."""


  def __init__(self,
               annotation_fields,  # type: List[str]
               header_fields,  # type: vcf_header_io.VcfHeader
               counter_factory,  # type: metrics_util.CounterFactoryInterface
               use_allele_num,  # type: bool
               minimal_match,  # type: bool
               infer_annotation_types,  # type: bool
              ):
    # type: (...) -> None
    """Creates an instance for adding annotations to `ProcessedVariant` objects.

    Note this class is intended to be an auxiliary for ProcessedVariantFactory
    and is used for creating annotation related parts of a `ProcessedVariant`
    object. So it is an implementation detail and not part of the public API.

    Args:
      annotation_fields: The list of INFO field names that store variant
        annotations. The format of how annotations are stored and their names
        are extracted from header_fields.
      header_fields: The VCF header information.
      infer_annotation_types: If set, then warnings will be provided if header
        fields fail to contain Info type lines for annotation fields
    """
    self._header_fields = header_fields
    self._annotation_names_map = {}  # type: Dict[str, List[str]]
    for field in annotation_fields or []:
      if field not in header_fields.infos:
        raise ValueError('{} INFO not found in the header'.format(field))
      header_desc = header_fields.infos[field][_HeaderKeyConstants.DESC]
      self._annotation_names_map[field] = (
          annotation_parser.extract_annotation_names(header_desc))
    self._alt_match_counter = counter_factory.create_counter(
        _CounterEnum.ANNOTATION_ALT_MATCH.value)
    self._alt_minimal_ambiguous_counter = counter_factory.create_counter(
        _CounterEnum.ANNOTATION_ALT_MINIMAL_AMBIGUOUS.value)
    self._alt_mismatch_counter = counter_factory.create_counter(
        _CounterEnum.ANNOTATION_ALT_MISMATCH.value)
    self._allele_num_missing_counter = counter_factory.create_counter(
        _CounterEnum.ALLELE_NUM_MISSING.value)
    self._allele_num_incorrect_counter = counter_factory.create_counter(
        _CounterEnum.ALLELE_NUM_INCORRECT.value)
    self._use_allele_num = use_allele_num
    self._minimal_match = minimal_match
    self._infer_annotation_types = infer_annotation_types

  def add_annotation_data(self, proc_var, annotation_field_name, data):
    # type: (ProcessedVariant, str, List[str]) -> None
    """The main function for adding annotation data to `proc_var`.

    This adds the data for annotation INFO field `annotation_field_name` based
    on the format specified for it in the header. `data` items are split
    among `proc_var._alternate_datas` based on the ALT matching logic.

    The only assumption about `proc_var` is that its `_alternate_datas`
    has been initialized with valid `AlternateBaseData` objects.

    Args:
      proc_var: The object to which the annotations are being added.
      annotation_field_name: The name of the annotation field, e.g., ANN or CSQ.
      data: The data part of the field separated on comma. A single element
        of this list looks something like (taken from an Ensembl VEP run):

        G|upstream_gene_variant|MODIFIER|PSMF1|ENSG00000125818|...

        where the '|' character is the separator. The first element is a way
        to identify the allele (one of the ALTs) that this annotation data
        refers to. The rest of the elements are annotations corresponding to the
        `annotation_field_name` format description in the header, e.g.,

        Allele|Consequence|IMPACT|SYMBOL|Gene|...
    """
    alt_list = [a.alternate_bases for a in proc_var._alternate_datas]
    parser = annotation_parser.Parser(
        proc_var.reference_bases, alt_list,
        self._annotation_names_map[annotation_field_name], self._use_allele_num,
        self._minimal_match)
    for annotation_str in data:
      try:
        ind, annotation_map = parser.parse_and_match_alt(annotation_str)
        for name, value in annotation_map.iteritems():
          if name == annotation_parser.ANNOTATION_ALT:
            continue
          type_key = infer_headers_util.get_inferred_annotation_type_header_key(
              annotation_field_name, name)
          vcf_type = self._vcf_type_from_annotation_header(
              annotation_field_name, type_key)
          typed_value = VCF_TYPE_TO_PY[vcf_type](value) if value else None
          annotation_map[name] = typed_value
        self._alt_match_counter.inc()
        alt_datas = proc_var._alternate_datas[ind]
        if annotation_field_name not in alt_datas._info:
          alt_datas._info[annotation_field_name] = [annotation_map]
        else:
          alt_datas._info[annotation_field_name].append(annotation_map)
        alt_datas.annotation_field_names.add(annotation_field_name)
      except annotation_parser.AnnotationParserException as e:
        logging.warning(
            'Parsing of annotation field %s failed at reference %s start %d: '
            '%s', annotation_field_name, proc_var.reference_name,
            proc_var.start, str(e))
        if isinstance(e, annotation_parser.AnnotationAltNotFound):
          self._alt_mismatch_counter.inc()
        elif isinstance(e, annotation_parser.AlleleNumMissing):
          self._allele_num_missing_counter.inc()
        elif isinstance(e, annotation_parser.InvalidAlleleNumValue):
          self._allele_num_incorrect_counter.inc()
        elif isinstance(e, annotation_parser.AmbiguousAnnotationAllele):
          self._alt_minimal_ambiguous_counter.inc()

  def _vcf_type_from_annotation_header(self, annotation_name, type_key):
    # type: (str, str) -> str
    if type_key in self._header_fields.infos:
      vcf_type = self._header_fields.infos[type_key][_HeaderKeyConstants.TYPE]
    else:
      vcf_type = vcf_header_io.VcfHeaderFieldTypeConstants.STRING
      if self._infer_annotation_types:
        logging.warning(('Annotation field %s has no corresponding header '
                         'field with id %s to specify type. Using type %s '
                         'instead.'), annotation_name, type_key, vcf_type)
    return vcf_type
