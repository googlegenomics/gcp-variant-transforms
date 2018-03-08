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

from collections import defaultdict
from typing import Dict, List, Any  # pylint: disable=unused-import

import vcf

from apache_beam.io.gcp.internal.clients import bigquery
from gcp_variant_transforms.beam_io import vcfio
from gcp_variant_transforms.libs import metrics_util
from gcp_variant_transforms.libs import bigquery_util
from gcp_variant_transforms.libs import vcf_header_parser  # pylint: disable=unused-import


_FIELD_COUNT_ALTERNATE_ALLELE = 'A'

# Counter names
class _CounterEnum(enum.Enum):
  VARIANT = 'variant_counter'
  ANNOTATION = 'annotation_counter'
  ANNOTATION_ALT_MISMATCH = 'annotation_alt_mismatch_counter'


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

  def __repr__(self):
    return ', '.join([str(self._alt_bases), str(self._info)])

  def __eq__(self, other):
    return (isinstance(other, AlternateBaseData) and
            vars(self) == vars(other))

  @property
  def alternate_bases(self):
    # type: () -> str
    return self._alt_bases

  @property
  def info(self):
    # type: () -> Dict[str, Any]
    return self._info


class ProcessedVariantFactory(object):
  """Factory class for creating `ProcessedVaraint` instances.

  This is the only right way for creating ProcessedVariants in production code.
  It uses the header information to process INFO fields and split them between
  alternates if needed. In the process, it does some header sanity checking too.
  """
  def __init__(
      self,
      header_fields,  # type: vcf_header_parser.HeaderFields
      split_alternate_allele_info_fields=True,  # type: bool
      annotation_fields=None,  # type: List[str]
      counter_factory=None  # type: metrics_util.CounterFactoryInterface
  ):
    """Sets the internal state of the factory class.

    Args:
      header_fields (:class:`vcf_header_parser.HeaderFields`): Header
        information used for parsing and splitting INFO fields of thei variant.
      split_alternate_allele_info_fields (bool): If True, splits fields with
        field_count='A' (i.e., one value for each alternate) among alternates.
      annotation_fields (List[str]): If provided, this is the list of
        INFO field names that store variant annotations. The format of how
        annotations are stored and their names are extracted from header_fields.
    """
    self._header_fields = header_fields
    self._split_alternate_allele_info_fields = (
        split_alternate_allele_info_fields)
    self._annotation_field_set = set(annotation_fields or [])
    cfactory = counter_factory or metrics_util.NoOpCounterFactory()
    self._variant_counter = cfactory.create_counter(
        _CounterEnum.VARIANT.value)
    self._annotation_counter = cfactory.create_counter(
        _CounterEnum.ANNOTATION.value)
    self._annotation_alt_mismatch_counter = cfactory.create_counter(
        _CounterEnum.ANNOTATION_ALT_MISMATCH.value)

  def create_processed_variant(self, variant):
    # type: (vcfio.Variant) -> ProcessedVariant
    """The main factory method for creating ProcessedVariants.

    Args:
      variant (:class:`vcfio.Variant`): The raw variant information.
    """
    proc_var = ProcessedVariant(variant)
    self._variant_counter.inc()
    for key, variant_info in variant.info.iteritems():
      # TODO(bashir2): field_count should be removed from VariantInfo and
      # instead looked up from header_fields.
      if (self._split_alternate_allele_info_fields and
          variant_info.field_count == _FIELD_COUNT_ALTERNATE_ALLELE):
        self._add_per_alt_info(proc_var, key, variant_info.data)
      elif key in self._annotation_field_set:
        self._add_annotation(proc_var, key, variant_info.data)
      else:
        proc_var._non_alt_info[key] = variant_info.data
    return proc_var

  def _add_per_alt_info(self, proc_var, field_name, variant_info_data):
    # type: (ProcessedVariant, str, vcfio.VariantInfo) -> None
    if len(variant_info_data) != len(proc_var._alternate_datas):
      raise ValueError(
          'Per alternate INFO field {} does not have same cardinality as '
          ' number of alternates: {} vs {}'.format(
              field_name, len(variant_info_data),
              len(proc_var._alternate_datas)))
    for alt_index, info in enumerate(variant_info_data):
      proc_var._alternate_datas[alt_index]._info[field_name] = info

  def _add_annotation(self, proc_var, field_name, data):
    # type: (ProcessedVariant, str, List[str]) -> None
    """Adds an annotation INFO field based on the format in the header.

    Args:
      proc_var (ProcessedVariant): The object to which the annotations are being
        added.
      field_name (str): The name of the annotation field.
      data(List[str]): The data part of the field separated on comma.
    """
    if field_name not in self._header_fields.infos:
      raise ValueError('{} INFO not found in the header, variant: {}'.format(
          field_name, proc_var))
    header_desc = self._header_fields.infos[field_name].desc
    annotation_names = _extract_annotation_names(header_desc)
    alt_annotation_map = self._convert_annotation_strs_to_alt_map(
        annotation_names, data)
    for alt_bases, annotations_list in alt_annotation_map.iteritems():
      # This assumes that number of alternate bases and annotation segments
      # are not too big. If this assumption is not true, we should replace the
      # following loop with a hash table search and avoid the quadratic time.
      for alt in proc_var._alternate_datas:
        if alt.alternate_bases == alt_bases:
          alt._info[field_name] = annotations_list
          self._annotation_counter.inc()
          break
      # TODO(bashir2): Currently we only check exact matches of alternate bases
      # which is not enough. We should implement the whole standard for finding
      # alternate bases for an annotation list.
      else:
        self._annotation_alt_mismatch_counter.inc()
        logging.warning('Could not find matching alternate bases for %s in '
                        'annotation filed %s', alt_bases, field_name)

  def _convert_annotation_strs_to_alt_map(self, annotation_names, field_data):
    # type: (List[str], List[str]) -> Dict[str, List[Dict[str, str]]]
    alt_annotation_map = defaultdict(list)
    for annotation_str in field_data:
      annotations = _extract_annotation_list_with_alt(annotation_str)
      alt_annotation_map[annotations[0]].append(
          self._create_annotation_map(annotations, annotation_names))
    return alt_annotation_map

  def _create_annotation_map(self, annotations, annotation_names):
    # type: (List[str], List[str]) -> Dict[str, str]
    if len(annotation_names) != len(annotations) - 1:
      raise ValueError('Expected {} annotations, got {}'.format(
          len(annotation_names), len(annotations) - 1))
    annotation_dict = {}
    for index, name in enumerate(annotation_names):
      annotation_dict[name] = annotations[index + 1]
    return annotation_dict

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
        if field.num == vcf.parser.field_counts[_FIELD_COUNT_ALTERNATE_ALLELE]:
          alternate_bases_record.fields.append(bigquery.TableFieldSchema(
              name=bigquery_util.get_bigquery_sanitized_field_name(key),
              type=bigquery_util.get_bigquery_type_from_vcf_type(field.type),
              mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
              description=bigquery_util.get_bigquery_sanitized_field(
                  field.desc)))

    for annot_field in self._annotation_field_set:
      if annot_field not in self._header_fields.infos:
        raise ValueError('Annotation field {} not found'.format(annot_field))
      annotation_names = _extract_annotation_names(
          self._header_fields.infos[annot_field].desc)
      annotation_record = bigquery.TableFieldSchema(
          name=bigquery_util.get_bigquery_sanitized_field(annot_field),
          type=bigquery_util.TableFieldConstants.TYPE_RECORD,
          mode=bigquery_util.TableFieldConstants.MODE_REPEATED,
          description='List of {} annotations for this alternate.'.format(
              annot_field))
      for annotation_name in annotation_names:
        annotation_record.fields.append(bigquery.TableFieldSchema(
            name=bigquery_util.get_bigquery_sanitized_field(annotation_name),
            type=bigquery_util.TableFieldConstants.TYPE_STRING,
            mode=bigquery_util.TableFieldConstants.MODE_NULLABLE,
            # TODO(bashir2): Add descriptions of well known annotations, e.g.,
            # from VEP.
            description=''))
      alternate_bases_record.fields.append(annotation_record)
    return alternate_bases_record

  def info_is_in_alt_bases(self, info_field_name):
    # type: (str) -> bool
    if info_field_name not in self._header_fields.infos:
      raise ValueError('INFO field {} not found'.format(info_field_name))
    is_per_alt_info = (
        self._split_alternate_allele_info_fields and
        self._header_fields.infos[info_field_name].num ==
        vcf.parser.field_counts[_FIELD_COUNT_ALTERNATE_ALLELE])
    is_annotation = info_field_name in self._annotation_field_set
    return is_per_alt_info or is_annotation


def _extract_annotation_list_with_alt(annotation_str):
  # type: (str) -> List[str]
  """Extracts annotations from an annotation INFO field.

  This works by dividing the `annotation_str` on '|'. The first element is
  the alternate allele and the rest are the annotations. For example, for
  'G|upstream_gene_variant|MODIFIER|PSMF1' as `annotation_str`, it returns
  ['G', 'upstream_gene_variant', 'MODIFIER', 'PSMF1'].

  Args:
    annotation_str: The content of annotation field for one alt.

  Returns:
    The list of annotations with the first element being the alternate.
  """
  return annotation_str.split('|')


def _extract_annotation_names(description):
  # type: (str) -> List[str]
  """Extracts annotation list from the description of an annotation INFO field.

  This is similar to extract_extract_annotation_list_with_alt with the
  difference that it ignores everything before the first '|'. For example, for
  'some desc ... Format: Allele|Consequence|IMPACT|SYMBOL|Gene', it returns
  ['Consequence', 'IMPACT', 'SYMBOL', 'Gene']

  Args:
    description: The "Description" part of the annotation INFO field
      in the header of VCF.

  Returns:
    The list of annotation names.
  """
  annotation_names = _extract_annotation_list_with_alt(description)
  if len(annotation_names) < 2:
    raise ValueError(
        'Expected at least one | in annotation description {}'.format(
            description))
  return annotation_names[1:]
