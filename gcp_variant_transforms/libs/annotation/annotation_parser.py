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

"""Module for parsing annotation fields.

The main class is `Parser`; see its documentation for usage. There are also
some helper methods that can be used in different contexts.
"""


import re

from typing import Dict, Iterable, List, Tuple  # pylint: disable=unused-import

# The key in annotation maps that keeps the original alternate allele in an
# annotation string (and the field name in the BigQuery table that holds that
# information).
ANNOTATION_ALT = 'allele'

# The annotation field that VEP uses to record the index of the alternate
# allele (i.e., ALT) that an annotation list is for.
_ALLELE_NUM_ANNOTATION = 'ALLELE_NUM'

# The representation of a deletion variant in VEP.
_COMPLETELY_DELETED_ALT = '-'
_MISSING_ANNOTATION_FIELD_VALUE = ''
# Regular expressions to identify symbolic and breakend ALTs used in
# annotation alt matching.
# Check the VCF spec for symbolic and breakend ALT formats.
_SYMBOLIC_ALT_RE = re.compile(r'^<(?P<ID>.*)>$')
_BREAKEND_ALT_RE = (re.compile(
    r'^(?P<up_to_chr>.*([\[\]]).*):(?P<pos>.*)([\[\]]).*$'))


class AnnotationParserException(Exception):
  pass


class AlleleNumMissing(AnnotationParserException):
  pass


class InvalidAlleleNumValue(AnnotationParserException):
  pass


class AmbiguousAnnotationAllele(AnnotationParserException):
  pass


class AnnotationAltNotFound(AnnotationParserException):
  pass


class Parser():
  """The main class for parsing annotation fields of a single variant record.

  The expected usage is to pass information about one variant record, namely the
  reference sequence and list of alternate allele sequences, plus some metadata
  like list of annotation names (coming from VCF headers),  and how to do allele
  matching, then call `parse_and_match_alt` method for each annotation string
  of that variant.
  """

  def __init__(
      self,
      reference,  # type: str
      alt_list,  # type: List[str]
      annotation_names,  # type: List[str]
      use_allele_num,  # type: bool
      do_minimal_match  # type: bool
  ):
    """Constructs an instance for parsing annotations of one variant record.

    In the variable name choices and documentations, when it says "alt" or "ALT"
    it refers to the alternate alleles of the variant. If alternate allele part
    of an annotation string is meant, "annotation_alt" or "annotation ALT" is
    used.

    Args:
      reference: The reference sequence, e.g., 'AT'
      alt_list: List of alternate alleles, e.g., ['ATT', 'C']
      annotation_names: List of expected annotations, e.g., ['Consequence',
        'IMPACT', 'SYMBOL'].
      use_allele_num: Whether to use 'ALLELE_NUM' annotation for ALT matching.
      do_minimal_match: Whether to simulate the minimal mode of VEP.
    """
    self._reference = reference or ''
    self._alt_list = alt_list or []
    self._annotation_names = annotation_names or []
    self._use_allele_num = use_allele_num
    self._do_minimal_match = do_minimal_match
    self._common_prefix = self._find_common_alt_ref_prefix_char(
        reference, alt_list)

  def _find_common_alt_ref_prefix_char(self, ref, alt_list):
    # type: (str, List[str]) -> str
    if not ref:
      return ''
    common_char = ref[0]
    for alt in alt_list:
      if not alt or alt[0] != common_char:
        return ''
    return common_char

  def parse_and_match_alt(self, annotation_str):
    # type: (str) -> Tuple[int, Dict[str, str]]
    """Parses the given annotation string and returns the matching ALT index.

    Args:
      annotation_str: The annotation string to parse, e.g.,
        'ATT|upstream_gene_variant|MODIFIER|PSMF1'

    Returns:
      A tuple consisting of an integer and a map. The integer is the index of
        the ALT that matches the given annotation string. For the above example
        and an instance constructed with the example in `__init__`, the returned
        index is 0 since 'ATT' is the first ALT. The returned map is:
        { 'Consequence': 'upstream_gene_variant',
          'IMPACT': 'MODIFIER',
          'SYMBOL': 'PSMF1'}

    Raises:
      AlleleNumMissing: When matching by 'ALLELE_NUM' but that field is not
        present in the given `annotation_str`.
      AmbiguousAnnotationAllele: If simulating minimal mode and the allele part
        of `annotation_str` matches more than one of variant ALTs.
      AnnotationAltNotFound: If the allele part of `annotation_str` does not
        match any of the variant ALTs.
      InvalidAlleleNumValue: When matching by 'ALLELE_NUM' but the value in that
        field is wrong, e.g., not a number or out of range of ALTs.
      ValueError: In a few other error cases, the exception message should be
        descriptive enough.
    """
    annotations = extract_annotation_list_with_alt(annotation_str)
    annotation_map = self._create_map(annotations)
    alt_ind = self._find_alt_index(annotation_map)
    return alt_ind, annotation_map

  def _create_map(self, annotations):
    # type: (List[str], List[str]) -> Dict[str, str]
    if len(self._annotation_names) != len(annotations) - 1:
      raise ValueError('Expected {} annotations, got {}'.format(
          len(self._annotation_names), len(annotations) - 1))
    annotation_dict = {}
    annotation_dict[ANNOTATION_ALT] = annotations[0]
    for index, name in enumerate(self._annotation_names):
      annotation_dict[name] = annotations[index + 1]
    return annotation_dict

  def _find_alt_index(self, annotation_map):
    # type: (Dict[str, str]) -> int
    if self._use_allele_num:
      return self._find_alt_index_by_allele_num(annotation_map)
    else:
      annotation_alt = annotation_map[ANNOTATION_ALT]
      return self._find_matching_alt_index(annotation_alt)

  def _find_matching_alt_index(self, annotation_alt):
    # type: (str) -> int
    """Searches among ALTs to find one that matches `annotation_alt`.

    Args:
      annotation_alt: The ALT part of annotation data, e.g., 'ATT'. Note that
        this is not necessarily equal to an ALT string of the original variant
        as the matching rules are not always exact match.

    Returns:
      The index in the variant ALT list (`self._alt_list`) that matches
        `annotation_alt`.

    Raises:
      AnnotationAltNotFound: If the given `annotation_alt` does not match any of
        the ALTs.
      AmbiguousAnnotationAllele: If `self._do_minimal_match` and the given
        `annotation_alt` matches more than one ALT.
    """
    # This assumes that number of alternate bases and annotation segments
    # are not too big. If this assumption is not true, we should replace the
    # following loop with a hash table search and avoid the quadratic time.
    for ind, alt in enumerate(self._alt_list):
      if self._alt_matches_annotation_alt(alt, annotation_alt):
        return ind
    found_alt_ind = -1
    if self._do_minimal_match:
      for ind, alt in enumerate(self._alt_list):
        if self._alt_matches_annotation_alt_minimal_mode(alt, annotation_alt):
          if found_alt_ind >= 0:
            raise AmbiguousAnnotationAllele(
                'Annotation ALT {} matches both ALTs {} and {} '
                'with reference bases {}'.format(
                    annotation_alt, self._alt_list[found_alt_ind], alt,
                    self._reference))
          found_alt_ind = ind
          # Note we do not `break` in this case because we want to know if this
          # match was an ambiguous match or an exact one.
    if found_alt_ind < 0:
      raise AnnotationAltNotFound(
          'Matching alternate bases for annotation ALT {} not found'.format(
              annotation_alt))
    return found_alt_ind

  def _alt_matches_annotation_alt(self, alt, annotation_alt):
    # type: (str, str) -> bool
    """Returns true if `alt` matches `annotation_alt`

    See the "VCF" and "Complex VCF entries" sections of
    https://ensembl.org/info/docs/tools/vep/vep_formats.html
    for details of prefix matching and indels. Some examples:
    REF      ALT         annotation-ALT
    A        T           T
    AT       ATT,A       TT,-
    A        <ID>        ID
    A        .[13:123[   .[13
    """
    if not self._do_minimal_match:
      # Check equality without the common prefix.
      # Note according to VCF spec the length of this common prefix should be
      # at most one. This string matching is skipped if in minimal_match mode.
      # TODO(bashir2): This is a VEP specific issue and should be updated once
      # we need to import annotations generated by other programs.
      if alt[len(self._common_prefix):] == annotation_alt:
        return True
      # Handling deletion.
      if (len(self._common_prefix) == len(alt)
          and annotation_alt == _COMPLETELY_DELETED_ALT):
        return True
    # Handling symbolic ALTs.
    id_match = _SYMBOLIC_ALT_RE.match(alt)
    if id_match and id_match.group('ID') == annotation_alt:
      return True
    # Handling breakend ALTs.
    # TODO(bashir2): Check if the following logic is documented anywhere! I
    # could not find it explicitly in any documentation but that's how I saw
    # VEP does it in some examples I ran.
    breakend_match = _BREAKEND_ALT_RE.match(alt)
    if breakend_match and breakend_match.group('up_to_chr') == annotation_alt:
      return True
    return False

  def _alt_matches_annotation_alt_minimal_mode(self, alt, annotation_alt):
    # type: (str, str) -> bool
    """Returns true if ALTs match in the --minimal mode of VEP.

    Note in the minimal mode, the matching can be non-deterministic, so this
    should only be done if _alt_matches_annotation_alt which is deterministic
    has not succeeded. For details of ALT matching in the --minimal mode of VEP,
    see the "Complex VCF entries" sections of
    https://useast.ensembl.org/info/docs/tools/vep/vep_formats.html
    Basically, each ALT is independently checked with REF and the common prefix
    and suffix is removed from ALT. The remaining part is the annotation ALT:
    REF      ALT         annotation-ALT
    A        T           T
    AT       TT,A        T,-
    C        CT,T        T               -> Note this is ambiguous.
    """
    if not alt or not annotation_alt:
      return False
    # Finding common leading and trailing sub-strings of ALT and REF.
    leading = 0
    trailing = 0
    min_len = min(len(alt), len(self._reference))
    while (leading < min_len and
           alt[leading] == self._reference[leading]):
      leading += 1
    while (trailing + leading < min_len and  # TODO check this condition
           alt[len(alt) - trailing - 1] ==
           self._reference[len(self._reference) - trailing - 1]):
      trailing += 1
    if alt[leading:len(alt) - trailing] == annotation_alt:
      return True
    if (leading + trailing == len(alt) and
        annotation_alt == _COMPLETELY_DELETED_ALT):
      return True
    return False

  def _find_alt_index_by_allele_num(self, annotation_map):
    # type: (Dict[str, str]) -> int
    if _ALLELE_NUM_ANNOTATION not in annotation_map:
      raise AlleleNumMissing
    index_str = annotation_map[_ALLELE_NUM_ANNOTATION]
    try:
      alt_index = int(index_str) - 1
      if alt_index >= len(self._alt_list) or alt_index < 0:
        raise InvalidAlleleNumValue('{} out of ALT range [{}, {}]'.format(
            alt_index + 1, 1, len(self._alt_list)))
      return alt_index
    except ValueError as e:
      raise InvalidAlleleNumValue(e) from e


class AnnotationStrBuilder():
  """The class for reconstructing annotation str."""

  def __init__(self, annotation_id_to_annotation_names):
    # type: (Dict[str, List[str]]) -> None
    """Initializes an object of `AnnotationStrBuilder`.

    Args:
      annotation_id_to_annotation_names: A map where the key is the annotation
        id (e.g., `CSQ`) and the value is a list of annotation names (e.g.,
        ['allele', 'Consequence', 'IMPACT', 'SYMBOL']). The annotation str
        (e.g., 'A|upstream_gene_variant|MODIFIER|PSMF1|||||') is reconstructed
        in the same order as the annotation names.
    """
    self._annotation_id_to_annotation_names = annotation_id_to_annotation_names

  def reconstruct_annotation_str(self, annotation_id, annotation_maps):
    # type: (str, List[Dict[str, Any]]) -> Iterable[str]
    """Yields annotation string reconstructed from `annotation_map`.

    Notice that the returned value is a list of annotation string since the
    annotation record is a repeated field. The annotation str (e.g.,
    'A|upstream_gene_variant|MODIFIER|PSMF1|||||') is
    reconstructed in the same order as `annotation_names`.

    Args:
      annotation_id: The annotation id of the `annotation_maps`. Example: 'CSQ'.
      annotation_maps: A list of annotation map to be reconstructed to
        annotation str. Example:
        [{'allele': 'G',
          'Consequence': 'upstream_gene_variant',
          'AF': ''
          'IMPACT': 'MODIFIER'},
         {'allele': 'G',
          'Consequence': 'upstream_gene_variant',
          'AF': '0.1',
          'IMPACT': ''}
        ]

    Yields:
      Annotation string reconstructed from `annotation_maps`.
        Example: 'G|upstream_gene_variant||MODIFIER'
    """
    if not self.is_valid_annotation_id(annotation_id):
      raise ValueError('No annotation names for {} are defined. The '
                       'annotation string cannot be reconstructed since the '
                       'order is not provided.'.format(annotation_id))
    for annotation_map in annotation_maps:
      annotation_values = []
      for annotation_name in self._annotation_id_to_annotation_names.get(
          annotation_id):
        value = annotation_map.get(annotation_name)
        if value is None:
          annotation_values.append(_MISSING_ANNOTATION_FIELD_VALUE)
        else:
          annotation_values.append(str(value))
      yield '|'.join(annotation_values)

  def is_valid_annotation_id(self, key):
    # type: (str) -> bool
    """Returns true if the key is a valid annotation id.

    The key is a valid annotation id only when the corresponding annotation
    names are given such that the annotation string can be reconstructed.
    """
    return (self._annotation_id_to_annotation_names and
            key in list(self._annotation_id_to_annotation_names.keys()))


def extract_annotation_list_with_alt(annotation_str):
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


def extract_annotation_names(description):
  # type: (str) -> List[str]
  """Extracts annotation list from the description of an annotation INFO field.

  This is similar to extract_annotation_list_with_alt with the difference
  that it ignores everything before the first '|'. For example, for
  'some desc ... Format: Allele|Consequence|IMPACT|SYMBOL|Gene', it returns
  ['Consequence', 'IMPACT', 'SYMBOL', 'Gene'].

  Args:
    description: The "Description" part of the annotation INFO field
      in the header of VCF.

  Returns:
    The list of annotation names.
  """
  annotation_names = extract_annotation_list_with_alt(description)
  if len(annotation_names) < 2:
    raise ValueError(
        'Expected at least one | in annotation description {}'.format(
            description))
  return annotation_names[1:]


def reconstruct_annotation_description(annotation_names):
  # type: (List[str]) -> str
  """Reconstructs annotation description.

  For example, given ['Allele', 'Consequence', 'IMPACT', 'SYMBOL', 'Gene'], it
  returns 'Format: Allele|Consequence|IMPACT|SYMBOL|Gene'.
  """
  return ' '.join(['Format:', '|'.join(annotation_names)])
