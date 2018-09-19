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

"""A PTransform to convert BigQuery table rows to a PCollection of `Variant`."""

from typing import Dict, List  # pylint: disable=unused-import

import apache_beam as beam

from gcp_variant_transforms.libs import bigquery_vcf_data_converter


class BigQueryToVariant(beam.PTransform):
  """Transforms BigQuery table rows to PCollection of `Variant`."""

  def __init__(self, annotation_id_to_annotation_names=None):
    # type: (Dict[str, List[str]]) -> None
    """Initializes a `BigQueryToVariant` object.

    Args:
      annotation_id_to_annotation_names: A map where the key is the annotation
        id (e.g., `CSQ`) and the value is a list of annotation names (e.g.,
        ['Consequence', 'IMPACT', 'SYMBOL']). The annotation str (e.g.,
        'A|upstream_gene_variant|MODIFIER|PSMF1|||||') is reconstructed in the
        same order as the annotation names.
    """
    self._variant_generator = bigquery_vcf_data_converter.VariantGenerator(
        annotation_id_to_annotation_names)

  def expand(self, pcoll):
    return (pcoll | 'BigQueryToVariant' >> beam.Map(
        self._variant_generator.convert_bq_row_to_variant))
