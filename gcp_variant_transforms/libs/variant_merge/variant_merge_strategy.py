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

"""Variant merge strategy interface."""

from typing import Iterable, Set  # pylint: disable=unused-import

from apache_beam.io.gcp.internal.clients import bigquery  # pylint: disable=unused-import

from gcp_variant_transforms.beam_io import vcfio  # pylint: disable=unused-import

__all__ = ['VariantMergeStrategy']


class VariantMergeStrategy(object):
  """Interface for a variant merge strategy."""

  def get_merged_variants(self, variants, key):
    # type: (List[vcfio.Variant], str) -> List[vcfio.Variant]
    """Returns a list of merged variant(s) from the provided `variants`.

    Args:
      variants: A list of variants grouped by the key as specified in
        ``get_merge_key``.
      key: This is the key representing variants to be merged.
    Returns:
      A list of merged variants. Typically this is one variant, but the
      interface allows for potentially multiple variants to be returned (e.g.
      if the key is not sufficient for determining a single merged variant).
    """
    raise NotImplementedError

  def get_merge_keys(self, variant):
    # type: (vcfio.Variant) -> Iterable[str]
    """Returns a generator of keys (str) used for merging variants."""
    raise NotImplementedError

  def modify_bigquery_schema(self, schema, info_keys):
    # type: (bigquery.TableSchema, Set[str]) -> None
    """Optionally modifies the bigquery schema based on merging logic.

    Args:
      schema: The original schema from unmerged variants.
      info_keys: Used to robustly identify keys in the schema that correspond to
        INFO keys (INFO keys are mixed with the rest of the schema as they don't
        have their dedicated parent record).
    Raises:
      ValueError: If updates to the schema are incompatible with the settings
        specified by the merge strategy.
    """
    pass
