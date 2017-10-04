"""Variant merge strategy interface."""

__all__ = ['VariantMergeStrategy']


class VariantMergeStrategy(object):
  """Interface for a variant merge strategy."""

  def get_merged_variants(self, variants):
    """Returns a list of merged variant(s) from the provided `variants`.

    Args:
      variants (list of ``Variant`` objects): A list of variants grouped by
        the key as specified in ``get_merge_key``.
    Returns:
      A list of merged variants. Typically this is one variant, but the
      interface allows for potentially multiple variants to be returned (e.g.
      if the key is not sufficient for determining a single merged variant).
    """
    raise NotImplementedError

  def get_merge_key(self, variant):
    """Returns a key (str) used for merging variants."""
    raise NotImplementedError

  def modify_bigquery_schema(self, schema, info_keys):
    """Optionally modifies the bigquery schema based on merging logic.

    Args:
      schema (``bigquery.TableSchema``): The original schema from unmerged
        variants.
      info_keys (set of str): Used to robustly identify keys in the schema that
        correspond to INFO keys (INFO keys are mixed with the rest of the
        schema as they don't have their dedicated parent record).
    Raises:
      ValueError: If updates to the schema are incompatible with the settings
        specified by the merge strategy.
    """
    pass
