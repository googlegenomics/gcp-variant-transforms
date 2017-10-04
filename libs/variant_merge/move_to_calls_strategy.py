"""Implements a variant merge stategy that moves fields to calls."""

import re
from beam_io.vcfio import Variant
from libs.bigquery_vcf_schema import ColumnKeyConstants
from libs.variant_merge.variant_merge_strategy import VariantMergeStrategy

__all__ = ['MoveToCallsStrategy']


class MoveToCallsStrategy(VariantMergeStrategy):
  """A merging strategy that moves fields to the corresponding calls records.

  Variants will be merged across files using
  'reference_name:start:end:reference_bases:alternate_bases' as key. INFO
  fields would be moved to calls if they match
  `info_keys_to_move_to_calls_regex`. Otherwise, one will be chosen as
  representatve (in no particular order) among the merged variants.
  Filters will be merged across all variants matching the key and the highest
  quality score will be chosen as representative for the merged variants.
  The filters and quality fields can be optionally copied to their associated
  calls using `copy_quality_to_calls` and `copy_filter_to_calls` options.

  Note: if a field is set to be moved from INFO to calls, then it must not
  already exist in calls (i.e. specified by FORMAT in the VCF header).
  """

  def __init__(self, info_keys_to_move_to_calls_regex, copy_quality_to_calls,
               copy_filter_to_calls):
    """Initializes the strategy.

    Args:
      info_keys_to_move_to_calls_regex (str): A regular expression specifying
        info fields that should be moved to calls.
      copy_quality_to_calls (bool): Whether to copy the quality field to
        the associated calls in each record.
      copy_filter_to_calls (bool): Whether to copy filter field to the
        associated calls in each record.
    """
    self._info_keys_to_move_to_calls_re = (
        re.compile(info_keys_to_move_to_calls_regex)
        if info_keys_to_move_to_calls_regex else None)
    self._copy_quality_to_calls = copy_quality_to_calls
    self._copy_filter_to_calls = copy_filter_to_calls

  def get_merged_variants(self, variants):
    if not variants:
      return []
    merged_variant = None
    for variant in variants:
      if not merged_variant:
        merged_variant = Variant(reference_name=variant.reference_name,
                                 start=variant.start,
                                 end=variant.end,
                                 reference_bases=variant.reference_bases,
                                 alternate_bases=variant.alternate_bases)
      merged_variant.names.extend(variant.names)
      merged_variant.filters.extend(variant.filters)
      if (variant.quality is not None and
          (merged_variant.quality is None or
           merged_variant.quality < variant.quality)):
        merged_variant.quality = variant.quality
      additional_call_info = {}
      if self._should_copy_filter_to_calls():
        additional_call_info[ColumnKeyConstants.FILTER] = variant.filters
      if self._should_copy_quality_to_calls():
        additional_call_info[ColumnKeyConstants.QUALITY] = variant.quality
      for info_key, info_value in variant.info.iteritems():
        if self._should_move_info_key_to_calls(info_key):
          additional_call_info[info_key] = info_value.data
        else:
          merged_variant.info[info_key] = info_value
      for call in variant.calls:
        call.info.update(additional_call_info)
        merged_variant.calls.append(call)

    # Deduplicate names and filters.
    merged_variant.names = sorted(set(merged_variant.names))
    merged_variant.filters = sorted(set(merged_variant.filters))
    return [merged_variant]

  def get_merge_key(self, variant):
    return ':'.join(
        [str(x) for x in [
            variant.reference_name or '',
            variant.start or '',
            variant.end or '',
            variant.reference_bases or '',
            ','.join(variant.alternate_bases or [])]])

  def modify_bigquery_schema(self, schema, info_keys):
    # Find the calls record so that it's easier to reference it below.
    calls_record = None
    for field in schema.fields:
      if field.name == ColumnKeyConstants.CALLS:
        calls_record = field
        break
    if not calls_record:
      raise ValueError('calls record must exist in the schema.')

    existing_calls_keys = set([field.name for field in calls_record.fields])
    updated_fields = []
    for field in schema.fields:
      if (self._should_copy_filter_to_calls() and
          field.name == ColumnKeyConstants.FILTER):
        if ColumnKeyConstants.FILTER in existing_calls_keys:
          self._raise_duplicate_key_error(ColumnKeyConstants.FILTER,
                                          'should_copy_filter_to_calls')
        calls_record.fields.append(field)
        updated_fields.append(field)
      elif (self._should_copy_quality_to_calls() and
            field.name == ColumnKeyConstants.QUALITY):
        if ColumnKeyConstants.QUALITY in existing_calls_keys:
          self._raise_duplicate_key_error(ColumnKeyConstants.QUALITY,
                                          'should_copy_quality_to_calls')
        calls_record.fields.append(field)
        updated_fields.append(field)
      elif (field.name in info_keys and
            self._should_move_info_key_to_calls(field.name)):
        if field.name in existing_calls_keys:
          self._raise_duplicate_key_error(field.name,
                                          'info_keys_to_move_to_calls_regex')
        calls_record.fields.append(field)
      else:
        updated_fields.append(field)
    schema.fields = updated_fields

  def _should_move_info_key_to_calls(self, info_key):
    return bool(self._info_keys_to_move_to_calls_re and
                self._info_keys_to_move_to_calls_re.match(info_key))

  def _should_copy_filter_to_calls(self):
    return self._copy_filter_to_calls

  def _should_copy_quality_to_calls(self):
    return self._copy_quality_to_calls

  def _raise_duplicate_key_error(self, key, flag_name):
    raise ValueError(
        'The field "%s" already exists in calls, but %s flag also moves a '
        'field with the same name to calls. Please either change the flag '
        'or rename the field.' % (key, flag_name))
