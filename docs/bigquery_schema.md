## Understanding the BigQuery Variants Table Schema

See [this page](https://cloud.google.com/genomics/v1/bigquery-variants-schema)
for a general explanation of the schema.

**Note:** the schema used in Variant Transforms has the following changes
compared to the linked page:

* `start` and `end` have been renamed to `start_position` and `end_position`.
  This is because `end` is a reserved keyword in SQL.
* `alternate_bases` has been changed to a record, which also contains any
  INFO field with `Number=A`. This is to make querying easier because it avoids
  having to map each field with the corresponding alternate record. If you
  prefer to use the old schema where `Number=A` fields appear independent of
  alternate bases, then set `--split_alternate_allele_info_fields False` when
  running the pipeline.
* `call_set_name` has been renamed to `name`.
* `call_set_id` and `variant_set_id` columns have been removed. These fields
  are no longer applicable in this pipeline.
* Explicit transform of `call.GL` to `call.genotype_likelihood` has been
  removed, so any `GL` field will be loaded to BigQuery 'as is'.

In addition, the schema from Variant Transforms has the following properties:
* If a record has a large number of calls such that the resulting BigQuery
  row is more than 10MB, then that record will be automatically split into
  multiple rows such that each row is less than 10MB. This is needed to
  accommodate BigQuery's
  [10MB per row limit](https://cloud.google.com/bigquery/quotas#import).
* _Only for float/integer repeated fields containing a null value:_ BigQuery
  does not allow null values in repeated fields (the entire record can be null,
  but values within the record must each have a value). For instance, if a
  VCF INFO field is `1,.,2`, we cannot load `1,null,2` to BigQuery and need to
  use a numeric replacement for the null value. The replacement value is
  currently set to `-2^31` (equal to `-2147483648`).
  [Issue #68](https://github.com/googlegenomics/gcp-variant-transforms/issues/68)
  tracks the feature to make this value configurable. The alternative is to
  convert such values to a string and use `.` to represent the null value.
  To do this, please change the header to specify the type as `String`.

