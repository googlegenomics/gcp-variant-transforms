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

