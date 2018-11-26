## Understanding the BigQuery Variants Table Schema

See [this page](https://cloud.google.com/genomics/v1/bigquery-variants-schema)
for a general explanation of the schema.

In addition, the schema from Variant Transforms has the following properties:
* If a record has a large number of calls such that the resulting BigQuery
  row is more than 100MB, then that record will be automatically split into
  multiple rows such that each row is less than 100MB. This is needed to
  accommodate BigQuery's
  [10MB per row limit](https://cloud.google.com/bigquery/quotas#import).
* _Only for float/integer repeated fields containing a null value:_ BigQuery
  does not allow null values in repeated fields (the entire record can be null,
  but values within the record must each have a value). For instance, if a
  VCF INFO field is `1,.,2`, we cannot load `1,null,2` to BigQuery and need to
  use a numeric replacement for the null value. By default, the replacement
  value is set to `-2^31` (equal to `-2147483648`). You can also use
  `--null_numeric_value_replacement` to customize this value. The alternative is
  to convert such values to a string and use `.` to represent the null value.
  To do this, please change the header to specify the type as `String`.

