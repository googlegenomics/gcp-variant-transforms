# Appending data to existing tables

In order to append data to an existing table, simply add `--append` when
running the pipeline.

The `--append` option requires the schema of the appended data to be exactly the
same as the existing table. If you still like to append the data even though the
schemas are not the same, add `--update_schema_on_append` together with
`--append`. The new fields (if any) will be added to the existing schema, and
the values in the new columns are set to NULL for the existing rows. Similarly,
if some existing fields are not present in the appended data, the values of
these columns are also set to NULL for the newly added data. Note that if there
are fields with the same names present in both schemas, but with a different
type or mode, the pipeline fails.
