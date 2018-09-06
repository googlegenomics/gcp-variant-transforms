# Dealing with malformed files


## Field compatibility

When loading multiple files, the `INFO` and `FORMAT` fields
from all VCF files are merged together to generate a "representative header",
which is then used to generate a single BigQuery schema. If the same key is
defined in multiple files, then its definition must be compatible across files.
The compatibility rules are as follows:

* Fields are compatible if they have the same `Number` and `Type` fields.
  Annotation fields (i.e. those specified by `--annotation_fields`) must also
  have the same `Description`.

* Fields with different `Type` values are compatible in the following cases:

  * `Integer` and `Float` fields are compatible and are converted to `Float`.
  * You must run the pipeline with `--allow-incompatible-records` to
    automatically resolve conflicts between incompatible fields (e.g. String
    and Integer). This is to ensure incompatible types are not silently ignored.
    See [below](#specifying---allow-incompatible-records) for more details.

* Fields with different `Number` values are compatible in the following cases:

  * "Repeated" numbers are compatible with each other. They include:
    * `Number=.` (unknown number)
    * Any `Number` greater than 1.
    * `Number=G` (one value per genotype) and `Number=R` (one value for each
      alternate and reference).
    * `Number=A` (one value for each alternate) only if running with
      `--split_alternate_allele_info_fields False`.

  * You must run the pipeline with --allow_incompatible-records to
    automatically resolve conflicts between incompatible fields (e.g.
    `Number=1` and `Number=.`). This is to ensure incompatible types
    are not silently ignored.
    See [below](#specifying---allow-incompatible-records) for more details.
    
You can run preprossing tool to get a summary of malformed/incompatible
records. Please refer to 
[VCF files preprocessor](./vcf_files_preprocessor.md) for more details.

## Specifying `--representative_header_file`

The headers in the `--representative_header_file <path_to_file>` essentially
specify the merged headers from all files being loaded to BigQuery. This file is
used to directly generate the BigQuery schema. Note that we only read the
header info from the file and ignore VCF records, so the
`representative_header_file` can either be a file containing *just* the header
fields or can point to an actual VCF file. Providing this file can be useful
for:

* Speeding up the pipeline especially if a large number of files are provided.
  The pipeline will use the provided file to generate the BigQuery schema and
  will skip merging headers across files. This is particularly useful if all
  files have identical VCF headers.
* Providing definitions for missing header fields. See the
  [troubleshooting page](./troubleshooting.md) for more details.
* Resolving incompatible field definition across files. See
  [below](#specifying---allow-incompatible-records) for an alternative.

## Specifying `--infer_headers`

If this flag is set, pipeline will infer `TYPE` and `NUMBER` for undefined
fields based on field values seen in VCF files. It will also output a
representive header that contains infered definitions as well as definitions
from headers. Use this flag if there are fields with missing definition.


## Specifying `--allow_incompatible_records`

Pipeline will fail by defualt if there is a mismatch between field definition
and actual values or if a field has two inconsistent definitions in two
different VCF files.
By specifying `--allow_incompatible_records`, pipeline will resolve conflicts
in header definitons. It will also cast field values to match BigQuery schema if
there is a mismatch between field definition and field value (e.g. Integer field
value is casted to String to match a field schema of type String).
