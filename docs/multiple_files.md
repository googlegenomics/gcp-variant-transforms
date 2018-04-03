# Loading multiple files

You can use Variant Transforms to load multiple files at once
by providing a pattern like `--input_pattern gs://mybucket/*.vcf`. The number of
files that can be loaded at once is essentially limitless (we have tested with
more than 750,000 files), but please see the instructions for
[handling large inputs](./large_inputs.md) if you want to load a large number
of files.

The following operations are performed when loading multiple files:

* A merged BigQuery schema is created that contains data from all matching
  files. Particularly, all `INFO` and `FORMAT` fields in all VCF files will
  be merged together. This assumes that fields with the same key that are
  defined in multiple files are compatible (see [below](#field-compatibility)
  for field compatiblility).
* Records from all files are loaded into the single table. Missing fields
  are set to `null` in the associated column(s).
* Samples can be optionally merged together using a merging strategy. See
  [variant merging](./variant_merging.md) for details.

## Input pattern

In addition to `*`, you may specify more specific patterns such as
`gs://mybucket/[a-c]*.vcf`, which will match any VCF file in `mybucket` that
is included in a folder starting with `a`, `b`, or `c`. See the
[Cloud Storage documentation](https://cloud.google.com/storage/docs/gsutil/addlhelp/WildcardNames)
for more details.

## Field compatibility

As mentioned above, when loading multiple files, the `INFO` and `FORMAT` fields
from all VCF files are merged together to generate a "representative header",
which is then used to generate a single BigQuery schema. If the same key is
defined in multiple files, then its definition must be compatible across files.
The compatibility rules are as follows:

* Fields are compatible if they have the same `Number` and `Type` fields.
  Annotation fields (i.e. those specified by `--annotation_fields`) must also
  have the same `Description`.

* Fields with different `Type` values are compatible in the following cases:

  * `Integer` and `Float` fields are compatible and are converted to `Float`.
  * We are adding more compatibility options (e.g. resolving `String` and
  `Integer` to `String`). Stay tuned!

* Fields with different `Number` values are compatible in the following cases:

  * "Repeated" numbers are compatible with each other. They include:
    * `Number=.` (unknown number)
    * Any `Number` greater than 1.
    * `Number=G` (one value per genotype) and `Number=R` (one value for each
      alternate and reference).
    * `Number=A` (one value for each alternate) only if running with
      `--split_alternate_allele_info_fields False`.

  * We are adding more compatibility options (e.g. resolving `Number=1` and
    `Number=.` with `Number=.`). Stay tuned!

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
* Coming soon: resolving incompatible fields across files.
