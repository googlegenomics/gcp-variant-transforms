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
  defined in multiple files are compatible).
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

## Incompatible VCF files

When loading multiple files, field definitions and their values must be
consistent across all VCF files, or else pipeline fails by default.
However, Variant Transforms pipeline is able to handle such malformed files if
instructed to do so. See [Dealing with malformed files](./malformed_files.md)
for more details.

