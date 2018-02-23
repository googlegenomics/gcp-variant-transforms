# Variant merging

You can use Variant Transforms to load multiple files at once
by providing a pattern like `--input_pattern gs://mybucket/*.vcf`. This
will create a merged BigQuery schema that contains all variants from all
matching files. Note that the VCF fields must be compatible across files (e.g.
`INFO` and `FORMAT` fields with the same key have compatible types and numbers
across files).

However, by default, calls between files will not be merged. For instance,
consider the following VCF files:

*`gs://mybucket/a.vcf`*:

```
## <headers omitted>
#CHROM  POS   ID    REF   ALT   QUAL  FILTER  INFO    FORMAT  S1      S2
1       100   rs1   A     T     29    PASS    DP=14   GT:GQ   0|1:48  0/0:30
```

*`gs://mybucket/b.vcf`*:

```
## <headers omitted>
#CHROM  POS   ID    REF   ALT   QUAL  FILTER  INFO         FORMAT  S3
1       100   rs1   A     T     7     q10     DP=11;AA=G   GT:GQ   1/1:2
```

If you load both files together (i.e. by specifying
`--input_pattern gs://mybucket/*.vcf`), you would end up with 2 rows in
BigQuery. One with calls `S1` and `S2`, and another with just call `S3` even
though all calls have the same variant.

Variant merging can be used to merge calls across files. There are different
merging strategies and you can specify any of the following
strategies by adding `--variant_merge_strategy <merge_strategy>` to the
arguments.

## MOVE_TO_CALLS strategy

This strategy merges calls that have the same `reference_name`,
`start_position`, `end_position`, `reference_bases`, and all `alternate_bases`
across files. By default, the variant fields are merged as follows:

* `Calls` are merged together in no particular order.
* The highest `quality` value is chosen among all variants being merged.
* All `filter` values are merged and deduplicated.
* All `names` (`ID` column in the VCF spec) are merged and deduplicated.
* For each `INFO` field, one is chosen as representative for the *entire* merged
  variant record in *no particular order*. Note that for repeated fields, a
  single set is chosen as representative for that particular field.

Depending on your use case, you may customize the merge strategy as follows:

* Adding `--copy_quality_to_calls` and/or `--copy_filter_to_calls` to the
  arguments will copy the `quality` and/or `filter` values in each file to the
  set of calls specified in that file. This option is useful when merging
  single-call VCF files as `quality` and `filter` values usually correspond
  to call-level quality/filter. Note that the variant-level `quality` and
  `filter` values will still be merged according to the above logic.
* Adding `--info_keys_to_move_to_calls_regex <regex>` will move `INFO` fields
  that match the provided regular expression to the associated calls. This is
  useful to ensure all `INFO` fields are kept when merging variants. For
  instance, specifying `.*` will move all `INFO` keys to calls, or specifying
  `^(AA|AF)$` will only move `AA` and `AF` fields to calls. See
  [here](https://docs.python.org/2/library/re.html#regular-expression-syntax)
  for more details on the regular expression specifications. Note that if a
  key is the same in both `INFO` and `FORMAT`, then such a key cannot be moved
  to calls and an error will be raised.

The examples below help with illustrating the details of the above options.
You may also read the code and documentation in
[move_to_calls_strategy.py](../gcp_variant_transforms/libs/variant_merge/move_to_calls_strategy.py).
Particularly the `get_merged_variants` method implements the merging logic.

### Examples

The following examples illustrate the merging outcome using the example files
`gs://mybucket/a.vcf` and `gs://mybucket/b.vcf` that are defined above.

* Default (i.e. just adding `--variant_merge_strategy MOVE_TO_CALLS`):

```
reference_name: "1"
start_position: 100
end_position: 101
reference_bases: "A"
alternate_bases: {alt: "T"}
names: ["rs1"]
quality: 29
filter: ["PASS", "q10"]
call: {
  [
    name: "S3",
    genotype: [1, 1]
    phaseset: null
    GQ: 2
  ],
  [
    name: "S1",
    genotype: [0, 1]
    phaseset: "*"
    GQ: 48
  ],
  [
    name: "S2",
    genotype: [0, 0]
    phaseset: null
    GQ: 30
  ]
}
DP: 14  # This can also be 11.
AA: "G"
```

* With `--copy_quality_to_calls` and `--copy_filter_to_calls`:

```
reference_name: "1"
start_position: 100
end_position: 101
reference_bases: "A"
alternate_bases: {alt: "T"}
names: ["rs1"]
quality: 29
filter: ["PASS", "q10"]
call: {
  [
    name: "S3",
    genotype: [1, 1]
    phaseset: null
    GQ: 2
    quality: 7
    filter: ["q10"]
  ],
  [
    name: "S1",
    genotype: [0, 1]
    phaseset: "*"
    GQ: 48
    quality: 29
    filter: ["PASS"]
  ],
  [
    name: "S2",
    genotype: [0, 0]
    phaseset: null
    GQ: 30
    quality: 29
    filter: ["PASS"]
  ]
}
DP: 14  # This can also be 11.
AA: "G"
```

* With `--info_keys_to_move_to_calls_regex ^DP$`, `--copy_quality_to_calls`,
  `--copy_filter_to_calls`:

```
reference_name: "1"
start_position: 100
end_position: 101
reference_bases: "A"
alternate_bases: {alt: "T"}
names: ["rs1"]
quality: 29
filter: ["PASS", "q10"]
call: {
  [
    name: "S3",
    genotype: [1, 1]
    phaseset: null
    GQ: 2
    quality: 7
    filter: ["q10"]
    DP: 11
  ],
  [
    name: "S1",
    genotype: [0, 1]
    phaseset: "*"
    GQ: 48
    quality: 29
    filter: ["PASS"]
    DP: 14
  ],
  [
    name: "S2",
    genotype: [0, 0]
    phaseset: null
    GQ: 30
    quality: 29
    filter: ["PASS"]
    DP: 14
  ]
}
AA: "G"
```

## MERGE_WITH_NON_VARIANTS strategy [EXPERIMENTAL]

This strategy is not yet production ready and can't be used by users. This
section is included here to provide more context about our ongoing work
([Issue #80](https://github.com/googlegenomics/gcp-variant-transforms/issues/80)
and [Issue #88](https://github.com/googlegenomics/gcp-variant-transforms/issues/88)).

The `MOVE_TO_CALLS` strategy is effective for merging calls that have exactly
the same reference and alternate bases. However, this strategy is not effective
for merging
[gVCF files](https://gatkforums.broadinstitute.org/gatk/discussion/4017/what-is-a-gvcf-and-how-is-it-different-from-a-regular-vcf)
since non-variants are represented as intervals in the genome.

Consider the following VCF file:


*`gs://mybucket/c.g.vcf`*:

```
## <headers omitted>
#CHROM  POS   ID    REF   ALT        QUAL  FILTER  INFO     FORMAT  S4
1       50    .     T     <NON_REF>  .     .       END=150  GT:GQ   0/0:40
```

If you specify `--input_pattern gs://mybucket/*.vcf` with `MOVE_TO_CALLS` merge
strategy, the non-variant region from call `S4` will not be merged with calls
`S1`, `S2`, and `S3`. However, we know that call `S4` has genotype `0/0` in
chr1:100 since it matches the reference from position 50-150 in chr1.
The `MERGE_WITH_NON_VARIANTS` strategy will infer `0/0` for call `S4` at
chr1:100 so that the resulting BigQuery table has a single row with all calls
`S1-4`.

Our initial work is focused on correctly calling SNPs
([Issue #80](https://github.com/googlegenomics/gcp-variant-transforms/issues/80)).
We will then attempt to merge insertions/deletions
([Issue #88](https://github.com/googlegenomics/gcp-variant-transforms/issues/88)).

