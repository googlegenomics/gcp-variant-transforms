# Reducing cost of queries on BigQuery
The [pricing model of BigQuery](https://cloud.google.com/bigquery/pricing#queries)
is based on the total data processed in the columns selected by each query
regardless of filtering criteria specified by `WHERE` or `LIMIT` conditions.
For example, if you want to process only variants of chromosome 1 in a table
containing entire genome variants, you would write a query like:

```
SELECT ...
FROM imported_table
WHERE reference_name = 'chr1' AND ...
```

In this example BigQuery processes all variants even though the query is
limited to the variants on chromosome 1. This query will cost the entire size
of the columns being accessed in the SELECT clause, regardless of the `WHERE`
clause. This extra cost can add up to a significant amount, specially if a few
*hot spots* in the genome are being queried regularly.

With the 1.0.0 release of Variant Transforms improvements to the BigQuery schema
have been introduced to remove the redundancy described above, to vastly reduce
the query costs:

## 1. Shard the data into multiple tables, based on chromosome.

Since more often than not analysis is done on a single chromosome at a time, it
is wasteful to compute entirety of the data just to query a subsection of it.
Therefore, data in Variant Transforms will be sharded into multiple tables to
help with the analysis.

By default, a VCF is parsed in accordance with human genome and will produce 25
separate tables for chromosomes 1-22, X, Y with addition of a residual table,
for unknown data. This behavior is dictated by `--sharding_config_path` flag.
If users would like to modify the sharding configuration to either use other
genome or to combine multiple chromosomes, they can create a castom sharding
config and pass it via the aforementioned flag.

### Sharding Config.

The config file is formatted as a [`YAML`](https://en.wikipedia.org/wiki/YAML)
file with a straight forward structure. [Here](https://github.com/googlegenomics/gcp-variant-transforms/blob/master/gcp_variant_transforms/data/sharding_configs/homo_sapiens_default.yaml) is the default config file that splits output table
into 25 tables, one per chromosome plus an extra [residual shard](#residual-shard),
and can serve as a template for custom configs. Here is a snippet of that file:

```
-  partition:
     partition_name: "chr1"
     regions:
       - "chr1"
       - "1"
     total_base_pairs: 249,240,615
```

This defines a shard, named `chr1`, that will include all variants whose
`reference_name` is equal to `chr1` or `1`. Note that the `reference_name`
string is *case-insensitive*, so if your variants have `Chr1` or `CHR1` they
will all be matched to this shard. Finally `total_base_pairs` field should
provide the maximum amount of base pairs in that chromosome(s). This value is
used partitioning the tables (more on that below).

The final output table name for this shard will have `__chr1`
suffix. More precisely, if
`--output_table my-project:my_dataset.my_table`
is set, then the output table for chromosome 1
variants will be available at
`my-project:my_dataset.my_table__chr1`. Note that you can use any string as
suffix for your table names. Here, for simplicity, we used the same string
(`chr1`) for both `reference_name` matching and table name suffix.

Sharding can be done at a more fine grained level and does not have to be
limited to chromosomes. For example, the following config defines two shards
that contain variants of chromosome X:
```
-  partition:
     partition_name: "chrX_part1"
     regions:
       - "chrX:0-100,000,000"
-  partition:
     partition_name: "chrX_part2"
     regions:
       - "chrX:100,000,000-999,999,999"
```
If the *start position* of a variant on chromosome X is less than `100,000,000`
it will be assigned to `chrX_part1` table otherwise it will be assigned to
`chrX_part2` table.

#### Residual Shard
All shards defined in a config file follow the same principal, variants will
be assigned to them based on their defined `regions`. The only exception is the
`residual` shard, this shard acts as *default shard* that
all variants that were not assigned to any shard will end up in this
shard. For example consider the following config file:
```
-  partition:
     partition_name: "first_50M"
     regions:
       - "chr1:0-50,000,000"
       - "chr2:0-50,000,000"
       - "chr3:0-50,000,000"
-  partition:
     partition_name: "second_50M"
     regions:
       - "chr1:50,000,000-100,000,000"
       - "chr2:50,000,000-100,000,000"
       - "chr3:50,000,000-100,000,000"
-  partition:
     partition_name: "all_remaining"
     regions:
       - "residual"
```

This config file splits all the variants into 3 tables:
 * `first_50M`: all variants of `chr1`, `chr2`, and `chr3` whose start position
 is `< 50M`
 * `second_50M`: all variants of `chr1`, `chr2`, and `chr3` whose start position
 is `>= 50M` and `< 100M`
 * `all_remaining`: all remaining variants. This includes:
   * All variants of `chr1`, `chr2`, and `chr3` with start position `>= 100M`
   * All variants of other chromosomes.

Using the `residual` shard you can make sure your output tables will include
*all* input variants. However, if in your analysis you don't need the residual
variants, you can simply remove the last shard from your config file. In
the case of previous example, you will have only 2 tables as output and variants
that did not match to those two shards will be dropped from the final output.

This feature can be used more broadly for filtering out unwanted variants from
the output tables. Filtering reduces the cost of running Variant Transforms
as well as the cost of running queries on the output tables.

## 2. Duplicate data for ease of querying
New design reduces the query time for two most common analysis - sample based
querying and variant based querying. Given that storing data is way cheaper than
querying it, we duplicate the data into new tables and apply sample or variant
partitioning and clustering on each, to reduce querying costs.

## 2. Partition and Cluster the BigQuery tables.

When creating the BigQuery tables, we are utilizing [integer based paritioning](https://docs.google.com/document/d/1pRazq2e5ZT-FO4wNMp4meb7ACqDbokJnOJb-Mm7rnEg/edit#heading=h.o9yofthklgid) and [clustering](https://cloud.google.com/blog/products/data-analytics/skip-the-maintenance-speed-up-queries-with-bigquerys-clustering)
to reduce the query costs.

Clustering and parititioning reduces the query time by 10 fold, but it has to be
done over integer values. For sample-querying table, we cluster and partition
the data based on start position field. However, for variant-querying table, we
introduce a new sample ID integer field that will be used as the basis of
paritioning and clustering.

### Sample ID.

There are two ways how Sample ID is getting generated. If users specify
`--sample_name_encoding` as WITHOUT_FILE_PATH *(defualt)*, sample ID will be
generated by 64 bit hashing of the sample name. Via this method, sample names
that come from different files but have the identical values will be treated as
the same sample ID.

Users can additionally make a disctinction between similar sample names that
are derived from different files. In order to do so, when gererating a BigQuery
table, they need to set `'--sample_name_encoding WITH_FILE_PATH'` which will use
both sample name ***and*** the original input file as the basis for the hash.

When a BigQuery tables are getting created, an addition sample ID to name and
file mapping table will be generated. This table will have the same base table
name but will have `__sample_info` suffix.

## Example Queries

TODO(tneymanov): add queries