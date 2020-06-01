# Reducing cost of queries on BigQuery
The [pricing model of BigQuery](https://cloud.google.com/bigquery/pricing#queries)
is based on the total data processed in the columns selected by each query
regardless of filtering criteria specified by `WHERE` or `LIMIT` conditions.
For example, if you want to process only variants of chromosome 1 in a table
containing entire genome variants, you would write a query like:

```
SELECT refence_name, refernce_bases, quality
FROM imported_table__chr1
WHERE start_position > 1,000,000 AND start_position < 2,000,000
```

In this example BigQuery processes all rows of the table even though the query is
limited to the variants in `[1,000,000, 2,000,000]` range. This query will cost the entire size
of the 3 columns being accessed in the SELECT clause, regardless of the `WHERE`
clause. Following figure shows this fact schematically:
[Image here]

This extra cost can add up to a significant amount, specially if a few
*hot spots* in the genome are being queried frquently. We are offering three techniques to reduce the cost of queries like this. In the following sections we breifly explain each cost reduction technique:


## Technique 1: Sharding per chromosome

The first technique for reducing the cost of queries is to split variants between multiple smaller output tables, we call this technique *sharding*.
Variant transforms shards its output into multiple tables, each containing variants of a specific region of a
genome, according to a [sharding config file](link_to_sharding_doc). Our default [config file](gcp-variant-transforms/gcp_variant_transforms/data/sharding_configs/homo_sapiens_default.yaml) results in one output table per chromosome. 

By splitting output tables based on the `reference_name` you are
guaranteed that per-chromosome queries will only process variants of the
chromosome under study. 

As a concrete example, during the process of publishing [gnomAD on GCP marketplace](https://console.cloud.google.com/marketplace/details/broad-institute/gnomad) we imported [gnomAD v3](https://gnomad.broadinstitute.org/) VCF files into a single BigQuery table with 707,950,943 rows which accupied 1.1 TB storage. After spliting by chromosome, we ended up with smaller tables for each chromosome, here are some of their sizes:
 * chr1: 55,215,812 rows, 88.31 GB
 * chr2: 58,605,556 rows, 94.01 GB
 * chr3: 47,688,920 rows, 77.78 GB
 * chr21: 10,248,793 rows, 15.62 GB
 * chr22: 10,882,958 rows, 17.78 GB
 * chrY: 1,344,278 rows, 1.64 GB
 * chrX: 28,156,663 rows, 40.51 GB

Other than making each table smaller and more manageable, sharding per chromosome is a prerequisite for the next technique.

## Technique 2: Partitioing of `start_position` column

BigQuery [partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables) is a technique that divides a large table into segments, called partitions, this makes it easier to manage and query your data. By dividing a large table into smaller partitions, you can improve query performance, and you can control costs by reducing the number of bytes read by a query.

As we mentioned earlier, since BigQuery uses a columnar data structure, prices of a query is set according to the total data processed in the columns you select. For large tables, such as gnomAD, this cost will be very significant, especially if we are querying a small region of genome. Partitioning is the perfect solution here: BigQuery analyzes the `WHERE` clause and based on that it only processes the set of partitioning that contain rows that are relevant to the query. Following figure demonestrates this schematically:

[Image here]

Partitioning is a very effective way to reduce the cost of queries, specially in the genomics applications where most of the queries are analyzing a small genomic region.

```
SELECT * from `1000genomes_chr17`
WHERE start_position >= 41197694
  AND end_position <= 41276113
```

[more explanation here]

## Technique 3: BigQuery clustering
[Updated up to here]
[BigQuery clustering](https://cloud.google.com/bigquery/docs/clustered-tables)
is a technique for automatically organizing a table  based on the
contents of one or more columns in the table’s schema. Clustered columns
are used to colocate related data. More specifically, BigQuery sorts the data
based on the values of the clustered columns and organizes the data into
multiple blocks in BigQuery storage.

Clustering can improve the performance of certain types of queries such as
queries that use filter clauses and queries that aggregate data of clustered
columns. When you submit such a query, BigQuery uses the sorted blocks to
eliminate scans of unnecessary data. Similarly, performance of aggregation
queries is improved because the sorted blocks colocate rows with similar values.
For more information about clustering please refer to this
[blog post](https://medium.com/@hoffa/bigquery-optimized-cluster-your-tables-65e2f684594b).

As a concrete example, consider the following query which is finding all
variants in *BRCA1* gene in a table that contains all
[1000 genomes](https://en.wikipedia.org/wiki/1000_Genomes_Project) variants:

```
SELECT * from `1000genomes`
WHERE reference_name = '17'
  AND start_position >= 41197694
  AND end_position <= 41276113
```

Running this query will process the entire table, which is 5.37 TB. Running the
same query on a clustered table (based on 3 columns: `reference_name,
start_position, end_position`) will process only 551.09 MB. In terms
of actual price (as of today BigQuery costs $5 per TB), we are looking at
reduction from $26.85 to $0.0026. You may try this out in the publicly hosted
[1000 genomes table](https://bigquery.cloud.google.com/table/bigquery-public-data:human_genome_variants.1000_genomes_phase_3_variants_20150220) on GCP.

### Create a clustered variants table

For quick reference, you can cluster your table using a query like this:

```
#standardSql
CREATE TABLE `clustered_table`
PARTITION BY partition_date_please_ignore
CLUSTER BY reference_name, start_position, end_position AS (
  SELECT *, DATE('1980-01-01') partition_date_please_ignore
  FROM `original_table`
)
```

Since clustering currently is only supported for partitioned tables, in this
query first we add a dummy `DATE` column to our table. By partitioning table
using this `DATE` column, we are able to cluster it based on the values of
`reference_name, start_position, end_position` columns.

### Copying field descriptions from original table

Variant Transforms will set column descriptions in the BigQuery table
created by `vcf_to_bq`. To copy those column descriptions to your new
clustered table, save the following shell script, set the `ORIGINAL_TABLE`
and `CLUSTERED_TABLE` values appropriately, and run the script:

```
#!/bin/bash

set -o errexit
set -o nounset

readonly ORIGINAL_TABLE=<project>:<dataset>.<original_table>
readonly CLUSTERED_TABLE=<project>:<dataset>.<clustered_table>

readonly TMP_SCHEMA_FILE=/tmp/schema.txt

# Get the schema from the original table
bq show --schema --format=json "${ORIGINAL_TABLE}" > "${TMP_SCHEMA_FILE}"

# Add the dummy date field to the end of the schema file
sed -i \
 -e 's#] *$#,{"description":"","type":"DATE","name":"partition_date_please_ignore", "mode":"NULLABLE", "description": "This field is used for BigQuery clustering and contains no useful information"}]#' \
  "${TMP_SCHEMA_FILE}"

# Update the clustered table
bq update "${CLUSTERED_TABLE}" "${TMP_SCHEMA_FILE}"
```

### Limitations

Clustering can be very effective in reducing the cost of queries, however,
it has a few limitations:
 * Clustering does not offer any *guarantees* on the cost (it's a best effort
 reduction).
 * It needs a one time clustering step which can take a few hours for large
 datasets.
 * If you append data to an existing clustered table it will become partially
 sorted. So you need to regularly re-cluster your table.



## Solution 3: Hybrid Solution

This solution combines two previous solutions to offer the benefits of both.
Using the Variant Transforms' native sharding, the output table will be
split into several smaller tables (perhaps one table per chromosome) and then
each table will be clustered based on the `start_position, end_position`
columns to further optimize them for running queries.

Using this technique the *BRCA1 query* will cost 226 MB or $0.0011 which is more
than half of the clustering cost. In our experiments, we found that the most
effective way to reduce query cost, especially for point lookup queries, is
this hybrid solution.

If you append new rows to your clustered table and your table gradually becomes
partially sorted, you still have a strict guarantee that your query cost will
be limited to the size of the sharded table ($0.74 in this case).
Also, sharding output table into several smaller tables reduces the initial
clustering time significantly.

In the following section we will explain how you could use Variant Transforms
to easily shard your output table to minimize the cost of your queries.

## Sharding Config files

Solution #2 or #3 require a *sharding config file* to specify the output
tables. The config file is set using the `--sharding_config_path` flag and is
formatted as a [`YAML`](https://en.wikipedia.org/wiki/YAML) file with a straight
forward structure. [Here](https://github.com/googlegenomics/gcp-variant-transforms/blob/master/gcp_variant_transforms/data/sharding_configs/homo_sapiens_default.yaml)
you can find a config file that splits output table into 25 tables, one per
chromosome plus an extra [residual shard](#residual-shard). We
recommend using this config file as default for human samples by adding:
`--sharding_config_path gcp_variant_transforms/data/sharding_configs/homo_sapiens_default.yaml`
flag to your variant transforms command. Here is a snippet of that file:

```
-  partition:
     partition_name: "chr1"
     regions:
       - "chr1"
       - "1"
```

This defines a shard, named `chr1`, that will include all variants whose
`reference_name` is equal to `chr1` or `1`. Note that the `reference_name`
string is *case-insensitive*, so if your variants have `Chr1` or `CHR1` they
will all be matched to this shard.

The final output table name for this shard will have `_chr1`
suffix. More precisely, if
`--output_table my-project:my_dataset.my_table`
is set, then the output table for chromosome 1
variants will be available at
`my-project:my_dataset.my_table_chr1`. Note that you can use any string as
suffix for your table names. Here, for simplicity, we used the same string
(`chr1`) for both `reference_name` matching and table name suffix.

As we mentioned earlier, sharding can be done at a more fine grained level
and does not have to be limited to chromosomes. For example, the following
config defines two shards that contain variants of chromosome X:
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

### Residual Shard
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
