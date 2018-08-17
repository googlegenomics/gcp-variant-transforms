# Partitioning output tables

The [pricing model of BigQuery](https://cloud.google.com/bigquery/pricing#queries)
is based on the total data processed in the columns selected by each query
regardless of filtering criteria specified by `WHERE` or `LIMIT` conditions.
As a result, partitioning the output table can reduce query costs significantly
if only a particular region of the genome is being processed.

Let us elaborate this using a more concrete example. If you want to query
variants in chromosome 1 from a BigQuery table that contains variants from
the entire genome, you would write a query like:
```
SELECT ...
FROM imported_table
WHERE reference_name = 'chr1' AND ...
```

In the above example, BigQuery actually processes all variants even though the
query only considers variants on chromosome 1. You would also get charged for
the entire size of the columns accessed in the SELECT clause. With partitioning,
the data would be split into smaller tables, each containing variants of one
chromosome. In that case the above query can be written as:
```
SELECT ...
FROM bigquery_table_chr1
WHERE ...
```
The second query will cost a fraction of the first query as it only processes
variants in chromosome 1.

Note that partitioning can be done at a more fine grained level and does not have
to be limited to chromosomes. For example, a partition can be defined as a
region of interest on a particular chromosome which will be heavily queried.
In the following section we will explain how you could use
Variant Transforms to easily partition your output to match your future
requirements.

## Partition Config files

Output table is partitioned based on the *partition config file* which can be
set using `--partition_config_path` flag. The config file is formatted as
a [`YAML`](https://en.wikipedia.org/wiki/YAML) file and has a straight forward
structure. [Here](https://github.com/googlegenomics/gcp-variant-transforms/blob/master/gcp_variant_transforms/data/partition_configs/homo_sapiens_default.yaml)
you can see a config file which splits output table into 25 tables, one for
each chromosome plus an extra [residual partition](#residual-partition). We
recommend using this config file as default for human samples by adding:
`--partition_config_path gcp_variant_transforms/data/partition_configs/homo_sapiens_default.yaml`

Here is a snippet of that file:
```
-  partition:
     partition_name: "chr1"
     regions:
       - "chr1"
       - "1"
```

This defines a partition, named `chr1`, that will include all variants whose
`reference_name` is equal to `chr1` or `1`. Note that the `reference_name`
string is *case-insensitive*, so if your variants have `Chr1` or `CHR1` they
will all be matched to this partition.

The final output table name for this partition will have `_chr1`
suffix. More precisely, if
`--output_table my-project:my_dataset.my_table`
is set, then the output table for chromosome 1
variants will be available at
`my-project:my_dataset.my_table_chr1`. Note that you can use any string as
suffix for your table names. Here, for simplicity, we used the same string
(`chr1`) for both `reference_name` matching and table name suffix.

As we mentioned earlier, partitioning can be done at a more fine grained level and does not have
to be limited to chromosomes. For example, the following defines two
partitions that contain variants of chromosome X:
```
-  partition:
     partition_name: "chrX_01"
     regions:
       - "chrX:0-100,000,000"
-  partition:
     partition_name: "chrX_02"
     regions:
       - "chrX:100,000,000-999,999,999"
```
If the *start position* of a variant on chromosome X is less than `100,000,000`
it will be assigned to `chrX_01` otherwise it will be assigned to `chrX_02`.

### Residual Partition
All partitions defined in a config file follow the same principal, variants will
be assigned to them based on their `regions`. The only exception is the `residual`
partition, this partition acts as *default
partition* meaning that all variants that were not assigned to any partition
will end up in this partition. For example consider the following config file:
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
 * `first_50M`: all variants of `chr1`, `chr2`, and `chr3` whose start position is `< 50M`
 * `second_50M`: all variants of `chr1`, `chr2`, and `chr3` whose start position is `>= 50M` and `< 100M`
 * `all_remaining`: all remaining variants including:
   * All variants of `chr1`, `chr2`, and `chr3` whose start position is `>= 100M`
   * All variants of other chromosomes.

Using the `residual` partition you can make sure your output tables will include
*all* input variants. However, if in your analysis you don't need the residual
variants, you can simply remove the last partition from your config file. In
that case you will have only 2 tables as output and variants that did not match
to those two partitions will be dropped from the final output.

#### Filtering Variants
Partitioning can be used for filtering out unwanted variants from the output
table, this will significantly reduce the cost of queries in cases where only a
small region of genome is under study. By providing a config file that includes
the details of the region of interest and does not have a residual partition
the output BigQuery table will contain only the desired variants. This
filtering not only reduces the cost of running Variant Transforms but also it
will reduce the cost of running queries on the output table significantly.

### Comparing Variant Transforms' Partitioning to BigQuery's Clustering
BigQuery announced **[clustering](https://cloud.google.com/bigquery/docs/clustered-tables)** at GCPNEXT2018 ([a relevant blog post](https://medium.com/@hoffa/bigquery-optimized-cluster-your-tables-65e2f684594b)).
This new feature can be used in addition/instead of Variant Transform's
partitioning in order to reduce the cost of queries. By clustering your tables
you can still run queries to process the entire dataset without modifying your
existing queries. Also you can sort up to 4 nested columns, for example
`CLUSTER BY reference_name, start_position, end_position`. Clustering can be
very effective in reducing the costs, however, it has a few limitations:
 * Clustering does not offer 'guarantees' on the cost (a best effort reduction).
 * It needs a one time clustering step which can take a few hours for large datasets.
 * If you append data to an existing table it will become partially sorted. you need to regularly re-clustering your table.

On the other hand, Variant Transforms partitioning provides a cost guarantee.
For example, by separating tables based on the `reference_name` you are
guaranteed that each chromosome's query will only process variants of that
chromosome. Also, you can append new rows to your existing tables and still
enjoy the same guarantee.

In our experimental studies,  we found out the most effective way to reduce the
cost of queries, specially *the point lookup queries*, is a hybrid approach:
specify the hotspots in the config file so that their variants end up in
separate tables, but also add clustering to each table and sort based on
`start_position` and `end_position` to further optimize your queries. This is
a very effective strategy specially when having only 1 output table makes the
clustering step very long and practically impossible.
table for range queries.
