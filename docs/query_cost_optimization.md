# Query cost optimization with v2 Schema

With the introduction of v2 schema, we attempt to reduce is the cost and
increase the efficiency of the most common types of queries

## Sharding tables

By default, varaint data is assumed to be for human genome and is sharded into
25 tables (1-22 chromosomes, X chromosome, Y chromosome and unidentified
variants). This allows per chromosome querying of the data, without the need of
computing the rest of the data. Custom sharding configs can be provided, to
either split the data into more shards, or to combine into less.

## Partition BigQuery Tables

Table partitioning comes at no cost, and allows to divide a table in up to 4000
segments. When querying such tables, at most a couple of partitions will be
scanned, thus reducing the cost of the query dramatically.

## Clustering Data

Furthermore, partitioned tables can be clustered based on the partitioned
values. [WIP]

## Query Eperiments

We analyzed data on 1000 genomes public data. Following shows the speed
[WIP]


|                                                  |    |Old Schema|New Schema|
|--------------------------------------------------|----|----------|----------|
|Retrieve all variants within a region             |Cost|24 Mb     |1.19 Mb   |
|                                                  |Time|23.8 s    |0.9 s     |
|Retrieve all variants from certain samples on chr1|Cost|39 Mb     |704 Mb   |
|                                                  |Time|4.7 s     |4.7 s     |
|Retrieve variants from region and sample on chr1  |Cost|24 Mb     |1.19 Mb   |
|                                                  |Time|23.8 s    |0.9 s     |
|Filter samples or variants on variant metadata    |Cost|24 Mb     |1.19 Mb   |
|                                                  |Time|23.8 s    |0.9 s     |
