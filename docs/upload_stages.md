# Data Upload Stages

For v2 schema users, variant data will no longer be directly uploaded into
BigQuery designated table. Instead, as an intermediary step, data is converted
into Avro files and stored on GCS in the
`{TEMP_LOCATION}/avro/{JOB_NAME}/{PIPELINE_INITIATION_TIME_BASED_ID}/*`
directory, from where it's uploaded into BQ. This approach serves several
purposes:

 - Speed up writing stage.
   - In v1 approach, data is converted into JSON files instead of AVRO, which
   takes a toll on the operation speed of the pipeline.
 - Allow partitioning and clustering of the output BQ tables.
   - At the moment of writing this document, BigQuery sink did not provide
   functionality to partition output tables, which had to be done via bq CLI
   during table's creation.
 - Provide backup functionality
   - Uploading Avro files into BigQuery is a free and relatively quick
   opertation. Therefore, once Avro files are generated, they can be useed as a
   substitution for backing up BigQuery data.

## Generating Avro files

Prior to variants being generated, for each sample name a unique ID is
generated. A mapping of these sample IDs, names, input file names along with the
creation time (to minute granularity) are converted to Avro format and stored in
the aforementioned directory. Then for each variant, its sample names are
converted to sample IDs instead, sharded into corresponding cohorts defined in
the provided sharding config, and stored in the corresponding Avro files.
Alongside them, a schema for the BQ tables is stored.

Note that Apache Beam splits data into multiple files if their size is too
large, so for each provided `suffix` in the partition config, N tables will be
generated with a `suffix-K-of-N` name format.

## Creating BQ Tables

Once the pipeline finishes, for each suffix an empty BigQuery table is
generated, with the corresponding schema. Each tables is partitioned based on
the variant start position, to improve query speeds. Then, Avro files are
uploaded into the generated BQ tables, in a multi threaded way.

Once the upload of the files is complete, Avro directory is automatically
deleted. However, if the upload stage fails for one reason or another, all the
generated BigQuery tables are deleted, while Avro files are not. This allows
customers to use the Avro files and the schema to retry uploading Avro files
back to BigQuery at no cost, instead of regenerating data from the VCF files.
Simultaneously, they can use the generated sample info Avro file to reupload
the sample ID to sample name mapping table as well.

## Generating Sample Lookup Optimized Tables.

The partitioning logic in the first table optimizes querying variants by
clustering and partitioning on the  start position. However, if users anticipate
heavy usage of the sample based queries, they have an option of duplicating
their BigQuery tables and partition them based on the sample IDs, by enabling
`--sample_lookup_optimized_output_table` flag. If this flag is supplied,
second set of empty BQ tables is created with a modified schema where sample
IDs are partitioned instead; then, the original output BigQuery tables are
unflattened, and their data is duplicated into the newly created tables.

Note that when a table is unflattened, all the deduped records will affect the
Bigquery storage costs, so having sample lookup optimized tables will *more*
than double the storage costs, while heavily reducing querying fees.

## Backing Up Data
If users wish to store the Avro files for backup reasons, they would need to
supply `--keep_intermediate_avro_files` flag, which, when enabled, will prevent
the deletion of the Avro files for successful runs of the pipeline. If the
original VCF files had only 1 sample per file, resulting Avro files will be
smaller and thus will require less storage resources. However, since the records
in Avro files are unflattened, whenever VCF files have many samples per
file, Avro file sizes may surpass VCF file sizes. Therefore, it may be useful to
instead store such VCF files as backups instead, and use the Variant Transforms
tools again, to recreate the BigQuery tables.