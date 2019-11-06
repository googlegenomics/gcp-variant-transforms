# Troubleshooting

This page summarizes common error scenarios when running the pipeline and
provide recommanded workarounds. If you are still unable to successfully load
or export your VCF files, please post on the
[google-genomics-discuss](https://groups.google.com/forum/#!forum/google-genomics-discuss)
group or file a GitHub issue if you believe that there is a bug in the pipeline.

## Pipeline is too slow

* Try increasing `--max_num_workers`.
* Try changing `--worker_machine_type` to a larger machine (e.g.
  `n1-standard-32`). See
  [predefined machine types](https://cloud.google.com/compute/pricing#predefined_machine_types)
  for the full list.
* Ensure you have enough [quota](https://cloud.google.com/compute/quotas) in the
  region running the pipeline. You need to [set a region](./setting_region.md) 
  for running the pipeline by specifying `--region <region>`. You can check for
  quota issues by navigating to the [Compute Engine quotas page](https://console.cloud.google.com/iam-admin/quotas?service=compute.googleapis.com)
  while the pipeline is running, which shows saturated quotas at the top of the
  page in red color.
* `gzip` and `bzip2` file formats cannot be sharded, which considerably slows
  down the pipeline. Consider decompressing the files prior to running the
  pipeline. You may use [dsub](https://github.com/googlegenomics/dsub) to write
  a script to decompress the files in scalable manner. Note that this is only an
  issue if running the pipeline with a small number of large files (i.e. running
  with a large number of small files is usually fine as each file can be read by
  a separate process).

See [handling large inputs](./large_inputs.md) for more details.

## Pipeline crashes due to out of disk error

Try increasing the disk size allocated to each worker by specifying
`--disk_size_gb <disk_size>` and/or increasing the number of workers by
specifying `--max_num_workers <num_workers>`. By default, each worker gets 250GB
of disk, and the aggregate disk size available to all workers should be at least
as large as the uncompressed size of the VCF files being loaded. However, to
accomoddate for intermediate stages of the pipeline and also to account for
the additional overhead introduced by the transforms, the aggregate disk size
among all workers should be at least 3 to 4 times the total size of raw VCF
files.

See [handling large inputs](./large_inputs.md) for more details.

## Error: "JSON parsing error ... No such field: <field_name>"

If you see an error in the form,
`Error while reading data, error message: JSON parsing error in row starting at
position 0: No such field: <field_name>`, it means that the `<field_name>`
is missing from the BigQuery schema, which is caused by its definition missing
from a VCF header file.

You can fix this by:

* Changing the VCF file containing the field and adding an entry in the header
  with proper definition. For instance, if the error is for the field `AF`, you
  can add the following to the VCF file:

  ```
  ##INFO=<ID=AF,Number=A,Type=Float,Description="Allele Frequency">
  ```

  Note that it's important to provide valid `Type` and `Number` for the field.
  If unsure, you can provide `Type=String` and `Number=.` (i.e. a generic list
  of strings), which will match any field. Please also check whether the
  missing field is for `##INFO` or `##FORMAT`.

* If changing the file(s) is not an option, you can also run the pipeline with
  `--representative_header_file <file_path>`, where you provide a merged view
  of all headers in all files. You can add any missing fields to that file.
  We are working on a tool to make this process easier and provide
  recommendations for missing fields.

* Run the pipeline with `--infer_headers`. This will do two passes on the data
  and will infer definition for undefined headers and mismatched headers (header
  field definition does not match the field value). You do not need to make any
  changes to the VCF files or provide a representative header file. However,
  running with this option adds ~30% more compute to the pipeline.

## Error: "BigQuery schema has no such field"

Same as [above](#error-json-parsing-error--no-such-field-field_name).

## BigQuery to VCF fails: "A work item was attempted 4 times without success."

The error "Each time the worker eventually lost contact with the service." may
relate to insufficient memory. 

* Try changing `--worker_machine_type` to a larger machine (e.g.
  `n1-standard-64`). See
  [predefined machine types](https://cloud.google.com/compute/pricing#predefined_machine_types)
  for the full list.

* Try lowering the value of `--number_of_bases_per_shard` (e.g. `10000`)

