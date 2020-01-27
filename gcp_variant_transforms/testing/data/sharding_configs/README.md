This file summarizes the contents and the purpose for each files/folder within
current folder.

All files in this folder are used as inputs for --partition_config_path flag.

`residual_at_end.yaml`, `residual_in_middle.yaml`, and `residual_missing.yaml`
are used by unit tests in gcp_variant_transforms/libs/variant_partition_test.py
which are testing the functionality of VariantSharding class. This class
contains the core logic of partitioning functionality of VariantTransform.

The main difference between these 3 yaml files is related to their residual
partition (the default partition which all variants will be assigned to if they
did not get mapped to any partition defined in the config file). As their name
suggests, `residual_at_end.yaml` has its residual partition at the end,
`residual_in_middle.yaml` has its residual partition in the middle, and
`residual_missing.yaml` does not have a residual partition (which means all
unmapped variants will be dropped from the final output).

Similarly, `integration_with_residual.yaml` and
`integration_without_residual.yaml` are used for integration testing of
partitioning functionality and as their name suggests their main difference is
presence or absense of residual partition.
