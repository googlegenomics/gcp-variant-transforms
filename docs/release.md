# Release process

To release a new version, you need to be a member of
`gcp-variant-transforms-test` project and have write access to the GitHub repo.

The process is as follows:

1. Navigate to
   [Container Builder build history](https://console.cloud.google.com/gcr/builds?project=gcp-variant-transforms-test)
   and verify that the latest integration tests have passed.
1. Choose the appropriate version number by following the semantics in
   https://semver.org/. See the **Summary** section for a quick overview.
1. Update the version number in
   [setup.py](https://github.com/googlegenomics/gcp-variant-transforms/blob/master/setup.py)
   by submitting a change. Please include `[skip ci]` in the commit message, so
   the auto build will not be triggered and we will run the release build
   trigger manually instead in the next step.
1. Go to
   [Cloud Build](https://pantheon.corp.google.com/cloud-build/triggers?project=gcp-variant-transforms-test),
   find the trigger `Release (disabled)`, and click `Run trigger`. It will start
   running all tests and publish the verified image to the main project at the
   end.
1. Wait until it is done. It will take about 2 hours. Ensure that there
   are no failures. In case of failures, rollback the version change in
   `setup.py` and continue from Step #1 once the errors are fixed.
1. Navigate to the
   [Container registry](https://console.cloud.google.com/gcr/images/gcp-variant-transforms/GLOBAL/gcp-variant-transforms?project=gcp-variant-transforms)
   page and update the published image to have the labels `latest` and `X.X.X`
   for the version number. You may optionally run a "sanity" test on the newly
   released image by running:

   ```bash
   temp_dir="$(mktemp -d)"
   virtualenv "${temp_dir}"
   source ${temp_dir}/bin/activate
   pip install --upgrade .[int_test]
   python gcp_variant_transforms/testing/integration/run_vcf_to_bq_tests.py \
       --project gcp-variant-transforms-test \
       --staging_location "gs://integration_test_runs/staging" \
       --temp_location "gs://integration_test_runs/temp" \
       --logging_location "gs://integration_test_runs/temp/logs" \
       --image gcr.io/gcp-variant-transforms/gcp-variant-transforms
   ```
1. Navigate to the
   [releases tab](https://github.com/googlegenomics/gcp-variant-transforms/releases)
   and click on **Draft a new release**.
   1. Set the **Tag version** to `vX.X.X`
      where `X.X.X` is the release version specified previously.
   1. Set the **Target** to the commit that changed the version number.
   1. Set the **Release title** to "Release vX.X.X".
   1. Enter a description of the main features and/or bug fixes going into the
      release.
   1. Click on **Publish release**.
1. Congratulations! The release process is now complete!

