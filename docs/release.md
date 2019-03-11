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
   by submitting a change.
1. Wait until the integration tests are done. It will take about 2 hours. Ensure
   that there are no failures. In case of failures, rollback the version change
   in `setup.py` and continue from Step #1 once the errors are fixed.
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
   1. This will kick off a trigger to copy the verified image to the main
      project.
1. Congratulations! The release process is now complete!

