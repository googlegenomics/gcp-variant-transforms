# Development Guide

## How to Contribute

[Contribution Guide](../CONTRIBUTING.md)

### Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/)
for more information on using pull requests.


## Setup

### Fork the repository on Github
Visit the
[gcp-variant-transforms repository](https://github.com/googlegenomics/gcp-variant-transforms)
to create your own fork of the repository. See
[https://guides.github.com/activities/forking/](https://guides.github.com/activities/forking/)
for more information.

### Setup dev environment

#### Clone the forked repository
```bash
git clone git@github.com:<username>/gcp-variant-transforms.git
cd gcp_variant_transforms
```
You will also need to add googlegenomics repository as a remote so that you can
easily pull changes later.
```bash
git remote add upstream git@github.com:googlegenomics/gcp-variant-transforms.git
```

#### Setup virtualenv
```bash
sudo apt-get install python-pip python-dev build-essential
sudo pip install --upgrade pip
sudo pip install --upgrade virtualenv
virtualenv venv
. venv/bin/activate
```

#### Install dependences
```bash
pip install --upgrade .
```
Note that after running the above command we get some dependency conflicts in
installed packages which is currently safe to ignore. For details see
[Issue #71](https://github.com/googlegenomics/gcp-variant-transforms/issues/71).

## Making Changes
### Create a branch in your forked repository

Running this command will create a branch named `<branch-name>` and switch
you to it.

```bash
git checkout -b <branch-name> origin/master
```

### Testing
To run all unit tests:

```bash
python setup.py test
```

To run a specific test:
```bash
python setup.py test -s gcp_variant_transforms.<module>.<test class>.<test method>
```
To run integration tests, run this script in the root of the source tree:
```bash
./deploy_and_run_tests.sh
```
This will create a Docker image from your current source and run integration
tests against that. Have a look at script's top comments and usage. In
particular, if you want to run integration tests against a specific image `TAG`
in the container registry of cloud project `gcp-variant-transforms-test`,
you can do:
```bash
./deploy_and_run_tests.sh --skip_build --keep_image --image_tag TAG
```
For other projects you can use the `--project` and `--gs_dir` options of the
script.

### Pushing changes to your fork's branch
To push changes to your forked branch, you can run:
```bash
git add -p
```
This will allow you to browse through changes since your last commit and filter
the exact changes that you want to commit. You can then run:
```bash
git commit -m "<commit message>"
git push -u origin <branch name>
```
To commit and push those changes to your branch.

### Syncing your branch
If you want to pull in changes from the target branch (i.e. googlegenomic:master),
run:
```bash
git pull --rebase upstream master
```
This will pull in changes from the target branch and reapply you changes on top of
them. If you run into merge conflicts while rebasing, resolve them, then continue
the rebase by running:
```bash
git rebase --continue
```
If rebase changes the branch's history, you may be blocked from pushing changes
to your branch. If this happens, you can force push after a rebase by runnning:
```bash
git push -f
```

### Creating a pull request
Once your changes are pushed and ready for review, you can create a pull request
by visiting the
[gcp-variant-transforms repository](https://github.com/googlegenomics/gcp-variant-transforms)
and selecting "Create pull request". You will then be prompted to enter a
description of your commits and select reviewers and assignees. Please add
one of the repository contributors (@arostamianfar, @bashir2, @nmousavi).

In the pull request description, please include a `Tested:` field with a brief
description of how you have tested your change. As a minimum you should have
unit-test coverage for your change and make sure integration tests pass.

### Updating changes
After making changes, you must again add, commit, and push those changes. Rather
than creating new commits for related changes please run the following:

```bash
git add -p
git commit --amend
git push -f
```

To amend those changes to the original commit. Please note that using `--amend`
creates a new commit and is a way of rewriting git history. In particular, if
you have branched from the current branch to work on another change on top of
the current one, `--amend` will make merging of these branches non-trivial.

Another git approach that you can take is to create a new _review branch_ and use
the `--squash` option of `git merge` to create one commit from all your changes
and push that for review. For example, if you are on branch `foo` and ready to
send a pull request, you can:
```bash
git checkout master
git checkout -b foo_review
git merge --squash foo
git push origin foo_review
```
This approach is specially useful if you tend to do a lot of small commits
during your feature development and like to keep them as checkpoints.

### Continuous integration
Once your pull request is approved and merged into the main repo, there is an
automated process to create a new docker image from this commit, push it to the
[Container Registry](
https://cloud.google.com/container-builder/docs/running-builds/automate-builds)
of `gcp-variant-transforms-test` project, and run integration tests against
that image (see [`cloudbuild_CI.md`](../cloudbuild_CI.yaml)).
If this fails, your commit might be reverted. If you have access to this test
project, you can check the status of your build in the "Build history"
dashboard of Container Registry.
