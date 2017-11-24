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
python setup.py install
```

## Making Changes
### Create a branch in your forked repository

Running this command will create a branch named `<branch-name>` and switch
you to it.

```bash
git checkout -b <branch-name> origin/master
```

### Testing
To run all tests:

```bash
python setup.py test
```

To run a specific test:
```bash
python setup.py test -s gcp_variant_transforms.<module>.<test class>.<test method>
```

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
one of the repository contributors (@arostamianfar, @mhsaul).

### Updating changes
After making changes, you must again add, commit, and push those changes. Rather
than creating new commits for related changes please run the following:

```bash
git add -p
git commit --amend
git push -f
```

To ammend those changes to the original commit.
