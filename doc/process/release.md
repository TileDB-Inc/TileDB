# Releases

## Release Cycle

A release manager (RM) is appointed at the beginning of the release cycle.  RMs are responsible for determining what features go into the next release as the feature window closes, delegating tasks during the release cycle, making sure tasks are completed, and finalizing the release to ensure it is shipped on time.

Major release cycles are **6** weeks:

- **6** Weeks for feature development
  - The release manager dictates what features will make it into this feature window, and what are pushed to the next major release.
- **2** Weeks after feature freeze for bug fixes, documentation, and preparing the release
  - The dev branch is branched and tagged as a “pre-release”,  all bug fixes and docs are backported to this branch from dev.

**Timeline:**

`Feature development  → Tag pre-release → Bug Fixes / Docs → Major Release`

## Creating a Major/Minor Release

Major/minor releases happen according to the release cycle. The process is similar to creating a patch release. It is the responsibility of the release manager to delegate tasks and ensure that major releases are shipped on time.

**Terms:**

- `x.y.z` relates to the symver spec release number (major, minor, patch)
- `release-x.y.z` is the in preparation release branch (or new release), For example the if previous release was `1.2.1` this will be `release-1.3.0`.

### Process
- Prepare a change for `dev` with the following changes:
  - Ensure `HISTORY.md` is up-to-date with the version number in the title, and a list of additions/changes/breaking changes/etc.
  - Ensure the docs version in `doc/source/conf.py` is set to `x.y.z` in the release branch, and the docs text-substitution links for the example programs point to the `x.y.z` GitHub URL.
  - Ensure the library version in `tiledb/sm/c_api/tiledb_version.h` is set to `x.y.z`.
  - If there were format changes, ensure that the `format_version` was properly incremented in `tiledb/sm/misc/constants.cc` (this should preferably be done as a part of format change PRs). Ensure the same version number is listed in `format_spec/FORMAT_SPEC.md`. If the format changed, ensure that we have already created backwards-compatibility arrays and put them under test, instructions [here](https://github.com/TileDB-Inc/TileDB/wiki/Backwards-Compatibility-Test-for-Arrays).
- When these changes are in `dev`, create a new branch `release-x.y` from branch `dev`.

### Pre-release

Use the [Github Release UI](https://github.com/TileDB-Inc/TileDB/releases/new) to tag a new release on branch `release-x.y`.
The new tag must be `x.y.z-rc1` (**important**: we treat tags as immutable, double check that this is correct and includes an `-rcX` postfix). Mark the release as a “pre-release” (a checkbox in the Github UI when creating the release). Set the title of the release to `TileDB vx.y.z-rc1`. This will allow testing and fixes to occur using the `x.y.z-rc1` tag itself while warning users that the release is not yet finalized. Click “Publish release”. Increment the `rc1` to `rc2` for release candidate 2, etc.

### Release

Once the pre-release has been tested, use the [Github Release UI](https://github.com/TileDB-Inc/TileDB/releases/new) to tag a new release on branch `release-x.y`. The new tag must be `x.y.z`. Set the title of the release to `TileDB vx.y.z`.

### Update downstream build products

Follow the same process as documented for [patch releases](#creating-a-patch-release). If these reveal any bugfixes that **must** be incorporated into the `x.y.z` release, push these fixes first as normal PRs to `dev` and then cherry-pick them onto the `release-x.y.z` branch (follow the same process as backporting regular bugfixes).

Low-priority bugfixes should be opened as issues on Github and assigned to the next `x.y.(z+1)` patch release.

### Increment patch version in dev

After releasing a major version x.y.z, open a PR onto the dev branch with the following updates:

- Update the version number in `tiledb/sm/c_api/tiledb_version.h` to the next patch version. For example, if `1.5.0` was just released, update the version number to `1.5.1` in dev.
- Update the version number in `doc/source/conf.py`
- Update the HISTORY.md file with the release notes from the release-x.y.z branch and create above it a new "In progress" section (which will be empty).

### Announce the release

- Create a post in the following venues announcing the release:
  - [TileDB Forum](https://forum.tiledb.com)
  - TileDB Community Slack
  - Internal `#core` and `#release` slack channels

## Creating a Patch Release

Patch (bug fix) releases happen during the release cycle.  Bug fixes identified on the dev branch are backported to a point stable release branch.  It is the responsibility of the release manager to delegate tasks and ensure that patch releases are shipped on time.

The goal is to have a roughly ~2 week bugfix window between point releases.  As development shifts to the next major release (3 month mark) the patch release cycle ends and all effort is shifted to shipping the next major release.

**Timeline:**

`Major release → bugfix window → pt release → bugfix window → pt release → Major pre-release`

### Process

- When a PR is identified to be backported, add a label `backport release-x.y` where `release-x.y` is an existing minor-release branch. The backport bot will automatically create a PR against this branch after the PR has been merged into `dev`.
- In `dev` and `release-x.y`, ensure the patch version number is correct in `tiledb/sm/c_api/tiledb_version.h`
- In `dev` and `release-x.y`, Ensure the patch version number is correct in `doc/source/conf.py`
- Copy the "In Progress" section to "TileDB vx.y.z Release Notes" in `HISTORY.md`. Remove empty sub-section headers. Open a PR against `dev` and backport it to `release-x.y`.

### Tag the release

Use the [Github Release UI](https://github.com/TileDB-Inc/TileDB/releases/new) to tag a new release on branch `release-x.y`.
The new tag must be `x.y.z`

### Update downstream build products

After the release is tagged, the version numbers and binaries of build artifacts have to be updated.

**Update TileDB Docker Images**

- Within the core TileDB repo, checkout the `x.y.z` tag.
- Build the dockerfile with the appropriate tag:
  - `docker build -f examples/Dockerfile/Dockerfile --no-cache -t tiledb:2.2.1 .`
- Test the build image:
  - `docker run -it tiledb:x.y.z`
  - `./TileDB/build/tiledb/examples/c_api/quickstart_dense_c`
  - `exit`
- Push the locally built image to DockerHub:
  - `docker login`
  - `docker images`
    - Note the IMAGE ID of the image with TAG `x.y.z`
  - `docker tag <IMAGE ID> tiledb/tiledb:x.y.z`
  - `docker push tiledb/tiledb:<x.y.z>`
- Verify tagged build: https://hub.docker.com/r/tiledb/tiledb/tags?page=1&ordering=last_updated
- Update tag `latest` to point to tag `x.y.z`
  - `docker tag <IMAGE ID> tiledb/tiledb:latest`
  - `docker push tiledb/tiledb:latest`
- Verify tagged build: https://hub.docker.com/r/tiledb/tiledb/tags?page=1&ordering=last_updated

**Update Homebrew  package version**

- Clone the https://github.com/TileDB-Inc/homebrew-stable repo locally
- Create a new release branch
  - `git checkout master`
  - `git pull`
  - `git checkout -b release-x.y.z`
- Download the binary release tar.gz archive
  - You can find the URL within https://github.com/TileDB-Inc/TileDB/releases/
  - `wget https://github.com/TileDB-Inc/TileDB/releases/download/x.y.z/tiledb-macos-x.y.z-hash-full.tar.gz`
- Compute the SHA 256 of the download:
  - `openssl dgst -sha256 tiledb-macos-x.y.z-hash-full.tar.gz`
- Remove the download:
  - `rm tiledb-macos-x.y.z-hash-full.tar.gz`
- Update the `url` , `sha256` , and `version` fields in `tiledb.rb`
- Commit the changes
  - `git commit -am "Update to version x.y.z"`
- Test the updates locally
  - `brew uninstall tiledb`
  - `brew install ./tiledb.rb`
  - `brew test ./tiledb.rb`
  - `brew info ./tiledb.rb`
- Push the new release branch, open a PR.
  - `git push --set-upstream origin release-<x.y.z>`
- Once CI passes, merge the release branch into `master`, **do not delete** the release branch

**Update Conda-forge package**

Conda-forge has a bot that listens to release events on Github, the version should be bumped automatically. This may take a day or so and you should verify that it went through:

- https://github.com/conda-forge/tiledb-feedstock/pulls
- https://github.com/conda-forge/tiledb-py-feedstock
- https://github.com/conda-forge/aws-sdk-cpp-feedstock/pulls

 The below manual instructions are if the bot cannot do this automatically / fails for some reason:

- Clone the https://github.com/TileDB-Inc/tiledb-feedstock repo locally

It is very important that a PR to bump the version is submitted to the TileDB-Inc **FORK** of the conda-forge tiledb-feedstock repo and not to the original feedstock repo itself.  This prevents build products from getting pushed until the PR is merged.


- We need to update the tiledb-feedstock repo fork to match the upstream repo:
  - `git remote add upstream https://github.com/conda-forge/tiledb-feedstock`
  - `git fetch upstream`
  - `git checkout master`
  - `git merge upstream/master`
  - `git push origin`
- Create a new release branch
  - `git checkout -b release-<x.y.z>`
- Download the patch release tar.gz source archive
  - `wget https://github.com/TileDB-Inc/TileDB/archive/<x.y.z>.tar.gz`
- Compute the SHA 256 of the download:
  - `openssl dgst -sha256 TileDB-<x.y.z>.tar.gz`
- Update the `version`,  `sha256`  and `build: number` fields
  - `tiledb-feedstock/recipe/meta.yaml`
  - As this is a new release set the `build: number` back to 0
- Commit the changes
  - `git commit -am "Update to version <x.y.z)>"`
- Test the changes locally
  -  `CONFIG="linux_" ./circleci/run_docker_build.sh`
- Push the release branch to the TileDB-Inc feedstock **FORK**
  - `git push --set-upstream origin release-<x.y.z>`
- Submit a PR to the conda-forge upstream repo
  - https://github.com/TileDB-Inc/tiledb-feedstock
- Merge the conda-forge PR after CI passes
  - https://github.com/conda-forge/tiledb-feedstock/pulls
- Ensure that the updated builds show up on Anaconda cloud:
  - https://anaconda.org/conda-forge/tiledb

The above instructions need to be (optionally) repeated with the aws-cpp-sdk conda package dependency that TileDB, Inc. maintains:

- https://github.com/conda-forge/aws-sdk-cpp-feedstock/

Re-render feedstock (in the toplevel directory of feedstock repo):

- `conda update -n root -c conda-forge conda-forge-pinning`
- `conda update -n root -c conda-forge conda-smithy`
- `conda smithy rerender`

Notes:
- To locally test a conda recipe, install the [`conda-build`](https://docs.conda.io/projects/conda-build/en/latest/) package, activate that environment, and run `conda build .` within the `recipe` sub-directory. When building the tiledb-py-feedstock recipe against upstream conda-forge/tiledb, you must use `conda build -c conda-forge .` to add conda-forge to the dependency search channels.
- `conda-smithy` depends on git, and will install git from conda-forge. The conda-forge git package has had problems building and installing the `osxkeychain` credential helper in the past, which may cause unexpected authentication failures if running `git push` within the same environment as conda-smithy. In that case, you must use git from a different shell session (where the conda-smithy environment has not been activated).
- One reason the tiledb-py-feedstock conda package is built against the PyPI package (vs a `github/REPO/archive/*` bundle) is that the PyPI upload supplies the necessary metadata for `setuptools_scm` to determine version information. Building against a bare archive from github does not provide sufficient metadata (a git clone will work, using the `git_url` and `git_rev` options).