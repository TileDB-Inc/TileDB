# GitHub Actions Workflows

This directory contains GitHub Actions ("GHA") workflows for code quality checks, documentation
generation, and continuous integration testing.

## CI

The primary entrypoint for CI is `ci-linux_mac.yml`. This file uses GHA
[`workflow_call` reusable workflows](https://docs.github.com/en/actions/using-workflows/reusing-workflows) to parametrically run CI jobs when trigger conditions are matched.

- See `paths-ignore` sections for exclusions: commits touching only the listed patterns will
not trigger full CI.
- macOS and linux jobs are triggered through a common, parametrized workflow file in
  `workflows/ci-linux_mac.yml` which uses auxiliary scripts located in `<root>/scripts/ci/posix`
  to do common setup tasks.
- Windows jobs are *not yet* parametrized, but are called via `workflow_call` dispatch in
  order to provide a single workflow matrix view per build in the GitHub UI (in particular, this allows canceling all CI jobs for a given build trigger with one button push).
- These jobs run the `tiledb_unit` agglomerated test suite.

### Standalone unit tests

The `unit-test-runs.yml` workflow runs all `unit_*` tests in the directory tree.

### Backward compatibility tests

Backward compatibility tests are run by the workflow `workflows/build-backwards-compatibility.yml`. To reduce CI load, this workflow uses a single job to build `tiledb_unit`, then stores that artifact. The next step creates build jobs for each compatibility
array version. The pre-built `tiledb_unit` is downloaded from artifact storage and used to execute
the version-specific tests. This fan-out setup reduces the runtime of each job from ~25 minutes to
10.

## Documentation

Documentation is built by `workflows/build-docs.yml` using `<root>/scripts/ci/build_docs.sh`.
