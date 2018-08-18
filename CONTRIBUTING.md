# Contributing to TileDB

Hi! Thanks for your interest in TileDB. The following notes are intended to
help you file issues, bug reports, or contribute code to the open source TileDB project.

## Contribution Checklist:

* Reporting a bug?  Please read [how to file a bug report](https://github.com/TileDB-Inc/TileDB/blob/dev/CONTRIBUTING.md#reporting-a-bug) section to make sure sufficient information is included.

* Contributing code? You rock! Be sure to [review the contributor section](https://github.com/TileDB-Inc/TileDB/blob/dev/CONTRIBUTING.md#contributing-code) for helpful tips on the tools we use to build TileDB, format code, and issue pull requests (PR)'s.

## Reporting a Bug

A useful bug report filed as a GitHub issue provides information about how to reproduce the error.

1. Before opening a new [GitHub issue](https://github.com/TileDB-Inc/TileDB/issues) try searching the existing issues to see if someone else has already noticed the same problem.

2. When filing a bug report, provide where possible:
  - The version TileDB (`tiledb_version()`) or if a `dev` version, specific commit that triggers the error.
  - The full error message, including the backtrace (if possible).  Verbose error reporting is enabled by building TileDB with `../bootstrap --enable-verbose`.
  - A minimal working example, i.e. the smallest chunk of code that triggers the error. Ideally, this should be code that can be a small reduced C / C++ source file. If the code to reproduce is somewhat long, consider putting it in a [gist](https://gist.github.com).


3. When pasting code blocks or output, put triple backquotes (\`\`\`) around the text so GitHub will format it nicely. Code statements should be surrounded by single backquotes (\`). See [GitHub's guide on Markdown](https://guides.github.com/features/mastering-markdown) for more formatting tricks.

## Contributing Code

*By contributing code to TileDB, you are agreeing to release it under the [MIT License](https://github.com/TileDB-Inc/TileDB/tree/dev/LICENSE).*

### Quickstart Workflow:

[From a fork of TileDB](https://help.github.com/articles/fork-a-repo/)

    git clone https://github.com/username/TileDB
    cd TileDB && mkdir build && cd build
    ../bootstrap
    make && make check
    cd ../
    git checkout -b <my_initials>/<my_bugfix_branch>
    ... code changes ...
    make -C build format
    make -C build check
    git commit -a -m "my commit message"
    git push --set-upstream origin <my_initials>/<my_bugfix_branch>

[Issue a PR from your updated TileDB fork](https://help.github.com/articles/creating-a-pull-request-from-a-fork/)

Branch conventions:
- `dev` is the development branch of TileDB, all PR's are merged into `dev`.
- `master` tracks the latest stable / released version of TileDB.
- `release-x.y.z` are major / bugfix release branches of TileDB.

Formatting conventions:
- 2 spaces per indentation level not tabs
- class names use CamelCase
- member functions, variables use lowercase with underscores
- class member variables use a trailing underscore `foo_`
- comments are good, TileDB uses [doxygen](http://www.stack.nl/~dimitri/doxygen/manual/docblocks.html) for class doc strings.
- format code using [clang-format](https://clang.llvm.org/docs/ClangFormat.html)

### Pull Requests:

- `dev` is the development branch, all PR’s should be rebased on top of the latest `dev` commit.

- Commit changes to a local branch.  The convention is to use your initials to identify branches: (ex. “Fred Jones” , `fj/my_bugfix_branch`).  Branch names should be identifiable and reflect the feature or bug that they want to address / fix. This helps in deleting old branches later.

- Make sure the test suite passes by running `make check`.

- When ready to submit a PR, `git rebase` the branch on top of the latest `dev` commit.  Be sure to squash / cleanup the commit history so that the PR preferably one, or a couple commits at most.  Each atomic commit in a PR should be able to pass the test suite.

- Run the limiting / code format tooling (`make format`) before submitting a final PR.
  Make sure that your contribution generally follows the format and naming conventions used by surrounding code.

- If the PR changes/adds/removes user-facing API or system behavior (such as API changes), add a note to the `TileDB/HISTORY.md` file.

- Submit a PR, writing a descriptive message.  If a PR closes an open issue, reference the issue in the PR message (ex. If an issue closes issue number 10, you would write `closes #10`)

- Make sure CI is passing for your PR:
  - [Travis](https://travis-ci.org/TileDB-Inc/TileDB) Linux / OSX CI
  - [Appveyor](https://ci.appveyor.com/project/StavrosPapadopoulos/tiledb/branch/dev) Windows CI

### Documentation Pull Requests:
 - TileDB uses [Sphinx](http://www.sphinx-doc.org/en/master/) as its documentation generator.
 - Documentation is written in [reStructuredText](http://docutils.sourceforge.net/rst.html) markup.

Building the docs locally:

    $ cd TileDB/doc
    $ ./local-build.sh

This will install all the required packages in a Python virtual environment, and build the docs locally. You can then open the `doc/source/_build/html/index.html` file in your browser.

**Note:** If there are additions to the C/C++ API in between builds, rerun bootstrap:

    $ cd ../build
    $ ../bootstrap <flags>

### Resources

* TileDB
  - [Homepage](https://tiledb.io)
  - [Documentation](https://docs.tiledb.io/en/latest/)
  - [Issues](https://github.com/TileDB-Inc/TileDB/issues)
  - [Forum](https://forum.tiledb.io/)
  - [Organization](https://github.com/TileDB-Inc/)


* Github / Git
  - [Git cheatsheet](https://services.github.com/on-demand/downloads/github-git-cheat-sheet/)
  - [Github Documentation](https://help.github.com/)
  - [Forking a Repo](https://help.github.com/articles/fork-a-repo/)
  - [More Learning Resources](https://help.github.com/articles/git-and-github-learning-resources/)
