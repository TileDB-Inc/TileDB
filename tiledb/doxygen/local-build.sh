#!/bin/bash
set -x
# Builds the ReadTheDocs documentation locally.
# Usage. Execute in this directory:
#   $ ./local-build.sh
# This creates a Python virtual env 'venv' in the current directory.
#
# Conda Env:
# To create an isolated conda environment for installing / running sphinx:
#    $ conda env create -f source/conda-env.yaml
#    $ source activate tiledb-env
#    $ ./local build.sh
#
# **Note** If you get errors running the script,
# try removing the local `venv` folder and re-running the script.

SCRIPT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
TILEDB_SRC_PATH=`realpath $SCRIPT_PATH/../../`

# Choose the default directories
source_dir="$SCRIPT_PATH/source"
build_dir="$SCRIPT_PATH/source/_build"
venv_dir="$SCRIPT_PATH/venv"
cmake=`which cmake`

make_jobs=1
if [[ $OSTYPE == linux* ]]; then
  if [ -n "$(command nproc)" ]; then
    make_jobs=`nproc`
  fi
elif [[ $OSTYPE == darwin* ]]; then
  make_jobs=`sysctl -n hw.logicalcpu`
fi

# Display bootstrap usage
usage() {
echo '
Usage: '"$0"' [<options>]
Options: [defaults in brackets after descriptions]
Configuration:
    --help                          print this message
    --tiledb_build=PATH                path to existing TileDB build dir root
'
    exit 10
}

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

arg() {
  echo "$1" | sed "s/^${2-[^=]*=}//" | sed "s/:/;/g"
}

# Parse arguments
while test $# != 0; do
    case "$1" in
    --tiledb_build=*) dir=`arg "$1"`
              tiledb_build="$dir";;
    --help) usage ;;
    *) die "Unknown option: $1" ;;
    esac
    shift
done

setup_venv() {
  if [ ! -d "${venv_dir}" ]; then
    python -m venv "${venv_dir}" || die "could not create virtualenv"
  fi
  source "${venv_dir}/bin/activate" || die "could not activate virtualenv"
  pip install -r source/requirements.txt || die "could not install doc dependencies"
}

run_doxygen() {
    if [ -z ${tiledb_build+x} ]; then
      pushd "$TILEDB_SRC_PATH"
      tiledb_build="$TILEDB_SRC_PATH/build"
      build_dir_arg="-D TILEDB_BUILD_DIR=${tiledb_build}"
      if [ ! -d "build" ]; then
          mkdir build
          pushd build
          ../bootstrap || die "could not bootstrap tiledb"
          popd
      fi
    fi
    cd $tiledb_build
    make doc || die "could not build doxygen docs"
    popd
}

build_site() {
    # Note:
    #  -E disables the build cache (slower builds).
    #  -W enables warnings as errors.
    sphinx-build -E -W -T -b html -d ${build_dir}/doctrees ${source_dir} ${build_dir}/html ${BUILD_DIR_ARG:-} || \
        die "could not build sphinx site"
}

open_index() {
  if [[ $OSTYPE == linux* ]]; then
    xdg-open ${build_dir}/html/index.html
  elif [[ $OSTYPE == darwin* ]]; then
    open ${build_dir}/html/index.html
  fi
}

run() {
  setup_venv
  run_doxygen
  build_site
  # check if we are running on CI
  # if [[ -z $CI ]]; then
  #  open_index
  # fi
}

run
