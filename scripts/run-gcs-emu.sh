#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) 2021 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

# Starts a GCS Emulator Server and exports credentials to the environment
# ('source' this script instead of executing).
# This script should be sourced from tiledb/build folder
set -xe

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export_gcs_env(){
  export CLOUD_STORAGE_EMULATOR_ENDPOINT=http://localhost:9000 # For JSON and XML API
}

run_gcs(){
  gunicorn --bind "localhost:9000" --worker-class sync --threads 10 --access-logfile - --chdir /tmp/google-cloud-cpp-1.23.0/google/cloud/storage/emulator "emulator:run()" &
  export GCS_PID=$!
  [[ "$?" -eq "0" ]] || die "could not run gcs server"
}

run() {
  export_gcs_env

  mkdir -p /tmp/gcs-data
  cp -f -r $DIR/../test/inputs/test_certs /tmp/gcs-data
  run_gcs
}

run
