#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) 2019-2021 TileDB, Inc.
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

# Starts a minio server and exports credentials to the environment
# ('source' this script instead of executing).

# Define MINIO_VERSION to default if not set
MINIO_VERSION=${MINIO_VERSION:="RELEASE.2024-02-09T21-25-16Z"}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if [[ "$BASH_SOURCE" = $0 ]]; then
  # exit when *not* sourced (https://superuser.com/a/1288646)
  echo "run-minio.sh was not sourced. Please set keys manually before running tests."
fi

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null;
}

run_cask_minio() {
  # note: minio data directories *must* follow parameter arguments
  minio server --certs-dir=/tmp/minio-data/test_certs --address 127.0.0.1:9999 /tmp/minio-data &
  export MINIO_PID=$!
  [[ "$?" -eq "0" ]] || die "could not run minio server"
}

run_docker_minio() {
  # note: minio data directories *must* follow parameter arguments
  docker run -v /tmp/minio-data:/tmp/minio-data \
       -e MINIO_ROOT_USER=minio -e MINIO_ROOT_PASSWORD=miniosecretkey \
       -d -p 9999:9000 \
       minio/minio:${MINIO_VERSION} server -S /tmp/minio-data/test_certs \
         /tmp/minio-data || die "could not run docker minio"
}

export_aws_keys() {
  export AWS_ACCESS_KEY_ID=minio
  export AWS_SECRET_ACCESS_KEY=miniosecretkey
  export MINIO_ROOT_USER=minio
  export MINIO_ROOT_PASSWORD=miniosecretkey
}

run() {
  export_aws_keys

  mkdir -p /tmp/minio-data
  cp -f -r $DIR/../test/inputs/test_certs /tmp/minio-data
  if [[ $OSTYPE == darwin* ]]; then
    run_cask_minio
  else
    run_docker_minio
  fi
}

run
