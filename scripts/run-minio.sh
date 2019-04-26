#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) 2019 TileDB, Inc.
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

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

run_cask_minio() {
  export MINIO_ACCESS_KEY=minio
  export MINIO_SECRET_KEY=miniosecretkey

  minio server --address localhost:9999 /tmp/minio-data &
  [[ "$?" -eq "0" ]] || die "could not run minio server"
}

run_docker_minio() {
  mkdir -p /tmp/minio-data
  docker run -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=miniosecretkey \
       -d -p 9999:9000 minio/minio server /tmp/minio-data || die "could not run docker minio"
  docker ps
}

export_aws_keys() {
  export AWS_ACCESS_KEY_ID=minio
  export AWS_SECRET_ACCESS_KEY=miniosecretkey
}

run() {
  export_aws_keys
  if [[ "$AGENT_OS" == "Darwin" ]]; then
    run_cask_minio
  else
    run_docker_minio
  fi
}

run
