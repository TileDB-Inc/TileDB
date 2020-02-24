#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) 2019-2020 TileDB, Inc.
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

# Starts a azurite server and exports credentials to the environment
# ('source' this script instead of executing).

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null;

  if [[ "$BASH_SOURCE" = $0 ]]; then
    # exit when *not* sourced (https://superuser.com/a/1288646)
    exit 1;
  fi
}

run_docker_azurite() {
  docker run \
    -v /tmp/azurite-data:/data \
    -p 10000:10000 \
    mcr.microsoft.com/azure-storage/azurite \
    azurite-blob \
    --blobPort 10000 \
    --blobHost 0.0.0.0 \
    --debug /data/debug.log \
    --loose
}

run() {
  mkdir -p /tmp/azurite-data
  cp -f -r $DIR/../test/inputs/test_certs /tmp/azurite-data

  if [[ "$AGENT_OS" == "Darwin" ]]; then
    die "Azurite unsupported on OS X"
  else
    run_docker_azurite
  fi
}

run
