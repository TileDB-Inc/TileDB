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
set -xe

# Starts an Azurite server

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

run_azurite() {
  azurite-blob \
    --silent \
    --location /tmp/azurite-data \
    --debug /tmp/azurite-data/debug.log \
    --blobPort 10000 \
    --blobHost 0.0.0.0 &
  export AZURITE_PID=$!
}

run_azurite_ssl_proxy() {
  $DIR/run-ssl-proxy.py \
    --source-port 10001 \
    --target-port 10000 \
    --public-certificate /tmp/azurite-data/test_certs/public.crt \
    --private-key /tmp/azurite-data/test_certs/private.key &
  export AZURITE_SSL_PID=$!
}

run() {
  mkdir -p /tmp/azurite-data
  cp -f -r $DIR/../test/inputs/test_certs /tmp/azurite-data

  run_azurite
  run_azurite_ssl_proxy
}

run
