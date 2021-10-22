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
set -xeuo pipefail

# Build and test libtiledb

# Set up arguments for bootstrap.sh
bootstrap_args="${bootstrap_args} --enable=verbose";

mkdir -p $GITHUB_WORKSPACE/build
cd $GITHUB_WORKSPACE/build

# Configure and build TileDB
echo "Bootstrapping with '$bootstrap_args'"
$GITHUB_WORKSPACE/bootstrap $bootstrap_args

make -j4
make examples -j4
make -C tiledb install

#- run: |
cd $GITHUB_WORKSPACE/build
ls -la

make -j4 -C tiledb tiledb_unit
