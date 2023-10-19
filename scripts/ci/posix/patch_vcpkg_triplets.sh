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
# Based on the comment here:
# https://github.com/microsoft/vcpkg/issues/143#issuecomment-705778244 ¯\_(ツ)_/¯
#
# The conditional logic here is to avoid re-patching the triplet files after
# they're restored by run-vcpkg. While re-patching is harmless, it breaks
# run-vcpkg's caching because it changes the generated package hashes.

TRIPLETS="${GITHUB_WORKSPACE}/external/vcpkg/triplets ${GITHUB_WORKSPACE}/ports/triplets"

find $TRIPLETS -type f -maxdepth 1 | xargs grep -q "VCPKG_BUILD_TYPE release"
if [ $? -ne 0 ]
then
  find $TRIPLETS -type f \
    -exec sh -c "echo \"\nset(VCPKG_BUILD_TYPE release)\n\" >> {}" \;
  echo "Triplets patched"
fi
