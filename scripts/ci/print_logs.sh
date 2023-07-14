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

# Prints log files (failed build only)
set -e pipefail
# Display log files if the build failed
echo "Dumping log files for failed build"
for f in $(find $GITHUB_WORKSPACE/{build,external} -name *.log);
  do
    echo "::group::$f"
    cat $f
    echo "::endgroup::"
  done;

# Display log files if the build failed
# echo "Dumping CMake files for failed build"
# for f in $(find $GITHUB_WORKSPACE/build/externals -type f -name *.cmake);
#   do
#     echo "::group::$f"
#     cat $f
#     echo "::endgroup::"
#   done;

# List files in externsl on error
find $GITHUB_WORKSPACE/build/externals | sort
