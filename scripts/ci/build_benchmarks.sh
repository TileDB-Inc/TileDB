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

# Builds CI benchmarks and checks test status
pushd $GITHUB_WORKSPACE/test/benchmarking && \
mkdir -p build && cd build && \
cmake -DCMAKE_PREFIX_PATH=$GITHUB_WORKSPACE/dist ../src && make && \
popd
# man bash
# ...
# -e The shell does not exit if the command that fails is part of the command
# list immediately following a while or until keyword, part of the test 
# following the if or elif reserved words, part of any command executed in a && 
# or || list except the command following the final && or  || ...
# ...
# given the above, explicitly check and bail.
poss_ret_exit_code=$?
if [ $poss_ret_exit_code -ne 0 ] ;  then
  return $poss_ret_exit_code 2>/dev/null
  exit $poss_ret_exit_code
fi


testfile=$(mktemp)
mv $testfile $testfile.cc
testfile=$testfile.cc
cat <<EOF > $testfile
#include <assert.h>
#include <tiledb/tiledb.h>
#include <tiledb/version.h>
int main(int argc, char **argv) {
  int major = 0;
  int minor = 0;
  int patch = 0;
  tiledb_version(&major,&minor,&patch);
  auto version = tiledb::version();
  assert(major == std::get<0>(version));
  return 0;
}
EOF
export TESTFILE_LDFLAGS="-ltiledb"
export LD_LIBRARY_PATH=$GITHUB_WORKSPACE/dist/lib:/usr/local/lib:${LD_LIBRARY_PATH:-}
$CXX -std=c++11 -g -O0 -Wall -Werror -I$GITHUB_WORKSPACE/dist/include -L$GITHUB_WORKSPACE/dist/lib $testfile -o $testfile.exe $TESTFILE_LDFLAGS && \
$testfile.exe && \
rm -f $testfile $testfile.exe

ps -U $(whoami) -o comm= | sort | uniq
#  displayName: 'Build examples, PNG test, and benchmarks (build-only)'
