#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) 2019-2021 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal # in the Software without restriction, including without limitation the rights
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

# Starts an HDFS server and exports environment variables ('source'
# this script).

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

super_export() {
  # exports to current shell and environment
  # persists across task steps in azure
  # args:
  #   var: variable name
  #   val: variable value
  var=$1
  val=$2
  export $var="$val"
  echo "##vso[task.setvariable variable=$var;]$val"
}

function restart_hadoop {
  export HADOOP_HOME=/usr/local/hadoop/home
  $HADOOP_HOME/sbin/stop-dfs.sh || die "error stopping datanode"
  $HADOOP_HOME/bin/hdfs namenode -format || die "error formatting hadoop namenode"
  $HADOOP_HOME/sbin/start-dfs.sh || die "error starting datanode"
}

function run {
  restart_hadoop || die "error restarting hadoop"
}

super_export JAVA_HOME $(readlink -n \/etc\/alternatives\/java | sed "s:\/bin\/java::")
super_export HADOOP_HOME /usr/local/hadoop/home
super_export HADOOP_LIB "$HADOOP_HOME/lib/native/"
super_export LD_LIBRARY_PATH "$HADOOP_LIB:$JAVA_HOME/lib/amd64/server/"
super_export CLASSPATH `$HADOOP_HOME/bin/hadoop classpath --glob`
super_export PATH "${HADOOP_HOME}/bin/":$PATH

#
run
