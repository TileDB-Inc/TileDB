#!/bin/bash

# Starts an HDFS server and exports environment variables ('source'
# this script).

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
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

run

export JAVA_HOME=$(readlink -n \/etc\/alternatives\/java | sed "s:\/bin\/java::")
export HADOOP_HOME=/usr/local/hadoop/home
export HADOOP_LIB="$HADOOP_HOME/lib/native/"
export LD_LIBRARY_PATH="$HADOOP_LIB:$JAVA_HOME/lib/amd64/server/"
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
export PATH="${HADOOP_HOME}/bin/":$PATH
