#!/bin/bash

sudo /etc/init.d/ssh start

export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`

$HADOOP_HOME/sbin/stop-dfs.sh || exit 1
$HADOOP_HOME/bin/hdfs namenode -format || exit 1
$HADOOP_HOME/sbin/start-dfs.sh || exit 1

echo ""
echo "Ready."

/bin/bash