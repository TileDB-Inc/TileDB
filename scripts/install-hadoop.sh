#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) 2019-2023 TileDB, Inc.
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

# Installs and configures HDFS.
set -x

HADOOP_VERSION="3.3.6"

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}


function update_apt_repo  {
  sudo apt-get install -y software-properties-common wget &&
    sudo apt-get update -y
    sudo apt-get install -y curl
} 

function install_java {
  sudo apt-get install -y openjdk-11-jre
}

function install_hadoop {
  sudo mkdir -p /usr/local/hadoop/ &&
    sudo chown -R $(whoami) /usr/local/hadoop || die "could not create local hadoop directory"
  pushd /usr/local/hadoop
  # download from closest mirror
  curl -G -L -d "action=download" \
    "https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    -o hadoop-${HADOOP_VERSION}.tar.gz
  if [ $? -ne 0 ]; then
    die "error downloading hadoop from apache mirror"
  fi;
  sudo tar xzf hadoop-${HADOOP_VERSION}.tar.gz || die "error extracting hadoop download"
  if [ -d ./home/hadoop-${HADOOP_VERSION} ]; then
     sudo rm -rf ./home/hadoop-${HADOOP_VERSION}
  fi
  sudo mv hadoop-${HADOOP_VERSION} home && sudo chown -R $(whoami) /usr/local/hadoop
  popd
}

function create_hadoop_user {
  sudo useradd -m hduser &&
    sudo adduser hduser sudo &&
    sudo chsh -s /bin/bash hduser
  echo -e "hduser123\nhduser123\n" | sudo passwd hduser

  sudo useradd -m hadoop &&
    sudo adduser hadoop sudo &&
    sudo chsh -s /bin/bash hadoop
  echo -e "hadoop123\nhadoop123\n" | sudo passwd hadoop
}

function setup_core_xml {
  export HADOOP_HOME=/usr/local/hadoop/home
  local tmpfile=/tmp/hadoop_fafsa.xml
  local file=$HADOOP_HOME/etc/hadoop/core-site.xml
  sudo rm -rf $file
  cat >> $tmpfile <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>hadoop.tmp.dir</name>
<value>/tmp/hadooop</value>
<description>Temporary directories.</description>
</property>
<property>
<name>fs.default.name</name>
<value>hdfs://localhost:9000</value>
</property>
</configuration>
EOF
  tmpfile=/tmp/hadoop_fafsa.xml
  mv $tmpfile $file
}

function setup_mapred_xml {
  export HADOOP_HOME=/usr/local/hadoop/home
  local tmpfile=/tmp/hadoop_mapred.xml
  local file=$HADOOP_HOME/etc/hadoop/mapred-site.xml
  sudo rm -rf $file
  cat >> $tmpfile <<EOT
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
<name>mapred.job.tracker</name>
<value>localhost:9010</value>
<description>The tracker of MapReduce</description>
</property>
<property>
<name>mapreduce.map.log.level</name>
<value>WARN</value>
</property>
<property>
<name>mapreduce.reduce.log.level</name>
<value>WARN</value>
</property>
</configuration>
EOT
  tmpfile=/tmp/hadoop_mapred.xml
  mv $tmpfile $file
}

function setup_hdfs_xml {
  export HADOOP_HOME=/usr/local/hadoop/home
  local tmpfile=/tmp/hadoop_hdfs.xml
  local file=$HADOOP_HOME/etc/hadoop/hdfs-site.xml
  sudo rm -rf $file
  cat >> $tmpfile <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

<property>
<name>dfs.replication</name>
<value>1</value>
</property>

<!-- libhdfs3 -->

<property>
<name>dfs.default.replica</name>
<value>1</value>
</property>

<property>
<name>output.replace-datanode-on-failure</name>
<value>false</value>
</property>

<property>
<name>dfs.client.read.shortcircuit</name>
<value>false</value>
</property>

<property>
<name>rpc.client.connect.retry</name>
<value>10</value>
</property>

<property>
<name>rpc.client.read.timeout</name>
<value>3600000</value>
</property>

<property>
<name>rpc.client.write.timeout</name>
<value>3600000</value>
</property>

</configuration>
EOF
  tmpfile=/tmp/hadoop_hdfs.xml
  mv $tmpfile $file
}

function setup_environment {
  export HADOOP_HOME=/usr/local/hadoop/home
  JAVA_HOME=$(readlink -f \/usr\/bin\/java | sed "s:bin\/java::")
  HADOOP_ENVSH=/usr/local/hadoop/home/etc/hadoop/hadoop-env.sh
  HADOOP_USER=$(whoami)

  # Make a copy
  cp -n $HADOOP_ENVSH $HADOOP_ENVSH.bk
  # Write the new one
  echo "JAVA_HOME=${JAVA_HOME}" > $HADOOP_ENVSH
  cat >> $HADOOP_ENVSH <<EOT
export HDFS_NAMENODE_USER=${HADOOP_USER}
export HDFS_DATANODE_USER=${HADOOP_USER}
export HDFS_SECONDARYNAMENODE_USER=${HADOOP_USER}
export YARN_RESOURCEMANAGER_USER=${HADOOP_USER}
export YARN_NODEMANAGER_USER=${HADOOP_USER}
EOT
  setup_core_xml &&
    setup_mapred_xml &&
    setup_hdfs_xml || die "error in generating xml configuration files"
}

function test_passwordless_ssh {
  # test the ssh setup so we don't proceed and fail later with nonspecific errors
  ssh localhost "echo 'hello world'"
}

function passwordless_ssh {
  if [ -d ~/.ssh ]; then
    rm -rf ~/.ssh
  fi
  sudo apt-get --reinstall install -y openssh-server openssh-client || die "error (re)installing openssh"
  mkdir ~/.ssh

  # reset permissions to avoid ssh errors for world-readable directory
  chmod og-rw ~

  ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  ssh-keyscan -H localhost >> ~/.ssh/known_hosts
  ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts
  ssh-keyscan -H 0.0.0.0 >> ~/.ssh/known_hosts
  sudo service ssh restart || die "error restarting ssh service"
  # sleep to make sure the ssh service restart is done because systemd
  sleep 2

  test_passwordless_ssh || die "failed to run passwordless ssh!"
}

function run {
  update_apt_repo || die "error updating apt-repo"
  passwordless_ssh || die "error setting up passwordless ssh"
  install_java || die "error installing java"
  create_hadoop_user || die "error creating hadoop user"
  install_hadoop || die "error installing hadoop"
  setup_environment || die "error setting up environment"
}

run
