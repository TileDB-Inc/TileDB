FROM ubuntu:16.04
WORKDIR /tmp

# Install dependencies
RUN apt-get update -y && apt-get install -y \
    build-essential \
    wget \
    unzip \
    sudo \
    git \
    cmake \
    libssl-dev \
    openjdk-8-jre-headless \
    openssh-server \
    openssh-client \
    rsync \
    curl \
    && apt-get clean \
    && apt-get purge -y \
    && rm -rf /bar/lib/apt/lists*

# Set up environment
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
    HADOOP_HOME=/usr/local/hadoop-3.0.2 \
    HADOOP_PREFIX=/usr/local/hadoop-3.0.2 \
    USER=hadoop \
    HOME=/home/hadoop
ENV PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH \
    LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH \
    HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Add hadoop user
RUN useradd $USER -d $HOME -G sudo -s /bin/bash -p '' \
    && mkdir -p $HOME \
    && chown -R $USER:$USER $HOME

# Install hadoop
RUN curl -G -L -d "action=download" -d "filename=hadoop/common/hadoop-3.0.2/hadoop-3.0.2.tar.gz" \
	  https://www.apache.org/dyn/mirrors/mirrors.cgi -o hadoop-3.0.2.tar.gz \
	&& tar xzf hadoop-3.0.2.tar.gz -C /usr/local \
	&& chown -R $USER:$USER $HADOOP_HOME \
	&& rm -f hadoop-3.0.2.tar.gz \
    && mkdir -p /hdfsdata/tmp \
    && mkdir -p /hdfsdata/namenode \
    && mkdir -p /hdfsdata/datanode1 \
    && chown -R $USER:$USER /hdfsdata

# Set up passwordless SSH access
RUN mkdir -p $HOME/.ssh \
    && chmod 0700 $HOME/.ssh \
    && ssh-keygen -t rsa -P '' -f $HOME/.ssh/id_rsa \
    && cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys \
    && chmod 0600 $HOME/.ssh/authorized_keys \
    && echo 'NoHostAuthenticationForLocalhost yes' >> $HOME/.ssh/config \
    && chown -R $USER:$USER $HOME/.ssh

# Configure hadoop
ADD core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
ADD hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
ADD mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
ADD hadoop-env.sh $HADOOP_HOME/etc/hadoop/hadoop-env.sh
ADD entrypoint.sh /usr/local/bin/entrypoint.sh

USER $USER
WORKDIR $HOME
ENTRYPOINT /usr/local/bin/entrypoint.sh