# Ubuntu Trusty Docker file for base TileDB Dependencies

# This Dockerfile copies files from the parent directory, requiring a build
# context from the parent directory. From `./../..`, execute the following to
# build:
# docker build -f examples/Dockerfile/Dockerfile -t tiledb:dev .
#
# Use the build arg 'enable' to configure optional TileDB components, e.g.:
#   docker build -f examples/Dockerfile/Dockerfile --build-arg enable=s3,hdfs -t tiledb:dev

FROM ubuntu:20.04

# Optional components to enable (defaults to empty).
ARG enable

# Setup home environment
RUN useradd tiledb
RUN mkdir /home/tiledb && chown tiledb /home/tiledb
ENV HOME /home/tiledb

# Install dependencies, accepting prompts with their default value
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    wget \
    unzip \
    git \
    cmake \
    python3 \
    python3-dev \
    python3-pip \
    libssl-dev \
    && apt-get clean \
    && apt-get purge -y \
    && rm -rf /var/lib/apt/lists*

# Copy TileDB source files from the build context.
COPY . /home/tiledb/TileDB

# Install TileDB
RUN cd /home/tiledb/TileDB \
    && mkdir build \
    && cd build \
    && ../bootstrap \
        --prefix=/usr/local \
        --enable-verbose \
        --enable-azure \
        --enable-s3 \
        --enable-serialization \
        --enable-tools \
        --enable=${enable} \
    && make -j2 \
    && make -j2 examples \
    && make install-tiledb \
    && ldconfig

RUN chown -R tiledb: /home/tiledb

USER tiledb
WORKDIR /home/tiledb
ENTRYPOINT /bin/bash
