# Ubuntu Docker file for TileDB

# This Dockerfile copies files from the parent directory, requiring a build
# context from the parent directory. From `./../..`, execute the following to
# build:
# docker build -f examples/Dockerfile/Dockerfile -t tiledb:dev .
#
# Use the build arg 'enable' to configure optional TileDB components, e.g.:
#   docker build -f examples/Dockerfile/Dockerfile --build-arg enable=s3,hdfs -t tiledb:dev

FROM ubuntu:22.04

# Optional components to enable (defaults to empty).
ARG enable

# Setup home environment.
RUN useradd tiledb \
    && mkdir /home/tiledb \
    && chown tiledb /home/tiledb
ENV HOME /home/tiledb

# Install dependencies, accepting prompts with their default value.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    wget \
    zip \
    autoconf \
    automake \
    libtool \
    pkg-config \
    curl \
    unzip \
    git \
    python3 \
    python3-dev \
    python3-pip \
    libssl-dev \
    && apt-get clean \
    && apt-get purge -y \
    && rm -rf /var/lib/apt/lists* \
    && pip3 install cmake

# Copy TileDB source files from the build context.
COPY . /home/tiledb/TileDB

# Build and install TileDB.
RUN cd /home/tiledb/TileDB \
    && mkdir build \
    && cd build \
    && ../bootstrap \
        --prefix=/usr/local \
        --enable-verbose \
        --enable-vcpkg \
        --enable-azure \
        --enable-s3 \
        --enable-serialization \
        --enable-tools \
        --enable=${enable} \
    && make -j$(nproc) \
    && make -j$(nproc) examples \
    && make install-tiledb \
# Update shared library links/cache.
    && ldconfig \
# Take recursive ownership of the home directory.
    && chown -R tiledb: /home/tiledb

USER tiledb
WORKDIR /home/tiledb
ENTRYPOINT /bin/bash
