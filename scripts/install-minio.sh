#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) 2019-2021 TileDB, Inc.
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

# Installs dependencies for minio.

# Define MINIO_VERSION to default if not set
MINIO_VERSION=${MINIO_VERSION:="RELEASE.2024-02-09T21-25-16Z"}

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_apt_pkgs() {
  if ! command -v docker &> /dev/null
  then
    sudo apt-get -y install docker || die "could not install docker dependency"
  fi
}

install_yum_pkgs() {
  if ! command -v docker &> /dev/null
  then
    sudo yum -y install docker || die "could not install docker dependency"
  fi
}

install_brew_pkgs() {
  # use minio repo rather than cask,
  # as recommended by minio
  #brew install minio/stable/minio

  # Use direct installation due to failed download from homebrew
  curl --output minio https://dl.min.io/server/minio/release/darwin-amd64/archive/${MINIO_VERSION}
  chmod +x ./minio
  sudo mv ./minio /usr/local/bin/
}

install_deps() {
  if [[ $OSTYPE == linux* ]]; then
    if [ -n "$(command -v apt-get)" ]; then
      install_apt_pkgs
    elif [ -n "$(command -v yum)" ]; then
      install_yum_pkgs
    fi
  elif [[ $OSTYPE == darwin* ]]; then
    if [ -n "$(command -v brew)" ]; then
      install_brew_pkgs
    else
      die "homebrew is not installed!"
    fi
  else
    die "unsupported package management system"
  fi
}

run() {
  install_deps
}

run 
