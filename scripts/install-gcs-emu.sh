#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) 2019-2020 TileDB, Inc.
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

# Installs dependencies for gcs.

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_gcs(){
    sudo git clone https://github.com/googleapis/google-cloud-cpp/ --branch v1.22.0 /tmp/google-cloud-cpp/
    sudo pip3 install -r /tmp/google-cloud-cpp/google/cloud/storage/emulator/requirements.txt
}

install_apt_pkgs() {
  if [ -n "$(! command -v git)" ]; then
    sudo apt-get -y install git
  fi
  install_gcs
}

install_yum_pkgs() {
  sudo yum update
  sudo yum -y install git
  install_gcs
}

install_brew_pkgs() {
  if [ -n "$(command -v git)" ]; then
    install_gcs
  else
    brew install git
    install_gcs
  fi
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
