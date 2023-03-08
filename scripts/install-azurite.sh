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

# Installs dependencies for Azurite.

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_apt_pkgs() {
  sudo apt-get -y install libnode-dev node-gyp libssl1.0 npm || die "could not install nodejs dependency"
}

install_yum_pkgs() {
  sudo yum -y install nodejs || die "could not install nodejs dependency"
}

install_brew_pkgs() {
  # Now installed on AZP images by default
  if [[ `which node` == "" ]]; then
    brew install node || brew link --overwrite node
  fi
}

install_deps() {
  AZURITE_PACKAGE="azurite@3.22.0"
  if [[ $OSTYPE == linux* ]]; then
    if [ -n "$(command -v apt-get)" ]; then
      install_apt_pkgs
    elif [ -n "$(command -v yum)" ]; then
      install_yum_pkgs
    fi

    sudo npm config set strict-ssl false
    sudo npm cache clean -f
    sudo npm install -g n
    sudo n stable
    sudo npm install -g $AZURITE_PACKAGE
  elif [[ $OSTYPE == darwin* ]]; then
    if [ -n "$(command -v brew)" ]; then
      install_brew_pkgs
    else
      die "homebrew is not installed!"
    fi

    npm install -g $AZURITE_PACKAGE
  else
    die "unsupported package management system"
  fi
}

run() {
  install_deps
}

run 
