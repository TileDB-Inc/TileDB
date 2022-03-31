#!/bin/bash

#
# The MIT License (MIT)
#
# Copyright (c) 2021 TileDB, Inc.
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
# The gcs emulator requires system to support python3 and pip3 accordingly
die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_gcs(){
    curl -L  https://github.com/googleapis/google-cloud-cpp/archive/v1.23.0.tar.gz > /tmp/google-cloud-cpp.tar.gz
    tar -xf /tmp/google-cloud-cpp.tar.gz -C /tmp
    #on pip3 install, github error, "The unauthenticated git protocol on port 9418 is no longer supported."
    #checked later versions, latest at check was v1.35.0, but found starting
    #at v1.32.0 directory structure had changed and there is no longer an 'emulator' directory.
    #v1.31.1 is last version that had same structure, and its requirements.txt still using now bad git+git:,
    #so, staying with our current version and patching to different protocol,
    #since there's currently only one instance in v1.23.0 emulator/requirements.txt,
    #patch git+git:// ==> git+https://
    #sed -i fails on GA CI macos...
    sed 's/git+git:/git+https:/' /tmp/google-cloud-cpp-1.23.0/google/cloud/storage/emulator/requirements.txt > /tmp/tdbpatchedrequirements.txt
    echo 'itsdangerous==2.0.1' >> /tmp/tdbpatchedrequirements.txt
    echo 'jinja2<3.1 # pinned due to incompatibility with gcs emulator 1.23' >> /tmp/tdbpatchedrequirements.txt
    echo 'werkzeug<2.1 # pinned due to incompatibility with gcs emulator 1.23' >> /tmp/tdbpatchedrequirements.txt
    cp -f /tmp/tdbpatchedrequirements.txt /tmp/google-cloud-cpp-1.23.0/google/cloud/storage/emulator/requirements.txt
    pip3 install -r /tmp/google-cloud-cpp-1.23.0/google/cloud/storage/emulator/requirements.txt
}

install_apt_pkgs() {
  pip3 install --upgrade pip
  sudo apt-get -y install python3-setuptools
  install_gcs
}

install_yum_pkgs() {
  sudo yum -y install python3-setuptools
  install_gcs
}

install_brew_pkgs() {
    install_gcs
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
