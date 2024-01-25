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

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_apt_pkg() {
  wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
  add-apt-repository 'deb http://apt.llvm.org/focal/ llvm-toolchain-focal-16 main' &&
  apt-get update -qq && apt-get install -qq -y clang-format-16
}

install_brew_pkg() {
  brew upgrade && brew install clang-format
}

install_clang_format() {
  if [[ $OSTYPE == linux* ]]; then
    if [ -n "$(command -v apt-get)" ]; then
      install_apt_pkg || die "could not install apt clang format package"
    else
      die "unsupported Linux package management system"
    fi
  elif [[ $OSTYPE == darwin* ]]; then
    if [ -n "$(command -v brew)" ]; then
      install_brew_pkg || die "could not install brew clang format package"
    else
      die "homebrew is not installed!"
    fi
  else
    die "unsupported OS"
  fi
}

run() {
  install_clang_format
}

run
