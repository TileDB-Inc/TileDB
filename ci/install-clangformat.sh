#!/bin/bash

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_apt_pkg() {
  add-apt-repository 'deb http://apt.llvm.org/trusty/ llvm-toolchain-trusty-5.0 main' &&
  wget -O - http://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
  apt-get update -qq && apt-get install -qq -y clang-format-5.0
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
