#!/bin/bash

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

build_aws_sdk_cpp() {
  git clone https://github.com/aws/aws-sdk-cpp.git
  cd aws-sdk-cpp
  git checkout 1.3.21
  mkdir build
  cd build
  export AWS_SDK_CPP=$(pwd)
  cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_ONLY="s3;core;transfer;config" \
        -DCUSTOM_MEMORY_MANAGEMENT=0 .. || die "aws-sdk-cpp build failed"
  make || die "aws-sdk-cpp build failed"
  sudo make install || die "aws-sdk-cpp installation failed"
  cd ../../
}

install_apt_pkgs() {
  sudo apt-get -y install libssl-dev || die "could not install openssl dependency"
  sudo apt-get -y install libcurl4-openssl-dev || die "could not install curl dependency"
}

install_yum_pkgs() {
  sudo yum -y install openssl-devel || "could not install openssl dependency"
  sudo yum -y install libcurl-devel || "could not install curl dependency"
}

install_brew_pkgs() {
  brew install aws-sdk-cpp || die "could not install aws-sdk-cpp dependency" 
}

install_deps() {
  if [[ $OSTYPE == linux* ]]; then
    if [ -n "$(command -v apt-get)" ]; then
      install_apt_pkgs
      build_aws_sdk_cpp
    elif [ -n "$(command -v yum)" ]; then
      install_yum_pkgs
      build_aws_sdk_cpp
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
