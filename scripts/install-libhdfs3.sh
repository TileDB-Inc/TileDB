#!/bin/bash

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_depends() {
  sudo apt-get update &&
  sudo apt-get install -qq cmake debhelper fakeroot \
    libxml2-dev uuid-dev \
    protobuf-compiler libprotobuf-dev \
    libgsasl7-dev || die "cannot install dependencies"
}

build_pkg_install() {
  TEMP=`mktemp -d` || die
  git clone https://github.com/Pivotal-Data-Attic/pivotalrd-libhdfs3 $TEMP/libhdfs3 || die "cannot clone libhdfs3 repo"
  pushd $TEMP/libhdfs3
  rm -rf build && mkdir -p build && cd build || die "cannot create build directory"
  ../bootstrap || die "bootstrap failed"
  make debian-package || die "failed make debian package"
  sudo dpkg -i ../../libhdfs3*.deb || die "failed to install debian packages" 
  popd
}

run() {
  install_depends || die "failed to install libhdfs3 dependencies"
  build_pkg_install || die "failed to build and install libhdfs3"
}

run
