#!/bin/bash

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_deps_apt() {
  sudo apt-get install -y wget cmake debhelper fakeroot \
    libxml2-dev uuid-dev \
    protobuf-compiler libprotobuf-dev \
    libgsasl7-dev
}

build_pkg_install() {
  TEMP=`mktemp -d` || die "cannot create temporary directory"
  pushd $TEMP
  wget  https://github.com/Pivotal-Data-Attic/pivotalrd-libhdfs3/archive/v2.2.31.tar.gz
  tar xzvf v2.2.31.tar.gz || die "failed to extract libhdfs3 sources"
  mkdir -p pivotalrd-libhdfs3-2.2.31/build && cd pivotalrd-libhdfs3-2.2.31/build
  ../bootstrap || die "bootstrap failed"
  make debian-package || die "failed to make debian package"
  sudo dpkg -i ../../libhdfs3*.deb || die "failed to install libhdfs3 debian packages"
  popd
}

run() {
  install_deps_apt || die "failed to install libhdfs3 dependencies"
  build_pkg_install || die "failed to build and install libhdfs3"
}

run
