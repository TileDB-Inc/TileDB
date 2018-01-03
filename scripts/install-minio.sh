#!/bin/bash

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

run_docker_minio() {
  mkdir -p /tmp/minio-data
  docker run -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=miniosecretkey \
       -d -p 9999:9000 minio/minio server /tmp/minio-data || die "could not run docker minio"
}

export_aws_keys() {
  export AWS_ACCESS_KEY_ID=minio
  export AWS_SECRET_ACCESS_KEY=miniosecretkey
}

install_apt_pkgs() {
  sudo apt-get -y install docker || die "could not install docker dependency"
}

install_yum_pkgs() {
  sudo yum -y install docker || die "could not install docker dependency"
}

install_brew_pkgs() {
  brew install docker
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
  run_docker_minio
  export_aws_keys
}

run 
