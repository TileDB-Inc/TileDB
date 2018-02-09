#!/bin/bash

# Starts a minio server and exports credentials to the environment
# ('source' this script instead of executing).

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

run() {
  run_docker_minio
  export_aws_keys
}

run 
