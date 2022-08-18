
if [[ "$TILEDB_CI_BACKEND" == "S3" ]]; then
  # Start minio server if S3 is enabled
  source scripts/install-minio.sh;
  source scripts/run-minio.sh;

  if [[ "$TILEDB_CI_OS" == "Linux" ]]; then
    # make sure docker is still running...
    printenv
    docker ps -a
  fi
fi

if [[ "$TILEDB_CI_BACKEND" == "GCS" ]]; then
   # Start GCS Emulator if GCS is enabled
  source scripts/install-gcs-emu.sh;
  source scripts/run-gcs-emu.sh;
fi

if [[ "$TILEDB_CI_BACKEND" == "AZURE" ]]; then
  # Start Azurite - Azure is enabled
  source scripts/install-azurite.sh;
  source scripts/run-azurite.sh;
fi

if [[ "$TILEDB_CI_BACKEND" == "HDFS" ]]; then
  # - run: |
  # Start HDFS server if enabled
  # - ssh to localhost is required for HDFS launch...
  # - /home/vsts has permissions g+w and is owned by user 'docker'
  #   for VSTS purposes, so disable ssh strictness
  sudo sed -i.bak 's/StrictModes\ yes/StrictModes\ no/g' /etc/ssh/sshd_config

  source scripts/install-hadoop.sh
  source scripts/run-hadoop.sh
fi
