
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
