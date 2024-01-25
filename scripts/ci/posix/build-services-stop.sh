# Terminate background processes to avoid job failure due to non-exit

# GCS emulator
if [[ "$TILEDB_CI_BACKEND" == "GCS" ]] && [[ "$GCS_PID" ]]; then
  kill -9 "$GCS_PID" || true # failure to stop should not fail job
  kill -9 "$GCS_SSL_PID" || true
fi

# Azure emulator
if [[ "$TILEDB_CI_BACKEND" = "AZURE" ]] && [[ "$AZURITE_PID" ]]; then
  # Kill the running Azurite server
  kill -n 9 "$AZURITE_PID" || true
  kill -n 9 "$AZURITE_SSL_PID" || true
fi

if [[ "$TILEDB_CI_BACKEND" == "S3" ]] && [[ "$TILEDB_CI_OS" == "macOS" ]]; then
  # Kill the running Minio server, OSX only because Linux runs it within
  # docker.
  kill -n 9 "$MINIO_PID"
fi
