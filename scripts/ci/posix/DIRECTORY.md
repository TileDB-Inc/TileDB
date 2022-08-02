This directory holds common auxiliary scripts used by Github actions workflows for macOS and Linux
(see `<root>/.github/workflows/ci-linux_mac.yml`).

- `prelim.sh`: install required system packages; configure system for core dumps
- `build-services-start.sh`: start emulators (eg S3, GCS) for backend-specific testing
- `build-services-stop.sh`: stop emulators
  (this has been necessary to avoid job failure due to unexited processes)
- `print-core-dumps.sh`: find and print location of core dump files (if applicable)
- `dump-core-stacks.sh`: dump the stack traces from saved core dumps (if applicable)