#derived from step in old azure-linux_mac.yml for use in github actions CI operations
echo "BEGIN, inside GA-linux-mac_buildtiledb.sh"
echo ${0}
set -x
printenv

# enable core dumps
ulimit -c               # should output 0 if disabled
ulimit -c unlimited     # Enable core dumps to be captured (must be in same run block)
ulimit -c               # should output 'unlimited' now
# Azure sets "SYSTEM=build" for unknown reasons, which breaks the OpenSSL configure script
#   - openssl configure uses ENV{SYSTEM} if available:
#     https://github.com/openssl/openssl/blob/6d745d740d37d680ff696486218b650512bbbbc6/config#L56
#   - error description:
#     https://developercommunity.visualstudio.com/content/problem/602584/openssl-build-error-when-using-pipelines.htm
unset SYSTEM

# azure bash does not treat intermediate failure as error
# https://github.com/Microsoft/azure-pipelines-yaml/issues/135
set -e pipefail

git config --global user.name 'Azure Pipeline'
git config --global user.email 'no-reply@tiledb.io'

BUILD_REPOSITORY_LOCALPATH=$GITHUB_WORKSPACE

if [[ "$BACKWARDS_COMPATIBILITY_ARRAYS" == "ON" ]]; then
  git clone https://github.com/TileDB-Inc/TileDB-Unit-Test-Arrays.git --branch 2.3.0 test/inputs/arrays/read_compatibility_test
fi
#   displayName: 'Clone Unit-Test-Arrays'

# - bash: |
# Start HDFS server if enabled
if [[ "$TILEDB_HDFS" == "ON" ]]; then
  # - ssh to localhost is required for HDFS launch...
  # - /home/vsts has permissions g+w and is owned by user 'docker'
  #   for VSTS purposes, so disable ssh strictness
  sudo sed -i "s/StrictModes\ yes/StrictModes\ no/g" /etc/ssh/sshd_config

  source scripts/install-hadoop.sh
  source scripts/run-hadoop.sh
fi

# Start minio server if S3 is enabled
if [[ "$TILEDB_S3" == "ON" ]]; then
  source scripts/install-minio.sh;
  source scripts/run-minio.sh;
fi

# Start Azurite if Azure is enabled
if [[ "$TILEDB_AZURE" == "ON" ]]; then
  source scripts/install-azurite.sh;
  source scripts/run-azurite.sh;
fi

# Start GCS Emulator if GCS is enabled
if [[ "$TILEDB_GCS" == "ON" ]]; then
  source scripts/install-gcs-emu.sh;
  source scripts/run-gcs-emu.sh;
fi

BUILD_BINARIESDIRECTORY=${BUILD_BINARIESDIRECTORY:-$BUILD_REPOSITORY_LOCALPATH/dist}
mkdir -p ${BUILD_BINARIESDIRECTORY}

# Set up arguments for bootstrap.sh
bootstrap_args="--enable=verbose";

# Enable TILEDB_STATIC by default
[ "$TILEDB_STATIC" ] || TILEDB_STATIC=ON
if [[ "$TILEDB_STATIC" == "ON" ]]; then
  bootstrap_args="${bootstrap_args} --enable-static-tiledb";
fi
if [[ "$TILEDB_HDFS" == "ON" ]]; then
  bootstrap_args="${bootstrap_args} --enable-hdfs";
fi;
if [[ "$TILEDB_S3" == "ON" ]]; then
  bootstrap_args="${bootstrap_args} --enable-s3";
fi;
if [[ "$TILEDB_AZURE" == "ON" ]]; then
  bootstrap_args="${bootstrap_args} --enable-azure";
fi;
if [[ "$TILEDB_GCS" == "ON" ]]; then
  bootstrap_args="${bootstrap_args} --enable-gcs";
fi;
if [[ "$TILEDB_TOOLS" == "ON" ]]; then
  bootstrap_args="${bootstrap_args} --enable-tools";
fi
if [[ "$TILEDB_DEBUG" == "ON" ]]; then
  bootstrap_args="${bootstrap_args} --enable-debug";
fi
if [[ "$TILEDB_CI_ASAN" == "ON" ]]; then
  # Add address sanitizer flag if necessary
  bootstrap_args="${bootstrap_args} --enable-sanitizer=address --enable-debug";
fi
if [[ "$TILEDB_CI_TSAN" == "ON" ]]; then
  # Add thread sanitizer flag if necessary
  bootstrap_args="${bootstrap_args} --enable-sanitizer=thread --enable-debug";
fi
if [[ "$TILEDB_SERIALIZATION" == "ON" ]]; then
  # Add serialization flag if necessary
  bootstrap_args="${bootstrap_args} --enable-serialization";
fi
if [[ "$TILEDB_FORCE_BUILD_DEPS" == "ON" ]]; then
  # Add superbuild flag
  bootstrap_args="${bootstrap_args} --force-build-all-deps";
fi
if [[ "$AGENT_OS" == "Darwin" ]]; then
  # We want to be able to print a stack trace when a core dump occurs
  sudo chmod 1777 /cores
  bootstrap_args="${bootstrap_args} --enable-release-symbols";
fi

# displayName: 'Install dependencies'

mkdir -p $BUILD_REPOSITORY_LOCALPATH/build
cd $BUILD_REPOSITORY_LOCALPATH/build

# Configure and build TileDB
echo "Bootstrapping with '$bootstrap_args'"
$BUILD_REPOSITORY_LOCALPATH/bootstrap $bootstrap_args

make -j4
make examples -j4
make -C tiledb install

#- bash: |
cd $BUILD_REPOSITORY_LOCALPATH/build
ls -la

if [[ ( "$AGENT_OS" == "Linux" && "$TILEDB_S3" == "ON" ) ]]; then
  # make sure docker is still running...
  printenv
  docker ps -a
fi

make -j4 -C tiledb tiledb_unit

if [[ "$TILEDB_CI_ASAN" == "ON" ]]; then
  export ASAN_OPTIONS=detect_leaks=0 LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.5
fi

if [[ "$AGENT_OS" == "Darwin" && "$TILEDB_GCS" == "ON" ]]; then
  # GCS unit tests are temporarily unsupported on CI for MACOS. Fake success with
  # this echo.
  echo "##vso[task.setvariable variable=TILEDB_CI_SUCCESS]1"
else
  # run directly the executable, cmake catches the segfault and blocks
  # the core dump
  ./tiledb/test/tiledb_unit -d yes
fi

# Kill the running Minio server, OSX only because Linux runs it within
# docker.
if [[ ( "$AGENT_OS" == "Darwin" && "$TILEDB_S3" == "ON" ) ]]; then
  kill -9 $MINIO_PID
fi

# Kill the running Azurite server
if [[ "$TILEDB_AZURE" == "ON" ]]; then
  kill -9 $AZURITE_PID
fi

# Kill the running GCS emulator server Linux only because OSX does not
# run the emulator
if [[ "$AGENT_OS" != "Darwin" && "$TILEDB_GCS" == "ON" ]]; then
  kill -9 $GCS_PID
fi

set -x
pwd
# move up a directory level so we are writing archive where tar won't fail reporting change while reading...
cd $GITHUB_WORKSPACE/..
pwd
source_archive_name=tiledb-source-${TDB_REF_NAME}-${TDB_COMMIT_HASH}.tar.gz
tar --exclude=build -zcf ${source_archive_name} ./TileDB
ls -l $GITHUB_WORKSPACE/..
sync
# move up a directory level so we are writing archive where tar won't fail reporting change while reading...
cd $GITHUB_WORKSPACE/..
pwd
binary_archive_name="tiledb-${ARTIFACT_OS}-${ARTIFACT_ARCH}-${TDB_REF_NAME}-${TDB_COMMIT_HASH}.tar.gz"
tar -zcf ${binary_archive_name} ./TileDB/build
ls -l $GITHUB_WORKSPACE/..
sync

TDB_SOURCE_ARCHIVE_PATH2="$GITHUB_WORKSPACE/../${source_archive_name}"
TDB_SOURCE_ARCHIVE_PATH=$(echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_SOURCE_ARCHIVE_PATH2\"))\n" | python)

TDB_BINARY_ARCHIVE_PATH2="$GITHUB_WORKSPACE/../${binary_archive_name}"
TDB_BINARY_ARCHIVE_PATH=$(echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_BINARY_ARCHIVE_PATH2\"))\n" | python)

echo "TDB_BINARY_ARCHIVE_PATH=$TDB_BINARY_ARCHIVE_PATH" >> "$GITHUB_WORKSPACE/../TDBRETENVVARS.TXT"
echo "TDB_SOURCE_ARCHIVE_PATH=$TDB_SOURCE_ARCHIVE_PATH" >> "$GITHUB_WORKSPACE/../TDBRETENVVARS.TXT"

if [[ $(find "$GITHUB_WORKSPACE/.." -name TDBRETENVVARS.TXT) ]]; then
  echo "found TDBRETENVVARS.TXT"
fi


pwd
ls -l $GITHUB_WORKSPACE/..
if [[ $(ls -l $TDB_SOURCE_ARCHIVE_PATH) ]]; then
  echo "found $TDB_SOURCE_ARCHIVE_PATH"
fi
if [[ $(ls -l $TDB_BINARY_ARCHIVE_PATH) ]]; then
  echo "found $TDB_BINARY_ARCHIVE_PATH"
fi
if [[ $(ls -l $TDB_SOURCE_ARCHIVE_PATH2) ]]; then
  echo "found $TDB_SOURCE_ARCHIVE_PATH2"
fi
if [[ $(ls -l $TDB_BINARY_ARCHIVE_PATH2) ]]; then
  echo "found $TDB_BINARY_ARCHIVE_PATH2"
fi
