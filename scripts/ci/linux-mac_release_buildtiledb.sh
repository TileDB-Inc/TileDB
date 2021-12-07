echo "BEGIN, inside linux-mac_release_buildtiledb.sh"
echo ${0}
set -x
printenv
# DELETEME work-around for https://github.com/microsoft/azure-pipelines-image-generation/issues/969
#Needed for GA CI or not?... $SUDO chown root.root /

# Azure sets "SYSTEM=build" for unknown reasonas, which breaks the OpenSSL configure script
#   - openssl configure uses ENV{SYSTEM} if available:
#     https://github.com/openssl/openssl/blob/6d745d740d37d680ff696486218b650512bbbbc6/config#L56
#   - error description:
#     https://developercommunity.visualstudio.com/content/problem/602584/openssl-build-error-when-using-pipelines.htm
unset SYSTEM

# azure bash does not treat intermediate failure as error
# https://github.com/Microsoft/azure-pipelines-yaml/issues/135
set -e pipefail

git config --global user.name 'GACI Pipeline'
git config --global user.email 'no-reply@tiledb.io'

BUILD_REPOSITORY_LOCALPATH=$GITHUB_WORKSPACE
# Set up arguments for bootstrap.sh
BUILD_BINARIESDIRECTORY=${BUILD_BINARIESDIRECTORY:-$BUILD_REPOSITORY_LOCALPATH/dist}
cmake_args="-DCMAKE_INSTALL_PREFIX=${BUILD_BINARIESDIRECTORY} -DTILEDB_TESTS=OFF -DTILEDB_INSTALL_LIBDIR=lib";
mkdir -p ${BUILD_BINARIESDIRECTORY}

# Enable TILEDB_STATIC by default
[ "$TILEDB_STATIC" ] || TILEDB_STATIC=ON
if [[ "$TILEDB_STATIC" == "ON" ]]; then
  cmake_args="${cmake_args} -DTILEDB_STATIC=ON";
fi
if [[ "$TILEDB_HDFS" == "ON" ]]; then
  cmake_args="${cmake_args} -DTILEDB_HDFS=ON";
fi;
if [[ "$TILEDB_S3" == "ON" ]]; then
  cmake_args="${cmake_args} -DTILEDB_S3=ON";
fi;
if [[ "$TILEDB_AZURE" == "ON" ]]; then
  cmake_args="${cmake_args} -DTILEDB_AZURE=ON";
fi;
if [[ "$TILEDB_GCS" == "ON" ]]; then
  cmake_args="${cmake_args} -DTILEDB_GCS=ON";
fi;
if [[ "$TILEDB_TOOLS" == "ON" ]]; then
  cmake_args="${cmake_args} -DTILEDB_TOOLS=ON";
fi
if [[ "$TILEDB_DEBUG" == "ON" ]]; then
  cmake_args="${cmake_args} -DCMAKE_BUILD_TYPE=Debug";
fi
if [[ "$TILEDB_CI_ASAN" == "ON" ]]; then
  # Add address sanitizer flag if necessary
  cmake_args="${cmake_args} -DSANITIZER=address";
fi
if [[ "$TILEDB_CI_TSAN" == "ON" ]]; then
  # Add thread sanitizer flag if necessary
  cmake_args="${cmake_args} -DSANITIZER=thread";
fi
if [[ "$TILEDB_SERIALIZATION" == "ON" ]]; then
  # Add serialization flag if necessary
  cmake_args="${cmake_args} -DTILEDB_SERIALIZATION=ON";
fi
if [[ "$TILEDB_FORCE_BUILD_DEPS" == "ON" ]]; then
  # Add superbuild flag
  cmake_args="${cmake_args} -DTILEDB_FORCE_ALL_DEPS=ON";
fi
if [[ "$TILEDB_WERROR" == "OFF" ]]; then
  # Add superbuild flag
  cmake_args="${cmake_args} -DTILEDB_WERROR=OFF";
fi

mkdir -p $BUILD_REPOSITORY_LOCALPATH/build
cd $BUILD_REPOSITORY_LOCALPATH/build

# Configure and build TileDB
echo "Running cmake with '${cmake_args}'"
cmake .. ${cmake_args}

make -j4
make -C tiledb install

# get this into 'env' context since runners don't automatically place items
# from env into context on startup!
#echo "GITHUB_SHA=$GITHUB_SHA" >> "$GITHUB_ENV"
#echo "GITHUB_WORKSPACE=$GITHUB_WORKSPACE" >> "$GITHUB_ENV"
#echo "ARTIFACT_OS=$ARTIFACT_OS" >> "$GITHUB_ENV"
#echo "ARTIFACT_EXTRAS=$ARTIFACT_EXTRAS" >> "$GITHUB_ENV"

set -x
pwd
#tar --exclude=build -zcf tiledb-${{ env.ARTIFACT_OS }}-build-dir-${{ env.ARTIFACT_EXTRAS }}.tar.gz $GITHUB_WORKSPACE
pwd
# sync in effort to avoid "tar: .: file changed as we read it" errors, guessing
# based on logs seen, poss. due to files from install just above not yet having 
# been completely written out with flushing happening during following 'tar' efforts.
sync
#sleep 10
# move up a directory level so we are writing archive where tar won't fail reporting change while reading...
cd $GITHUB_WORKSPACE/..
pwd
tar --exclude=build -vzcf tiledb-source-${ARTIFACT_OS}-build-dir-${ARTIFACT_EXTRAS}.tar.gz ./TileDB
ls -l $GITHUB_WORKSPACE/..
sync
#sleep 10
# move up a directory level so we are writing archive where tar won't fail reporting change while reading...
cd $GITHUB_WORKSPACE/..
pwd
#tar -zcf tiledb-${{ env.ARTIFACT_OS }}-build-dir-${{ env.ARTIFACT_EXTRAS }}.tar.gz $GITHUB_WORKSPACE/build
tar -vzcf tiledb-binary-${ARTIFACT_OS}-build-dir-${ARTIFACT_EXTRAS}.tar.gz ./TileDB/build
ls -l $GITHUB_WORKSPACE/..
sync
TDB_SOURCE_ARCHIVE_PATH=`dirname $GITHUB_WORKSPACE/..`/tiledb-source-${ARTIFACT_OS}-build-dir-${ARTIFACT_EXTRAS}.tar.gz
TDB_SOURCE_ARCHIVE_PATH2="$GITHUB_WORKSPACE/../tiledb-source-${ARTIFACT_OS}-build-dir-${ARTIFACT_EXTRAS}.tar.gz"
echo "TDB_SOURCE_ARCHIVE_PATH=$TDB_SOURCE_ARCHIVE_PATH"
#echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_SOURCE_ARCHIVE_PATH\"))\n" | /opt/rh/rh-python36/root/usr/bin/python
echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_SOURCE_ARCHIVE_PATH\"))\n" | python
echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_SOURCE_ARCHIVE_PATH2\"))\n" | python
#would like to use 'realpath', but seems not present in manylinux, and failed finding package that has it, whereas
#seems python functionality is present in all environs.
#TDB_SOURCE_ARCHIVE_PATH=$(echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_SOURCE_ARCHIVE_PATH\"))\n" | /opt/rh/rh-python36/root/usr/bin/python)
#TDB_SOURCE_ARCHIVE_PATH=$(echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_SOURCE_ARCHIVE_PATH\"))\n" | python)
TDB_SOURCE_ARCHIVE_PATH=$(echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_SOURCE_ARCHIVE_PATH2\"))\n" | python)
echo "TDB_SOURCE_ARCHIVE_PATH=$TDB_SOURCE_ARCHIVE_PATH"
#TDB_SOURCE_ARCHIVE_PATH=`dirname $TDB_SOURCE_ARCHIVE_PATH`
#echo "TDB_SOURCE_ARCHIVE_PATH=$TDB_SOURCE_ARCHIVE_PATH"
##echo "TDB_SOURCE_ARCHIVE_PATH=$TDB_SOURCE_ARCHIVE_PATH" >> "$GITHUB_ENV"

TDB_BINARY_ARCHIVE_PATH=`dirname $GITHUB_WORKSPACE/..`/tiledb-binary-${ARTIFACT_OS}-build-dir-${ARTIFACT_EXTRAS}.tar.gz
TDB_BINARY_ARCHIVE_PATH2="$GITHUB_WORKSPACE/../tiledb-binary-${{ env.ARTIFACT_OS }}-build-dir-${ARTIFACT_EXTRAS}.tar.gz"
echo "TDB_BINARY_ARCHIVE_PATH=$TDB_BINARY_ARCHIVE_PATH"
#'realpath' not available everywhere, see comment above.
#echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_BINARY_ARCHIVE_PATH\"))\n" | /opt/rh/rh-python36/root/usr/bin/python
echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_BINARY_ARCHIVE_PATH\"))\n" | python
echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_BINARY_ARCHIVE_PATH2\"))\n" | python
#TDB_BINARY_ARCHIVE_PATH=$(echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_BINARY_ARCHIVE_PATH\"))\n" | /opt/rh/rh-python36/root/usr/bin/python)
#TDB_BINARY_ARCHIVE_PATH=$(echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_BINARY_ARCHIVE_PATH\"))\n" | python)
TDB_BINARY_ARCHIVE_PATH=$(echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_BINARY_ARCHIVE_PATH2\"))\n" | python)
#TDB_BINARY_ARCHIVE_PATH=`dirname $TDB_BINARY_ARCHIVE_PATH`
echo "TDB_BINARY_ARCHIVE_PATH=$TDB_BINARY_ARCHIVE_PATH"
#echo "ARTIFACT_EXTRAS=$ARTIFACT_EXTRAS" >> "$GITHUB_ENV"
##echo "TDB_BINARY_ARCHIVE_PATH=$TDB_BINARY_ARCHIVE_PATH" >> "$GITHUB_ENV"

ls -l $TDB_SOURCE_ARCHIVE_PATH
ls -l $TDB_BINARY_ARCHIVE_PATH

find / -name 'tiledb-*-linux-build-*.tar.gz' -exec ls -l {} \;

echo "END, inside linux-mac_release_buildtiledb.sh"
