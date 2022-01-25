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
binary_archive_name=tiledb-${ARTIFACT_OS}-${ARTIFACT_ARCH}-${TDB_REF_NAME}-${TDB_COMMIT_HASH}.tar.gz
tar -zcf ${binary_archive_name} ./TileDB/build
ls -l $GITHUB_WORKSPACE/..
sync

TDB_SOURCE_ARCHIVE_PATH2="$GITHUB_WORKSPACE/../${source_archive_name}"
TDB_SOURCE_ARCHIVE_PATH=$(echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_SOURCE_ARCHIVE_PATH2\"))\n" | python)

TDB_BINARY_ARCHIVE_PATH2="$GITHUB_WORKSPACE/../${binary_archive_name}"
TDB_BINARY_ARCHIVE_PATH=$(echo -e "import sys\nimport os\nprint (os.path.realpath(\"$TDB_BINARY_ARCHIVE_PATH2\"))\n" | python)

echo "TDB_BINARY_ARCHIVE_PATH=$TDB_BINARY_ARCHIVE_PATH" >> "$GITHUB_WORKSPACE/../TDBRETENVVARS.TXT"
echo "TDB_SOURCE_ARCHIVE_PATH=$TDB_SOURCE_ARCHIVE_PATH" >> "$GITHUB_WORKSPACE/../TDBRETENVVARS.TXT"
echo "TDB_SOURCE_ARCHIVE_NAME=$source_archive_name" >> "$GITHUB_WORKSPACE/../TDBRETENVVARS.TXT"
echo "TDB_BINARY_ARCHIVE_NAME=$binary_archive_name" >> "$GITHUB_WORKSPACE/../TDBRETENVVARS.TXT"

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

echo "END, inside linux-mac_release_buildtiledb.sh"
