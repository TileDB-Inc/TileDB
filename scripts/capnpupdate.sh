#Needs to be run from inside a current build directory
BUILD_DIR=`pwd`
echo " build_dir: '${BUILD_DIR}'"
PATH="${PATH}:${BUILD_DIR}/externals/install/bin"

CAPNPPATH="${BUILD_DIR}/externals/install/bin/capnp"
if ! test -f ${CAPNPPATH}
then
  echo "Abort: Missing ${CAPNPATH}"
  return 1
fi

pushd $2 # location of tiledb-rest.capnp

CAPNPINCLUDEROOT="${BUILD_DIR}/externals/install/include"
echo "capnpincluderoot: ${CAPNPINCLUDEROOT}"
echo "path: '${PATH}'"

echo "pwd " `pwd`

echo "capnppath: '${CAPNPPATH}'"
outdir=$1
if [ x$outdir == x ] ; then
  CAPNPCMD="${CAPNPPATH} compile -I ${CAPNPINCLUDEROOT} -oc++ tiledb-rest.capnp"
else
  mkdir -p $outdir
  CAPNPCMD="${CAPNPPATH} compile -I ${CAPNPINCLUDEROOT} -oc++:${outdir} tiledb-rest.capnp"
fi
echo "capnpcmd: '${CAPNPCMD}'"
eval ${CAPNPCMD}
popd
