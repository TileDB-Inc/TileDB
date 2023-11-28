#!/bin/bash

SourceDir="$(dirname $0)/.."
BaseDir="$(pwd)"

TestAppDir="$(pwd)/tiledb/examples/c_api"
TestAppDataDir="$(pwd)/tiledb/examples/c_api/test_app_data"
for example in $(ls ${SourceDir}/examples/c_api/*.c) ;
do
  cd ${TestAppDir}
  rm -rf ${TestAppDataDir}
  mkdir ${TestAppDataDir}
  cd ${TestAppDataDir}
  exampleexe=${example%.c}_c
  exampleexe=${exampleexe##*/}
  cmake --build ${BaseDir}/tiledb --target ${exampleexe}
  echo $TestAppDir/$exampleexe
  $TestAppDir/$exampleexe;
  # Remove the executable after running it to prevent disk
  # space exhaustion when statically linking to tiledb.
  rm $TestAppDir/$exampleexe
  status=$?
  if (($status != 0)); then
    echo "FAILED: $exampleexe exited with $status"
    echo "TILEDB_CI_SUCCESS=0" >> $GITHUB_OUTPUT
  fi
done
cd ${TestAppDir}
rm -rf ${TestAppDataDir}

cd ${BaseDir}
TestAppDir="$(pwd)/tiledb/examples/cpp_api"
TestAppDataDir="$(pwd)/tiledb/examples/cpp_api/test_app_data"
for example in $(ls ${SourceDir}/examples/cpp_api/*.cc) ;
do
  # Skip running WebP example with no input
  if [ "${example##*/}" == png_ingestion_webp.cc ]; then
    continue
  fi;

  cd ${TestAppDir}
  rm -rf ${TestAppDataDir}
  mkdir ${TestAppDataDir}
  cd ${TestAppDataDir}
  exampleexe=${example%.cc}_cpp
  exampleexe=${exampleexe##*/}
  cmake --build ${BaseDir}/tiledb --target ${exampleexe}
  echo $TestAppDir/$exampleexe
  $TestAppDir/$exampleexe;
  rm $TestAppDir/$exampleexe
  status=$?
  if (($status != 0)); then
    echo "FAILED: $exampleexe exited with $status"
    echo "TILEDB_CI_SUCCESS=0" >> $GITHUB_OUTPUT
  fi
done
cd ${TestAppDir}
rm -rf ${TestAppDataDir}
cd ${BaseDir}
