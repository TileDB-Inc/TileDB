#!/bin/bash

BaseDir="$(pwd)"
TestAppDir="$(pwd)/tiledb/examples/c_api"
TestAppDataDir="$(pwd)/tiledb/examples/c_api/test_app_data"
for exampleexe in $(ls ${TestAppDir}/*_c) ;
do
  cd ${TestAppDir}
  rm -rf ${TestAppDataDir}
  mkdir ${TestAppDataDir}
  cd ${TestAppDataDir}
  echo $exampleexe
  $exampleexe;
  status=$?
  if (($status != 0)); then
    echo "FAILED: $exampleexe exited with $status"
    echo "::set-output name=TILEDB_CI_SUCCESS::0"
  fi
done
cd ${TestAppDir}
rm -rf ${TestAppDataDir}

cd ${BaseDir}
TestAppDir="$(pwd)/tiledb/examples/cpp_api"
TestAppDataDir="$(pwd)/tiledb/examples/cpp_api/test_app_data"
for exampleexe in $(ls ${TestAppDir}/*_cpp) ;
do
  # Skip running WebP example with no input
  if [ "${exampleexe##*/}" == png_ingestion_webp_cpp ]; then
    continue
  fi;

  cd ${TestAppDir}
  rm -rf ${TestAppDataDir}
  mkdir ${TestAppDataDir}
  cd ${TestAppDataDir}
  echo $exampleexe
  $exampleexe;
  status=$?
  if (($status != 0)); then
    echo "FAILED: $exampleexe exited with $status"
    echo "::set-output name=TILEDB_CI_SUCCESS::0"
  fi
done
cd ${TestAppDir}
rm -rf ${TestAppDataDir}
cd ${BaseDir}
