#
# FindGCSSDK_EP.cmake
#
#
# The MIT License
#
# Copyright (c) 2018-2021 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# This module finds the GCS C++ SDK, installing it with an ExternalProject if
# necessary. It then defines the imported targets GCSSDK::<component>, e.g.
# GCSSDK::storage-client or GCSSDK::google_cloud_cpp_common.

#########################
# from https://stackoverflow.com/a/56738858
## https://stackoverflow.com/questions/32183975/how-to-print-all-the-properties-of-a-target-in-cmake/56738858#56738858
## https://stackoverflow.com/a/56738858/3743145

## Get all properties that cmake supports
execute_process(COMMAND cmake --help-property-list OUTPUT_VARIABLE CMAKE_PROPERTY_LIST)
## Convert command output into a CMake list
STRING(REGEX REPLACE ";" "\\\\;" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
STRING(REGEX REPLACE "\n" ";" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")

list(REMOVE_DUPLICATES CMAKE_PROPERTY_LIST)

function(print_target_properties tgt)
    if(NOT TARGET ${tgt})
      message("There is no target named '${tgt}'")
      return()
    endif()

    foreach (prop ${CMAKE_PROPERTY_LIST})
        string(REPLACE "<CONFIG>" "${CMAKE_BUILD_TYPE}" prop ${prop})
        get_target_property(propval ${tgt} ${prop})
        if (propval)
            message ("${tgt} ${prop} = ${propval}")
        endif()
    endforeach(prop)
endfunction(print_target_properties)
#########################

include(CMakePrintHelpers)

#cmake_print_properties(
#  TARGETS TILEDB_CORE_OBJECTS TILEDB_CORE_OBJECTS_ILIB
#  PROPERTIES INCLUDE_DIRECTORIES INTERFACE_INCLUDE_DIRECTORIES
#  )

#print_target_properties( $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS> ) 
#print_target_properties( TILEDB_CORE_OBJECTS ) 

#############################
# Include some common helper functions.
include(TileDBCommon)

# If the EP was built, it will install the storage_client-config.cmake file,
# which we can use with find_package. CMake uses CMAKE_PREFIX_PATH to locate find
# modules.
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${TILEDB_EP_INSTALL_PREFIX}")

# Try searching for the SDK in the EP prefix.
set(GCSSDK_DIR "${TILEDB_EP_INSTALL_PREFIX}")

# First check for a static version in the EP prefix.
# The storage client is installed as the cmake package storage_client
# TODO: This should be replaced with proper find_package as google installs cmake targets for the subprojects
if (NOT TILEDB_FORCE_ALL_DEPS OR TILEDB_GCSSDK_EP_BUILT)
  #set(abslpath d:/dev/tiledb/gh.sc-17498-upd-gcs.git/bld.vs22.A/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/absl)
  #set(absl_DIR "${abslpath}")
  # TBD: Do artifacts of these need to get 'installed' into tiledb location to be included with distribution as well?
  #set(absl_DIR "${CMAKE_BINARY_DIR}/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/absl")
  #set(nlohmann_jsonConfig_DIR "${CMAKE_BINARY_DIR}/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/nlohmann_json")
  #set(benchmark_DIR "${CMAKE_BINARY_DIR}/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/benchmark")
  #set(Crc32c_DIR "${CMAKE_BINARY_DIR}/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/Crc32c")
  #set(CURL_DIR "${CMAKE_BINARY_DIR}/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/CURL")
  #set(gRPC_DIR "${CMAKE_BINARY_DIR}/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/gRPC")
  #set(GTest_DIR "${CMAKE_BINARY_DIR}/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/GTest")
  #set(re2_DIR "${CMAKE_BINARY_DIR}/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/re2")

  set(absl_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/absl")
  set(nlohmann_json_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/nlohmann_json")
  set(benchmark_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/benchmark")
  set(Crc32c_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/Crc32c")
  set(CURL_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/CURL")
  set(gRPC_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/gRPC")
  set(GTest_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/GTest")
  set(re2_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/re2")
#  set(OpenSSL_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/openssl")
  message(STATUS "absl_DIR is ${absl_DIR}")
  message(STATUS "nlohmann_json_DIR is ${nlohmann_json_DIR}")
  message(STATUS "benchmark_DIR is ${benchmark_DIR}")
  message(STATUS "Crc32c_DIR is ${Crc32c_DIR}")
  message(STATUS "CURL_DIR is ${CURL_DIR}")
  message(STATUS "gRPC_DIR is ${gRPC_DIR}")
  message(STATUS "GTest_DIR is ${GTest_DIR}")
  message(STATUS "re2_DIR is ${re2_DIR}")
#  message(STATUS "OpenSSL_DIR is ${OpenSSL_DIR}")
  # TBD: Will this satisfy gcs need? seems openssl not part of what gcs installs in build via vcpkg...
  # "unimplemented on windows" ... find_package(OpenSSL_EP)
  # TBD: There is an openssl.pc, but not an opensslConfig.cmake, will this still work? somehow the 
  # google buildis finding openssl, the outside experimental build I did contained a (very small) openssl.lib,
  # but the build incorp'd into the external project below does -not- seem to have that...
  #set(openssl_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/openssl")
  #set(OpenSSL_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/openssl")
  #set(OpenSSL_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/lib")
  set(OpenSSL_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows")
  message(STATUS "before fp OpenSSL_DIR is ${OpenSSL_DIR}")
  set(ENV{OpenSSL_DIR} "${OpenSSL_DIR}")
  set(ENV{OPENSSL_ROOT_DIR} "${OpenSSL_DIR}")
  message(STATUS "before fp env OpenSSL_DIR is $ENV{OpenSSL_DIR}")
  message(STATUS "before fp env OpenSSL_DIR is $ENV{OPENSSL_ROOT_DIR}")
  #find_package(openssl
  find_package(OpenSSL
    PATHS
      #${TILEDB_OPENSSL_DIR}
      #${openssl_DIR}
      ${OpenSSL_DIR}
      #${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/lib
    )
  message(STATUS "after OpenSSL_DIR is ${OpenSSL_DIR}")
#  set(VCPKG_INSTALLED_DIR "${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed")
  # gcs v2.0.0+ apparently requires externally, not sure why not pulling from vcpkg, attempts
  # to make it do so by adding openssl to gcssdk vcpkg.json do not get openssl built/included...
  # not going to work as is, "cmake/Modules/FindGCSSDK_EP.cmake:88 (find_package)"
  # "OpenSSL external project unimplemented on Windows."
  #find_package(OpenSSL_EP REQUIRED) 
  message(STATUS "findGCSSDK... TILEDB_EP_BASE is ${TILEDB_EP_BASE}")
  #find_package(OpenSSL
  #  PATHS ${TILEDB_OPENSSL_DIR}
  #    ${openssl_DIR}
  #  )
#  find_package(storage_client
  find_package(google_cloud_cpp_storage CONFIG
#  find_package(google_cloud_cpp::storage CONFIG
#  find_package(google-cloud-cpp::storage CONFIG
    PATHS ${TILEDB_EP_INSTALL_PREFIX}
          ${TILEDB_DEPS_NO_DEFAULT_PATH}
#          ${TILEDB_EP_BASE}/src/ep_gcssdk
          ${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/google/cloud/storage
  )
#  set(GCSSDK_FOUND ${storage_client_FOUND})
  set(GCSSDK_FOUND ${google_cloud_cpp_storage_FOUND})
#  set(GCSSDK_FOUND ${google-cloud-cpp::storage_FOUND})
  message(STATUS "findgcs, GCSSDK_FOUND is ${GCSSDK_FOUND}")
  if(NOT TARGET google_cloud_cpp_storage)
    message(STATUS "missing target google_cloud_cpp_storage")
  else()
    message(STATUS "have target google_cloud_cpp_storage")
    print_target_properties( google_cloud_cpp_storage ) 
  endif()
  if(NOT TARGET google-cloud-cpp::storage)
    message(STATUS "missing target google-cloud-cpp::storage")
  else()
    message(STATUS "have target google-cloud-cpp::storage")
    print_target_properties( google-cloud-cpp::storage ) 
  endif()
endif()

if (NOT GCSSDK_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Could NOT find GCSSDK")
    message(STATUS "Adding GCSSDK as an external project")

    set(DEPENDS)
    if (TARGET ep_curl)
      list(APPEND DEPENDS ep_curl)
    endif()
    if (TARGET ep_openssl)
      list(APPEND DEPENDS ep_openssl)
    endif()
    if (TARGET ep_zlib)
      list(APPEND DEPENDS ep_zlib)
    endif()

    # Fetch the number of CPUs on the this sytem.
    include(ProcessorCount)
    processorcount(NCPU)

    if(WIN32)
      find_package(Git REQUIRED)
      # can use ${GIT_EXECUTABLE}
      #set(TDB_PREP_ENV "set VCPKG_ROOT=%cd%/tdb.vcpkg/vcpkg &&")
      set(TDB_PREP_ENV "VCPKG_ROOT=%cd%/tdb.vcpkg/vcpkg &&")
      #set(TDB_VCPKG_TOOLCHAIN_PATH "%VCPKG_ROOT%/tdb.vcpkg/vcpkg/scripts/buildsystem/vcpkg.cmake")
      #set(TDB_VCPKG_TOOLCHAIN_PATH "${TILEDB_EP_BASE}/src/ep_gcssdk/tdb.vcpkg/vcpkg/scripts/buildsystem/vcpkg.cmake")
      set(TDB_VCPKG_TOOLCHAIN_PATH "${CMAKE_BINARY_DIR}/tdb.vcpkg/vcpkg/scripts/buildsystems/vcpkg.cmake")
    else()
      set(TDB_PREP_ENV "VCPKG_ROOT=`pwd`/tdb.vcpkg/vcpkg")
      #set(TDB_VCPKG_TOOLCHAIN_PATH "$${VCPKG_ROOT}/tdb.vcpkg/vcpkg/scripts/buildsystem/vcpkg.cmake")
      #set(TDB_VCPKG_TOOLCHAIN_PATH "${TILEDB_EP_BASE}/src/ep_gcssdk/tdb.vcpkg/vcpkg/scripts/buildsystem/vcpkg.cmake")
      set(TDB_VCPKG_TOOLCHAIN_PATH "${CMAKE_BINARY_DIR}/tdb.vcpkg/vcpkg/scripts/buildsystems/vcpkg.cmake")
    endif()
    #TBD: will this reach far enough 'out' that vcpkg finds its own stuff, also possibly negating
    #need for the various machinations used to temporarily set it below so gcssdk builds (which it
    #has been doing, but tiledb now failing to find vcpkg absl for one...)
    #set(env{VCPKG_ROOT} "${CMAKE_BINARY_DIR}/tdb.vcpkg/vcpkg/scripts/buildsystems/vcpkg.cmake")
    set(env{VCPKG_ROOT} "${CMAKE_BINARY_DIR}/tdb.vcpkg/vcpkg/")

    if (WIN32)
      find_package(Git REQUIRED)
      set(CONDITIONAL_PATCH cd ${CMAKE_SOURCE_DIR} && ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_gcssdk < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/gcssdk-v2.0.1-vcpkg.B.patch)
    else()
      set(CONDITIONAL_PATCH patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/gcssdk-v2.0.1-vcpkg.B.patch)
    endif()
    set(CONDITIONAL_PATCH "") #seems adding openssl to vcpkg.json is NOT helpful...
  message(STATUS "findGCSSDK B ... TILEDB_EP_BASE is ${TILEDB_EP_BASE}")
  ExternalProject_Add(ep_gcssdk
      PREFIX "externals"
      # Set download name to avoid collisions with only the version number in the filename
      DOWNLOAD_NAME ep_gcssdk.zip
      #URL "https://github.com/googleapis/google-cloud-cpp/archive/v1.22.0.zip"
      #URL_HASH SHA1=d4e14faef4095289b06f5ffe57d33a14574a7055
      #URL "https://github.com/googleapis/google-cloud-cpp/archive/v2.0.0.zip"
      #URL_HASH SHA1=96301fbb20e82043bbb46ca4232d568b4b7c1fa7
      URL "https://github.com/googleapis/google-cloud-cpp/archive/v2.0.1.zip"
      URL_HASH SHA1=da69d5e849e9fe904289d09531a3f6a406f512c8
      BUILD_IN_SOURCE 1
#      PATCH_COMMAND
#        patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/build.patch &&
#        patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/disable_tests.patch &&
#        patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/disable_examples.patch &&
        #The following patch is on top of a patch already done in build.patch above, application order is important!
        #does add_compile_options() hoping for
        #"These options are used when compiling targets from the current directory and below."
#        patch -N -p1 < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_gcssdk/v1.22.0.CMakelists.txt.openssl3md5deprecationmitigation.patch
      PATCH_COMMAND
        ${CONDITIONAL_PATCH}
      CONFIGURE_COMMAND
         #${CMAKE_COMMAND} -Hsuper -Bcmake-out
         #${TDB_PREP_ENV} ${CMAKE_COMMAND} -H. -Bcmake-out/ -DCMAKE_TOOLCHAIN_FILE=./tdb.vcpkg/scripts/buildsystem/vcpkg.cmake
         #${CMAKE_COMMAND} -E env ${TDB_PREP_ENV} ${CMAKE_COMMAND} -H. -Bcmake-out/ -DCMAKE_TOOLCHAIN_FILE=./tdb.vcpkg/vcpkg/scripts/buildsystem/vcpkg.cmake
         #${CMAKE_COMMAND} -E env ${TDB_PREP_ENV} ${CMAKE_COMMAND} --trace -H. -Bcmake-out/ -DCMAKE_TOOLCHAIN_FILE=${TDB_VCPKG_TOOLCHAIN_PATH}
         ${CMAKE_COMMAND} -E env ${TDB_PREP_ENV} ${CMAKE_COMMAND} --debug-find -H. -Bcmake-out/ -DCMAKE_TOOLCHAIN_FILE=${TDB_VCPKG_TOOLCHAIN_PATH}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DBUILD_SHARED_LIBS=OFF
        -DBUILD_SAMPLES=OFF
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        #-DOPENSSL_ROOT_DIR=${OpenSSL_DIR} #${TILEDB_OPENSSL_DIR}
        #Do we need this at all with vcpkg... 
        #-DOPENSSL_ROOT_DIR="${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/lib"
        #-DOPENSSL_ROOT_DIR=${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/lib
        -DOPENSSL_ROOT_DIR=${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows
        #-DOPENSSL_ROOT_DIR="${TILEDB_EP_BASE}/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/lib"
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DGOOGLE_CLOUD_CPP_ENABLE_MACOS_OPENSSL_CHECK=OFF
        # Disable unused api features to speed up build
        -DGOOGLE_CLOUD_CPP_ENABLE_BIGQUERY=OFF
        -DGOOGLE_CLOUD_CPP_ENABLE_BIGTABLE=OFF
        -DGOOGLE_CLOUD_CPP_ENABLE_SPANNER=OFF
        -DGOOGLE_CLOUD_CPP_ENABLE_FIRESTORE=OFF
        -DGOOGLE_CLOUD_CPP_ENABLE_STORAGE=ON
        -DGOOGLE_CLOUD_CPP_ENABLE_PUBSUB=OFF
        -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF # Set in case BUILD_SAMPLES=OFF above does not cover this
        -DBUILD_TESTING=OFF
        # Google uses their own variable instead of CMAKE_INSTALL_PREFIX
        -DGOOGLE_CLOUD_CPP_EXTERNAL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
      #BUILD_COMMAND ${CMAKE_COMMAND} --build cmake-out -- -j${NCPU}
      BUILD_COMMAND ${CMAKE_COMMAND} --build cmake-out
      # There is no install command, the build process installs the libraries
      # ... no longer seems to for 2.0.0, but still no install command... what's the vcpkg way of locating?
      #INSTALL_COMMAND ""
      INSTALL_COMMAND ${CMAKE_COMMAND} --build cmake-out --target install
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    if(WIN32)
      #set(psargs "$env{VCPKG_ROOT}=$env{CD}/tdb.vcpkg/vcpkg" && echo "vcpkg_root is $env{VCPKG_ROOT}" && "$env{VCPKG_ROOT}/bootstrap-vcpkg.ps1" -disableMetrics)
      #set(psargs "$$env{VCPKG_ROOT}=$env{CD}/tdb.vcpkg/vcpkg && echo vcpkg_root is $$env{VCPKG_ROOT} && $$env{VCPKG_ROOT}/bootstrap-vcpkg.ps1 -disableMetrics")
      set(arg1 set VCPKG_ROOT=""${CMAKE_BINARY_DIR}/tdb.vcpkg/vcpkg"" > tdb.bootstrap.vcpkg.bat)
      ExternalProject_Add_Step(ep_gcssdk gcssdk_prep_vcpkg
        DEPENDERS configure
        DEPENDEES download
        #DEPENDEES patch
        COMMAND cmd /c mkdir tdb.vcpkg
        COMMAND echo before git clone
        COMMAND git -C tdb.vcpkg clone https://github.com/microsoft/vcpkg.git
        #COMMAND set VCPKG_ROOT=%cd%/tdb.vcpkg/vcpkg 
        #COMMAND powershell %VCPKG_ROOT%/bootstrap-vcpkg.ps1 -disableMetrics
        #TBD: will the %cd% be interp'd before passing into powershell?
        #COMMAND powershell "$env{VCPKG_ROOT}=$env{CD}/tdb.vcpkg/vcpkg" && echo "vcpkg_root is $env{VCPKG_ROOT}" && "$env{VCPKG_ROOT}/bootstrap-vcpkg.ps1" -disableMetrics
        #COMMAND powershell "${psargs}"

        COMMAND echo setting up tdb.bootstrap.vcpkg.bat file
        #COMMAND echo set VCPKG_ROOT=%cd%/tdb.vcpkg/vcpkg > tdb.bootstrap.vcpkg.bat
        #COMMAND echo set VCPKG_ROOT=""${CMAKE_BINARY_DIR}/tdb.vcpkg/vcpkg"" > tdb.bootstrap.vcpkg.bat
        COMMAND echo set VCPKG_ROOT=${CMAKE_BINARY_DIR}/tdb.vcpkg/vcpkg > tdb.bootstrap.vcpkg.bat
        #COMMAND echo ${arg1}
        
        COMMAND echo SET >> tdb.bootstrap.vcpkg.bat
        #COMMAND echo powershell %VCPKG_ROOT%/bootstrap-vcpkg.ps1 -disableMetrics >> tdb.bootstrap.vcpkg.bat
        #COMMAND echo powershell $env{VCPKG_ROOT}/bootstrap-vcpkg.ps1 -disableMetrics >> tdb.bootstrap.vcpkg.bat
        #COMMAND echo powershell ${CMAKE_BINARY_DIR}/tdb.vcpkg/vcpkg/bootstrap-vcpkg.ps1 -disableMetrics >> tdb.bootstrap.vcpkg.bat
        COMMAND echo ${CMAKE_BINARY_DIR}/tdb.vcpkg/vcpkg/bootstrap-vcpkg.bat -disableMetrics >> tdb.bootstrap.vcpkg.bat
        #COMMAND echo "$\{VCPKG_ROOT\}/bootstrap-vcpkg.sh" >> tdb.bootstrap.vcpkg.sh
        #COMMAND type ./tdb.bootstrap.vcpkg.bat && echo "------------"
        COMMAND echo "following is contents of ./tdb.bootstrap.vcpkg.bat"
        #COMMAND cmd /c "type ./tdb.bootstrap.vcpkg.bat"
        #?syntax of command incorrect? yes, doesn't like '/'... COMMAND type ./tdb.bootstrap.vcpkg.bat
        #COMMAND type .\tdb.bootstrap.vcpkg.bat
        COMMAND echo "------------"
        COMMAND echo before invoking ./tdb.bootstrap.vcpkg.bat, %cd%
        #COMMAND cmd /C ".\tdb.bootstrap.vcpkg.bat" # doesn't '/', use '\'
        COMMAND cmd /C "tdb.bootstrap.vcpkg.bat" # doesn't '/', use '\'

        #COMMAND ${CMAKE_COMMAND} -H. -Bcmake-out/ -DCMAKE_TOOLCHAIN_FILE=$%VCPKG_ROOT%/scripts/buildsystems/vcpkg.cmake
        #COMMAND ${CMAKE_COMMAND} --build cmake-out -- -j${NCPU}
      )
    else()
      
      ExternalProject_Add_Step(ep_gcssdk gcssdk_prep_vcpkg 
        DEPENDERS configure
        DEPENDEES download
        #DEPENDEES patch
        COMMAND mkdir tdb.vcpkg
        COMMAND git -C tdb.vcpkg clone https://github.com/microsoft/vcpkg.git
        COMMAND pwd
        COMMAND ls -l
        COMMAND find . -name vcpkg.cmake
        COMMAND echo "export VCPKG_ROOT=`pwd`/tdb.vcpkg/vcpkg" > tdb.bootstrap.vcpkg.sh
        COMMAND echo printenv >> tdb.bootstrap.vcpkg.sh
        #COMMAND echo "$${VCPKG_ROOT}/bootstrap-vcpkg.sh" >> tdb.bootstrap.vcpkg.sh
        #COMMAND echo "${VCPKG_ROOT}/bootstrap-vcpkg.sh" >> tdb.bootstrap.vcpkg.sh
        #COMMAND echo "$$VCPKG_ROOT/bootstrap-vcpkg.sh" >> tdb.bootstrap.vcpkg.sh
        COMMAND echo "$\{VCPKG_ROOT}/bootstrap-vcpkg.sh -disableMetrics" >> tdb.bootstrap.vcpkg.sh
        #COMMAND echo "$\{VCPKG_ROOT\}/bootstrap-vcpkg.sh" >> tdb.bootstrap.vcpkg.sh
        COMMAND cat ./tdb.bootstrap.vcpkg.sh && echo "------------"
        COMMAND bash ./tdb.bootstrap.vcpkg.sh
        #COMMAND export VCPKG_ROOT=`pwd`/tdb.vcpkg/vcpkg && printenv && echo vcpkg_root is $VCPKG_ROOT && pwd && ls -l && find tdb.vcpkg -name bootstrap-vcpkg.sh && $VCPKG_ROOT/bootstrap-vcpkg.sh
        #COMMAND export VCPKG_ROOT=`pwd`/tdb.vcpkg/vcpkg && printenv && echo [[vcpkg_root is ${VCPKG_ROOT}]] && pwd && ls -l && find tdb.vcpkg -name bootstrap-vcpkg.sh && [[${VCPKG_ROOT}/bootstrap-vcpkg.sh]]
        #COMMAND export VCPKG_ROOT=`pwd`/tdb.vcpkg/vcpkg && printenv && echo vcpkg_root is $$VCPKG_ROOT && pwd && ls -l && find tdb.vcpkg -name bootstrap-vcpkg.sh && $$VCPKG_ROOT/bootstrap-vcpkg.sh
        #COMMAND export VCPKG_ROOT=$(pwd)/tdb.vcpkg/vcpkg && printenv && echo vcpkg_root is $VCPKG_ROOT && pwd && ls -l && find tdb.vcpkg -name bootstrap-vcpkg.sh && $VCPKG_ROOT/bootstrap-vcpkg.sh
        #COMMAND ${CMAKE_COMMAND} -H. -Bcmake-out/ -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake
        #COMMAND ${CMAKE_COMMAND} --build cmake-out -- -j${NCPU}
      )
    endif()

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_gcssdk)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_GCSSDK_EP_BUILT=TRUE
      # At what point might there be conflicts with multiple toolchain files?
      -DCMAKE_TOOLCHAIN_FILE=${TDB_VCPKG_TOOLCHAIN_PATH}
    )
    # TDB: parameterize if this works to find absl...
    set(abslpath d:/dev/tiledb/gh.sc-17498-upd-gcs.git/bld.vs22.A/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/absl)
    #string(replace "\\" "/" abslpath ${abslpath})
    message(STATUS "abslpath is ${abslpath}")
    list(APPEND CMAKE_MODULE_PATH 
       "${TILEDB_EP_INSTALL_PREFIX}/lib/cmake"
#       "${abslpath}"
       )
    list(APPEND CMAKE_PREFIX_PATH "${abslpath}")
    set(absl_DIR "${abslpath}")
    set(abslpath d:/dev/tiledb/gh.sc-17498-upd-gcs.git/bld.vs22.A/externals/src/ep_gcssdk/cmake-out/vcpkg_installed/x64-windows/share/absl)
    set(absl_DIR "${abslpath}")
  else()
    message(FATAL_ERROR "Unable to find GCSSDK")
  endif()
endif()

# If we found the SDK but it didn't have a cmake target build them
#if (GCSSDK_FOUND AND NOT TARGET storage_client)
if (GCSSDK_FOUND AND NOT TARGET google_cloud_cpp_storage)
#if (GCSSDK_FOUND AND NOT TARGET google-cloud-cpp::storage)
#  message(STATUS "missing target google-cloud-cpp::storage, hacking in")
  message(STATUS "missing target google_cloud_cpp_storage, hacking in")
  # Build a list of all GCS libraries to link with.
  #list(APPEND GCSSDK_LINKED_LIBS "storage_client"
  list(APPEND GCSSDK_LINKED_LIBS 
          #"storage_client"
          "google_cloud_cpp_storage" 
         "google_cloud_cpp_common"
         "crc32c")

  foreach (LIB ${GCSSDK_LINKED_LIBS})
    find_library(GCS_FOUND_${LIB}
      NAMES lib${LIB}${CMAKE_STATIC_LIBRARY_SUFFIX}
      PATHS 
        ${TILEDB_EP_INSTALL_PREFIX} 
#        ${TILEDB_EP_BASE}/src/ep_gcssdk
      PATH_SUFFIXES lib lib64
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )
    message(STATUS "Found GCS lib: ${LIB} (${GCS_FOUND_${LIB}})")

    # If we built a static EP, install it if required.
    if (GCSSDK_STATIC_EP_FOUND AND TILEDB_INSTALL_STATIC_DEPS)
      install_target_libs(GCSSDK::${LIB})
    endif()
  endforeach ()

#  if (NOT TARGET storage_client)
#  if (NOT TARGET google_cloud_cpp_storage)
  if (NOT TARGET google-cloud-cpp::storage)
    message(STATUS "missing target google-cloud-cpp::storage!")
#    add_library(storage_client UNKNOWN IMPORTED)
#    add_library(google_cloud_cpp_storage UNKNOWN IMPORTED)
    add_library(google-cloud-cpp::storage UNKNOWN IMPORTED)
    if(0)
#    set_target_properties(storage_client PROPERTIES
#      IMPORTED_LOCATION "${GCS_FOUND_storage_client};${GCS_FOUND_google_cloud_cpp_common};${GCS_FOUND_crc32c}"
#    set_target_properties(google_cloud_cpp_storage PROPERTIES
    set_target_properties(google-cloud-cpp::storage PROPERTIES
      IMPORTED_LOCATION "${GCS_FOUND_google_cloud_cpp_storage};${GCS_FOUND_google_cloud_cpp_common};${GCS_FOUND_crc32c}"
      INTERFACE_INCLUDE_DIRECTORIES ${TILEDB_EP_INSTALL_PREFIX}/include
    )
    endif()
  endif()
endif()
