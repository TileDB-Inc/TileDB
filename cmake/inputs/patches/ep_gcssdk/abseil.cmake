# ~~~
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ~~~

include(ExternalProjectHelper)

if (NOT TARGET abseil-cpp-project)
    # Give application developers a hook to configure the version and hash
    # downloaded from GitHub.
    set(GOOGLE_CLOUD_CPP_ABSEIL_CPP_URL
        "https://github.com/abseil/abseil-cpp/archive/20200225.2.tar.gz")
    set(GOOGLE_CLOUD_CPP_ABSEIL_CPP_SHA256
        "f41868f7a938605c92936230081175d1eae87f6ea2c248f41077c8f88316f111")

    set_external_project_build_parallel_level(PARALLEL)
    set_external_project_vars()

    set(TDB_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHsc /MTd")
    set(TDB_C_FLAGS "${CMAKE_CXX_FLAGS} /EHsc /MTd")

    include(ExternalProject)
    ExternalProject_Add(
        abseil-cpp-project
        EXCLUDE_FROM_ALL ON
        PREFIX "${CMAKE_BINARY_DIR}/external/abseil_cpp"
        INSTALL_DIR "${GOOGLE_CLOUD_CPP_EXTERNAL_PREFIX}"
        URL ${GOOGLE_CLOUD_CPP_ABSEIL_CPP_URL}
        URL_HASH SHA256=${GOOGLE_CLOUD_CPP_ABSEIL_CPP_SHA256}
        LIST_SEPARATOR |
        CMAKE_ARGS # "multiple not allows...", tho not sure where other is coming from... -A X64 
                   --trace 
                   ${GOOGLE_CLOUD_CPP_EXTERNAL_PROJECT_CCACHE}
                   -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                   -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
                   -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
                   -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
                   -DBUILD_SHARED_LIBS=OFF #${BUILD_SHARED_LIBS}
                   -DBUILD_TESTING=OFF
                   -DCMAKE_PREFIX_PATH=${GOOGLE_CLOUD_CPP_PREFIX_PATH}
                   -DCMAKE_INSTALL_RPATH=${GOOGLE_CLOUD_CPP_INSTALL_RPATH}
                   -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
                   -DCMAKE_CXX_FLAGS=${TDB_CXX_FLAGS} #/EHsc
                   -DCMAKE_C_FLAGS=${TDB_C_FLAGS} #/EHsc
                   -DLINK_FLAGS=/NODEFAULTLIB:LIBCMTD
                   -DSTATIC_LIBRARY_FLAGS=/NODEFAULTLIB:MSVCRTD #LIBCMTD
                   -DSTATIC_LIBRARY_OPTIONS=/NODEFAULTLIB:MSVCRTD #LIBCMTD # cmake v3.13
                   -DABSL_BUILD_DLL=OFF
                   -DABSL_DEFAULT_COPTS=${TDB_CXX_FLAGS}
                   #-DCMAKE_CXX_FLAGS_DEBUG="/MDd /Zi /Ob0 /Od /RTC1"
                   #static - which tiledb trying to  use
                   -DCMAKE_CXX_FLAGS_DEBUG="/MTd /Zi /Ob0 /Od /RTC1"
         BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> ${PARALLEL} -- /verbosity:detailed
        PATCH_COMMAND ${CMAKE_COMMAND} -P
                      "${CMAKE_CURRENT_LIST_DIR}/abseil-patch.cmake"
        LOG_CONFIGURE OFF
        LOG_BUILD ON
        LOG_INSTALL ON
	LOG_OUTPUT_ON_FAILURE ON)
endif ()
