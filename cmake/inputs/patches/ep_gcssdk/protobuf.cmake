#copied from stuff fetched to build ep_gcssdk...

# ~~~
# Copyright 2018 Google LLC
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

find_package(Threads REQUIRED)

include(ExternalProjectHelper)

if (NOT TARGET protobuf-project)
    # Give application developers a hook to configure the version and hash
    # downloaded from GitHub.
    set(GOOGLE_CLOUD_CPP_PROTOBUF_URL
        "https://github.com/google/protobuf/archive/v3.11.3.tar.gz")
    set(GOOGLE_CLOUD_CPP_PROTOBUF_SHA256
        "cf754718b0aa945b00550ed7962ddc167167bd922b842199eeb6505e6f344852")

    set_external_project_build_parallel_level(PARALLEL)
    set_external_project_vars()

    set(TDB_CXX_FLAGS "${CMAKE_CXX_FLAGS}")    
    set(TDB_CXX_FLAGS "${TDB_CXX_FLAGS} /EHsc")
    set(TDB_CXX_FLAGS "${TDB_CXX_FLAGS} /Gd")

    include(ExternalProject)
    ExternalProject_Add(
        protobuf-project
        EXCLUDE_FROM_ALL ON
        PREFIX "${CMAKE_BINARY_DIR}/external/protobuf"
        INSTALL_DIR "${GOOGLE_CLOUD_CPP_EXTERNAL_PREFIX}"
        URL ${GOOGLE_CLOUD_CPP_PROTOBUF_URL}
        URL_HASH SHA256=${GOOGLE_CLOUD_CPP_PROTOBUF_SHA256}
        LIST_SEPARATOR |
        CONFIGURE_COMMAND
            ${CMAKE_COMMAND}
            --trace
            -A X64
            -G${CMAKE_GENERATOR}
            ${GOOGLE_CLOUD_CPP_EXTERNAL_PROJECT_CMAKE_FLAGS}
            -DCMAKE_PREFIX_PATH=${GOOGLE_CLOUD_CPP_PREFIX_PATH}
            -DCMAKE_INSTALL_RPATH=${GOOGLE_CLOUD_CPP_INSTALL_RPATH}
            -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
            -Dprotobuf_BUILD_TESTS=OFF
            -Dprotobuf_DEBUG_POSTFIX=
            -H<SOURCE_DIR>/cmake
            -B<BINARY_DIR>
            #-DCMAKE_CXX_FLAGS="/EHsc"
            #-DCMAKE_CXX_FLAGS="/EHsc /Gd" # "/Gd" trying to force __cdecl member functions, grpc_cpp_plugin seems to expect it...
            -DCMAKE_CXX_FLAGS=${TDB_CXX_FLAGS}
#            -DLINK_FLAGS=/NODEFAULTLIB:LIBCMTD
            -DCMAKE_C_FLAGS=/EHsc
            #dlh additions...
            -Dprotobuf_DEBUG=ON
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            -DBUILD_SHARED_LIBS=OFF
            -Dprotobuf_BUILD_SHARED_LIBS=OFF
            -Dprotobuf_MSVC_STATIC_RUNTIME=ON
            #-DBUILD_SHARED_LIBS=ON
            #-Dprotobuf_BUILD_SHARED_LIBS=ON
            #-Dprotobuf_MSVC_STATIC_RUNTIME=ON
#        BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> ${PARALLEL}
        BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> ${PARALLEL} -- /verbosity:detailed
        LOG_DOWNLOAD ON
        LOG_CONFIGURE ON
        LOG_BUILD ON
        LOG_INSTALL ON
        LOG_OUTPUT_ON_FAILURE ON)
endif ()
