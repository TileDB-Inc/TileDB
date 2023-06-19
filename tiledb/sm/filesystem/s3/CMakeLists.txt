#
# tiledb/sm/vfs/s3/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021 TileDB, Inc.
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

include(common NO_POLICY_SCOPE)
include(object_library)

#
# `s3_providers` object library
#
commence(object_library s3_providers)
    if (TILEDB_S3)
        target_compile_definitions(s3_providers INTERFACE -DHAVE_S3)
        target_include_directories(s3_providers
                PRIVATE
                "${CMAKE_CURRENT_SOURCE_DIR}/../../../../external/include"
                )
        list(APPEND S3_PROVIDERS_SOURCES
            STSProfileWithWebIdentityCredentialsProvider.cc
        )
        this_target_sources(${S3_PROVIDERS_SOURCES})
        this_target_object_libraries(
            baseline
            buffer
            cancelable_tasks
            cert_file
            config
            math
            stats
            stringx
            thread_pool
        )
        # Add S3 dependencies
        if (TILEDB_VCPKG)
            find_package(AWSSDK_EP REQUIRED COMPONENTS s3)
            target_link_libraries(s3_providers INTERFACE ${AWSSDK_LINK_LIBRARIES})
        else()
            find_package(AWSSDK_EP REQUIRED COMPONENTS s3)
            target_link_libraries(s3_providers INTERFACE
                AWSSDK::aws-cpp-sdk-s3
                AWSSDK::aws-cpp-sdk-core
                AWSSDK::aws-c-event-stream
                AWSSDK::aws-checksums
                AWSSDK::aws-c-common
                AWSSDK::aws-cpp-sdk-identity-management
                AWSSDK::aws-cpp-sdk-sts
            )
        endif()
    endif()
conclude(object_library)