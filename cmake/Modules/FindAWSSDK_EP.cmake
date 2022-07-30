#
# FindAWSSDK_EP.cmake
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
# This module finds the AWS C++ SDK, installing it with an ExternalProject if
# necessary. It then defines the imported targets AWSSDK::<component>, e.g.
# AWSSDK::aws-cpp-sdk-s3 or AWSSDK::aws-cpp-sdk-core.

# Include some common helper functions.
include(TileDBCommon)

# If the EP was built, it will install the AWSSDKConfig.cmake file, which we
# can use with find_package. CMake uses CMAKE_PREFIX_PATH to locate find
# modules.
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "${TILEDB_EP_INSTALL_PREFIX}")

if(DEFINED ENV{AWSSDK_ROOT_DIR})
  set(AWSSDK_ROOT_DIR $ENV{AWSSDK_ROOT_DIR})
else()
  set(AWSSDK_ROOT_DIR "${TILEDB_EP_INSTALL_PREFIX}")
endif()

# Check to see if the SDK is installed (which provides the find module).
# This will either use the system-installed AWSSDK find module (if present),
# or the superbuild-installed find module.
if (TILEDB_SUPERBUILD)
  # Don't use find_package in superbuild if we are forcing all deps.
  # That's because the AWSSDK config file hard-codes a search of /usr,
  # /usr/local, etc.
  if (NOT TILEDB_FORCE_ALL_DEPS)
    find_package(AWSSDK CONFIG)
  endif()
else()
  find_package(AWSSDK CONFIG)
endif()

if (NOT AWSSDK_FOUND)
  if (TILEDB_SUPERBUILD)
    message(STATUS "Adding AWSSDK as an external project")

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

    # Set AWS cmake build to use specified build type except for gcc
    # For aws sdk and gcc we must always build in release mode
    # See https://github.com/TileDB-Inc/TileDB/issues/1351 and
    # https://github.com/awslabs/aws-checksums/issues/8
    set(AWS_CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE})
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(AWS_CMAKE_BUILD_TYPE "Release")
    endif()

    # Work around for: https://github.com/aws/aws-sdk-cpp/pull/1187
    # AWS SDK builds it's "aws-common" dependencies using `execute_process` to run cmake directly,
    # and does not pass the CMAKE_GENERATOR PLATFORM, so those projects default to Win32 builds,
    # causing linkage errors.
    if (CMAKE_GENERATOR MATCHES "Visual Studio 14.*" OR CMAKE_GENERATOR MATCHES "Visual Studio 15.*"
      AND NOT CMAKE_GENERATOR MATCHES ".*Win64")

      set(_CMKGEN_OLD "${CMAKE_GENERATOR}")
      set(_CMKPLT_OLD "${CMAKE_GENERATOR_PLATFORM}")
      set(CMAKE_GENERATOR "${CMAKE_GENERATOR} Win64")
      set(CMAKE_GENERATOR_PLATFORM "")
    endif()

    if (WIN32)
      find_package(Git REQUIRED)
      set(CONDITIONAL_PATCH cd ${CMAKE_SOURCE_DIR} && ${GIT_EXECUTABLE} apply --ignore-whitespace -p1 --unsafe-paths --verbose --directory=${TILEDB_EP_SOURCE_DIR}/ep_awssdk < ${TILEDB_CMAKE_INPUTS_DIR}/patches/ep_awssdk/aws_event_loop_windows.v1.9.291.patch)
    else()
      set(CONDITIONAL_PATCH "")
    endif()

    ExternalProject_Add(ep_awssdk
      PREFIX "externals"
      # Set download name to avoid collisions with only the version number in the filename
      DOWNLOAD_NAME ep_awssdk.zip
      GIT_REPOSITORY
        "https://github.com/aws/aws-sdk-cpp"
      GIT_TAG 1.9.291
      GIT_SUBMODULES_RECURSE ON #may be default, specifying for certainty.
      PATCH_COMMAND
        ${CONDITIONAL_PATCH}
      CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=${AWS_CMAKE_BUILD_TYPE}
        -DENABLE_TESTING=OFF
        -DBUILD_ONLY=s3\\$<SEMICOLON>core\\$<SEMICOLON>identity-management\\$<SEMICOLON>sts
        -DBUILD_SHARED_LIBS=OFF
        -DCMAKE_INSTALL_BINDIR=lib
        -DENABLE_UNITY_BUILD=ON
        -DCUSTOM_MEMORY_MANAGEMENT=0
        -DCMAKE_PREFIX_PATH=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_INSTALL_PREFIX=${TILEDB_EP_INSTALL_PREFIX}
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DCMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}
        -DCMAKE_OSX_SYSROOT=${CMAKE_OSX_SYSROOT}
      UPDATE_COMMAND ""
      LOG_DOWNLOAD TRUE
      LOG_CONFIGURE TRUE
      LOG_BUILD TRUE
      LOG_INSTALL TRUE
      LOG_OUTPUT_ON_FAILURE ${TILEDB_LOG_OUTPUT_ON_FAILURE}
      DEPENDS ${DEPENDS}
    )

    # restore cached values
    if (DEFINED _CMKGEN_OLD)

      set(CMAKE_GENERATOR "${_CMKGEN_OLD}")
      set(CMAKE_GENERATOR_PLATFORM "${_CMKPLT_OLD}")
    endif()

    list(APPEND TILEDB_EXTERNAL_PROJECTS ep_awssdk)
    list(APPEND FORWARD_EP_CMAKE_ARGS
      -DTILEDB_AWSSDK_EP_BUILT=TRUE
    )
  else ()
    message(FATAL_ERROR "Could not find AWSSDK (required).")
  endif ()
endif ()

if (AWSSDK_FOUND)
  if(0)
    # will something similar work from tiledb?
    #hmm, 2016 post at https://aws.amazon.com/blogs/developer/using-cmake-exports-with-the-aws-sdk-for-c/
    # suggests
    find_package(aws-sdk-cpp)
    # but searching in tree not finding either a findaws-sdk-cpp.cmake nor related *-config.cmake...
    # ???

    #add_executable(s3-sample main.cpp)

    #since we called find_package(), this will resolve all dependencies, header files, and cflags necessary
    #to build and link your executable.
    #target_link_libraries(s3-sample aws-cpp-sdk-s3)
  else()
  set(AWS_SERVICES s3)
  if(0)
  # descriptions of macros @ https://aws.amazon.com/blogs/developer/developer-experience-of-the-aws-sdk-for-c-now-simplified-by-cmake/
  # The description does not seem to indicate appropriate dependency ordering for name resolution, even though
  # comments seen elsewhere seemed to indicateit should be - direct usage attempts below seem to indicate
  # even if intended, dependency ordering is not reliable.
  # TBD: Maybe I've just not done something else correctly...?
  AWSSDK_DETERMINE_LIBS_TO_LINK(AWS_SERVICES AWS_LINKED_LIBS)
  # -- findaws, processing AWS_LINKED_LIBS (aws-cpp-sdk-s3;aws-cpp-sdk-core;aws-crt-cpp;aws-c-auth;aws-c-cal;aws-c-common;aws-c-compression;aws-c-event-stream;aws-c-http;aws-c-io;aws-c-mqtt;aws-c-s3;aws-checksums;aws-c-sdkutils;pthread;crypto;ssl;z;curl;;core)
  # libs are -not- returned in correct link order dependency, and attempting to re-add them (duplicate)
  # onto end apparently falls to cmake's removal of duplicates when it does not recognize the
  # dependencies involved as mentioned in https://cmake.org/pipermail/cmake/2011-February/042504.html
  # one example
  # pkcs11.c:(.text+0xb2a): undefined reference to `aws_ref_count_acquire' [found in aws-c-common]
  # /usr/bin/ld: ../../externals/install/lib/libaws-c-io.a(pkcs11.c.o): in function `aws_pkcs11_lib_new':
  # actual link line showing aws-c-common (containing aws_ref_count_acquire) before aws-c-io (containing aws_pkcsll_lib_new)
  # tho aws_kcs11_lib_new needs aws-c-common to come -after- aws-c-io to satisfy order dependency
  # /usr/bin/c++ -O2 -g -DNDEBUG -Wl,--no-as-needed -ldl CMakeFiles/tiledb_unit.dir/src/helpers.cc.o CMakeFiles/tiledb_unit.dir/src/serialization_wrappers.cc.o CMakeFiles/tiledb_unit.dir/src/unit-azure.cc.o CMakeFiles/tiledb_unit.dir/src/unit-backwards_compat.cc.o CMakeFiles/tiledb_unit.dir/src/unit-bufferlist.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-any.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-array.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-array_schema.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-async.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-attributes.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-buffer.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-config.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-context.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-consolidation.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-dense_array.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-dense_array_2.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-dense_neg.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-dense_vector.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-enum_values.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-error.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-filestore.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-fill_values.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-filter.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-fragment_info.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-group.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-incomplete.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-incomplete-2.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-metadata.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-nullable.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-object_mgmt.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-query.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-query_2.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-smoke-test.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-sparse_array.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-sparse_heter.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-sparse_neg.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-sparse_neg_2.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-sparse_real.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-sparse_real_2.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-string.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-string_dims.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-uri.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-version.cc.o CMakeFiles/tiledb_unit.dir/src/unit-capi-vfs.cc.o CMakeFiles/tiledb_unit.dir/src/unit-CellSlabIter.cc.o CMakeFiles/tiledb_unit.dir/src/unit-compression-dd.cc.o CMakeFiles/tiledb_unit.dir/src/unit-compression-rle.cc.o CMakeFiles/tiledb_unit.dir/src/unit-crypto.cc.o CMakeFiles/tiledb_unit.dir/src/unit-ctx.cc.o CMakeFiles/tiledb_unit.dir/src/unit-DenseTiler.cc.o CMakeFiles/tiledb_unit.dir/src/unit-dimension.cc.o CMakeFiles/tiledb_unit.dir/src/unit-domain.cc.o CMakeFiles/tiledb_unit.dir/src/unit-duplicates.cc.o CMakeFiles/tiledb_unit.dir/src/unit-empty-var-length.cc.o CMakeFiles/tiledb_unit.dir/src/unit-filter-buffer.cc.o CMakeFiles/tiledb_unit.dir/src/unit-filter-pipeline.cc.o CMakeFiles/tiledb_unit.dir/src/unit-global-order.cc.o CMakeFiles/tiledb_unit.dir/src/unit-gcs.cc.o CMakeFiles/tiledb_unit.dir/src/unit-gs.cc.o CMakeFiles/tiledb_unit.dir/src/unit-hdfs-filesystem.cc.o CMakeFiles/tiledb_unit.dir/src/unit-lru_cache.cc.o CMakeFiles/tiledb_unit.dir/src/unit-tile-metadata.cc.o CMakeFiles/tiledb_unit.dir/src/unit-tile-metadata-generator.cc.o CMakeFiles/tiledb_unit.dir/src/unit-bytevecvalue.cc.o CMakeFiles/tiledb_unit.dir/src/unit-ReadCellSlabIter.cc.o CMakeFiles/tiledb_unit.dir/src/unit-Reader.cc.o CMakeFiles/tiledb_unit.dir/src/unit-resource-pool.cc.o CMakeFiles/tiledb_unit.dir/src/unit-result-coords.cc.o CMakeFiles/tiledb_unit.dir/src/unit-result-tile.cc.o CMakeFiles/tiledb_unit.dir/src/unit-rtree.cc.o CMakeFiles/tiledb_unit.dir/src/unit-s3-no-multipart.cc.o CMakeFiles/tiledb_unit.dir/src/unit-s3.cc.o CMakeFiles/tiledb_unit.dir/src/unit-sparse-global-order-reader.cc.o CMakeFiles/tiledb_unit.dir/src/unit-sparse-unordered-with-dups-reader.cc.o CMakeFiles/tiledb_unit.dir/src/unit-status.cc.o CMakeFiles/tiledb_unit.dir/src/unit-Subarray.cc.o CMakeFiles/tiledb_unit.dir/src/unit-SubarrayPartitioner-dense.cc.o CMakeFiles/tiledb_unit.dir/src/unit-SubarrayPartitioner-error.cc.o CMakeFiles/tiledb_unit.dir/src/unit-SubarrayPartitioner-sparse.cc.o CMakeFiles/tiledb_unit.dir/src/unit-Tile.cc.o CMakeFiles/tiledb_unit.dir/src/unit-TileDomain.cc.o CMakeFiles/tiledb_unit.dir/src/unit-uuid.cc.o CMakeFiles/tiledb_unit.dir/src/unit-vfs.cc.o CMakeFiles/tiledb_unit.dir/src/unit-win-filesystem.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-array.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-checksum.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-config.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-consolidation-sparse.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-consolidation.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-consolidation-with-timestamps.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-datetimes.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-time.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-fill_values.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-filter.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-fragment_info.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-group.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-hilbert.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-metadata.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-nullable.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-query.cc.o CMakeFiles/tiledb_unit.dir/src/cpp-integration-query-condition.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-schema.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-string-dims.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-subarray.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-type.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-updates.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-util.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-var-offsets.cc.o CMakeFiles/tiledb_unit.dir/src/unit-cppapi-vfs.cc.o CMakeFiles/tiledb_unit.dir/src/unit.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/heap_memory.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/heap_profiler.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/logger.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/memory.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/stdx_string.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/interval/interval.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/types/dynamic_typed_datum.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/array/array.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/array/array_directory.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/array_schema/array_schema.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/array_schema/array_schema_evolution.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/array_schema/attribute.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/array_schema/dimension.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/array_schema/domain.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/buffer/buffer.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/buffer/buffer_list.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/c_api/api_argument_validator.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/c_api/tiledb.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/c_api/tiledb_filestore.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/c_api/tiledb_group.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/cache/buffer_lru_cache.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/compressors/bzip_compressor.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/compressors/dd_compressor.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/compressors/dict_compressor.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/compressors/gzip_compressor.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/compressors/lz4_compressor.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/compressors/rle_compressor.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/compressors/util/gzip_wrappers.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/compressors/zstd_compressor.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/config/config.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/config/config_iter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/consolidator/array_meta_consolidator.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/consolidator/commits_consolidator.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/consolidator/consolidator.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/consolidator/fragment_consolidator.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/consolidator/fragment_meta_consolidator.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/consolidator/group_meta_consolidator.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/crypto/crypto.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/crypto/encryption_key.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/crypto/encryption_key_validation.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/crypto/crypto_openssl.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/crypto/crypto_win32.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/azure.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/gcs.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/mem_filesystem.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/hdfs_filesystem.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/path_win.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/posix.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/s3.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/s3_thread_pool_executor.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/uri.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/vfs.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/vfs_file_handle.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filesystem/win.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/bit_width_reduction_filter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/bitshuffle_filter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/byteshuffle_filter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/checksum_md5_filter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/checksum_sha256_filter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/compression_filter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/encryption_aes256gcm_filter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/filter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/filter_buffer.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/filter_create.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/filter_pipeline.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/filter_storage.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/noop_filter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/filter/positive_delta_filter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/fragment/fragment_info.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/fragment/fragment_metadata.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/global_state/global_state.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/global_state/libcurl_state.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/global_state/signal_handlers.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/global_state/watchdog.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/group/group.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/group/group_directory.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/group/group_member.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/group/group_member_v1.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/group/group_v1.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/metadata/metadata.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/cancelable_tasks.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/constants.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/mgc_dict.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/parse_argument.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/tdb_math.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/tdb_time.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/types.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/utils.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/uuid.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/win_constants.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/misc/work_arounds.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/ast/query_ast.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/dense_reader.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/dense_tiler.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/global_order_writer.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/hilbert_order.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/ordered_writer.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/query.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/query_condition.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/reader.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/reader_base.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/result_tile.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/read_cell_slab_iter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/sparse_global_order_reader.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/sparse_index_reader_base.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/sparse_unordered_with_dups_reader.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/strategy_base.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/unordered_writer.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/query/writer_base.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/rest/rest_client.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/rtree/rtree.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/serialization/array.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/serialization/array_schema.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/serialization/array_schema_evolution.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/serialization/config.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/serialization/group.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/serialization/query.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/stats/global_stats.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/stats/stats.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/stats/timer_stat.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/storage_manager/context.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/storage_manager/storage_manager.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/subarray/cell_slab_iter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/subarray/range_subset.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/subarray/subarray.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/subarray/subarray_partitioner.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/subarray/subarray_tile_overlap.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/subarray/tile_cell_slab_iter.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/tile/tile.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/tile/generic_tile_io.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/tile/tile_metadata_generator.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/sm/tile/writer_tile.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/storage_format/uri/parse_uri.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/dynamic_memory/dynamic_memory.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/exception/exception.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/exception/status.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/governor/governor.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/common/thread_pool/thread_pool.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/__/external/src/md5/md5.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/__/external/src/bitshuffle/iochain.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/__/external/src/bitshuffle/bitshuffle_core.cc.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/__/external/blosc/src/shuffle.c.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/__/external/blosc/src/shuffle-generic.c.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/__/external/blosc/src/bitshuffle-stub.c.o ../tiledb/CMakeFiles/TILEDB_CORE_OBJECTS.dir/__/external/blosc/src/shuffle-avx2.c.o -o tiledb_unit  libtiledb_test_support_lib.a ../../externals/install/lib/libaws-cpp-sdk-s3.a ../../externals/install/lib/libaws-cpp-sdk-core.a ../../externals/install/lib/libaws-crt-cpp.a ../../externals/install/lib/libaws-c-auth.a ../../externals/install/lib/libaws-c-cal.a ../../externals/install/lib/libaws-c-common.a ../../externals/install/lib/libaws-c-compression.a ../../externals/install/lib/libaws-c-event-stream.a ../../externals/install/lib/libaws-c-http.a ../../externals/install/lib/libaws-c-io.a ../../externals/install/lib/libaws-c-mqtt.a ../../externals/install/lib/libaws-c-s3.a ../../externals/install/lib/libaws-checksums.a ../../externals/install/lib/libaws-c-sdkutils.a ../../externals/install/lib/libcurl.a /usr/lib/x86_64-linux-gnu/libbz2.so ../../externals/install/lib/liblz4.a ../../externals/install/lib/libspdlog.a /usr/lib/x86_64-linux-gnu/libz.so ../../externals/install/lib/libzstd.a ../../externals/install/lib/liblibmagic.a ../../externals/install/lib/libpcre2-posix.a ../../externals/install/lib/libpcre2-8.a /usr/lib/x86_64-linux-gnu/libssl.so /usr/lib/x86_64-linux-gnu/libcrypto.so -lpthread -ldl ../../externals/install/lib/libcurl.a -pthread

  AWSSDK_LIB_DEPS(AWS_SERVICES AWS_NEEDED_LIB_DEPENDENCIES)
  message(STATUS "AWS_NEEDED_LIB_DEPENDENCIES is ${AWS_NEEDED_LIB_DEPENDENCIES}")
  list(APPEND AWS_LINKED_LIBS "${AWS_NEEDED_LIB_DEPENDENCIES}")
  if(0) # was not helpful...
  # per https://github.com/aws/aws-sdk-cpp/issues/953,
  # following supposedly returns AWSSDK_LINKED_LIBRARIES handling correct dependency (order)
  find_package(AWSSDK REQUIRED COMPONENTS s3)
  message(STATUS "AWSSDK_LINKED_LIBRARIES is ${AWSSDK_LINKED_LIBRARIES}")
  message(STATUS "AWSSDK_LINK_LIBRARIES is ${AWSSDK_LINK_LIBRARIES}")
  #set(AWS_LINKED_LIBS "${AWSSDK_LINKED_LIBRARIES}")
  set(AWS_LINKED_LIBS "${AWSSDK_LINK_LIBRARIES}")
  endif()
  #list(APPEND AWS_LINKED_LIBS
  #        aws-cpp-sdk-identity-management
  #        aws-cpp-sdk-sts
  #        aws-c-common
  #        aws-c-auth
  #)
  #if (NOT WIN32)
  #  list(APPEND AWS_LINKED_LIBS #aws-c-common
  #        s2n # There is a libs2n.a generated and in install/lib path... but not an INTERFACE, hmm...
  #  )
  #endif()
  else()
  # append these libs in an order manually, by trial and error, determined to support successful
  # linking of dependencies on *nix.  (windows doesn't seem to care or 'just lucked out'.)
  list(APPEND AWS_LINKED_LIBS
#seems not needed...        aws-cpp-sdk-s3
        aws-cpp-sdk-core # needed,  unresolved external symbol "public: class std::basic_string<char,struct std::char_traits<char>,class std::allocator<char> > __cdecl Aws::Client::AWSClient::GeneratePresignedUrl(class Aws::Http::URI &,enum Aws::Http::HttpMethod,char const *,__int64)const
#        aws-crt-cpp
#seems not needed...        aws-c-event-stream
#seems not needed...        aws-c-mqtt
        aws-c-s3
        aws-c-auth  # needed for *nix, maybe not for windows?
        aws-c-http
#seems not needed        aws-c-compression
#seems not needed        aws-checksums
#seems not needed        aws-c-sdkutils
        aws-crt-cpp
        aws-cpp-sdk-s3
        aws-c-event-stream
        aws-c-mqtt
        aws-checksums
        # is this needed twice? cmake was eliminating some dups anyway, keeping first one encountered...
#        aws-c-auth
        aws-c-sdkutils
        aws-c-io
        aws-c-compression
        aws-c-cal
#seems not needed        aws-cpp-sdk-cognito-identity
        aws-cpp-sdk-identity-management
        aws-cpp-sdk-sts
        aws-c-common
  )
  if (NOT WIN32)
    list(APPEND AWS_LINKED_LIBS
        s2n # There is a libs2n.a generated and in install/lib path... but not an INTERFACE? hmm...
    )
  endif()
  endif()
  set(TILEDB_AWS_LINK_TARGETS "")
  message(STATUS "findaws, processing AWS_LINKED_LIBS (${AWS_LINKED_LIBS})")
  foreach (LIB ${AWS_LINKED_LIBS})
    if ((NOT ${LIB} MATCHES "aws-*" )
        AND (NOT LIB STREQUAL "s2n") )
      message(STATUS "AWS_LINKED_LIBS, skipping ${LIB}")
      continue()
    endif()

    find_library("AWS_FOUND_${LIB}"
      NAMES ${LIB}
      PATHS ${AWSSDK_LIB_DIR}
      ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )
    if ("${AWS_FOUND_${LIB}}" MATCHES ".*-NOTFOUND")
      message(STATUS "not found, ${LIB}")
      continue()
    else()
      message(STATUS "for ${LIB} adding link to ${AWS_FOUND_${LIB}}")
    endif()
    if (NOT TARGET AWSSDK::${LIB})
      add_library(AWSSDK::${LIB} UNKNOWN IMPORTED)
      set_target_properties(AWSSDK::${LIB} PROPERTIES
        IMPORTED_LOCATION "${AWS_FOUND_${LIB}}"
        INTERFACE_INCLUDE_DIRECTORIES "${AWSSDK_INCLUDE_DIR}"
      )
    endif()

    list(APPEND TILEDB_AWS_LINK_TARGETS "AWSSDK::${LIB}")

    # If we built a static EP, install it if required.
    if (TILEDB_AWSSDK_EP_BUILT AND TILEDB_INSTALL_STATIC_DEPS)
      install_target_libs(AWSSDK::${LIB})
    endif()

  endforeach ()

  message(STATUS "findaws, TILEDB_AWS_LINK_TARGETS is ${TILEDB_AWS_LINK_TARGETS}")

  endif()

  # the AWSSDK does not include links to some transitive dependencies
  # ref: github<dot>com/aws<slash>aws-sdk-cpp/issues/1074#issuecomment-466252911
  if (WIN32)
    list(APPEND AWS_EXTRA_LIBS userenv ws2_32 wininet winhttp bcrypt version crypt32 secur32 Ncrypt)
  endif()
endif ()
