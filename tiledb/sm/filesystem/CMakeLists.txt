#
# tiledb/sm/vfs/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021-2023 TileDB, Inc.
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
# `vfs` object library
#
commence(object_library vfs)
    list(APPEND VFS_SOURCES
        mem_filesystem.cc
        path_win.cc
        posix.cc
        uri.cc
        vfs.cc
        vfs_file_handle.cc
        win.cc
        filesystem_base.cc
        local.cc
        ../curl/curl_init.cc
    )
    if (TILEDB_S3)
        target_include_directories(vfs
                PRIVATE
                "${CMAKE_CURRENT_SOURCE_DIR}/../../../external/include"
        )
        list(APPEND VFS_SOURCES
            s3.cc
            s3_thread_pool_executor.cc
        )
    endif()
    this_target_sources(${VFS_SOURCES})
    this_target_object_libraries(
        baseline
        buffer
        cancelable_tasks
        cert_file
        config
        math
        seedable_global_PRNG
        stats
        stringx
        thread_pool
    )
    if(WIN32)
        this_target_link_libraries(shlwapi)
    endif()
conclude(object_library)

add_test_subdirectory()
