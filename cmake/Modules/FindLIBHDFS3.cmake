#
# FindHDFS.cmake
#
#
# The MIT License
#
# Copyright (c) 2016 MIT and Intel Corporation
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
# Finds the libhdfs3 c++ library with a convient RPC interface to hdfs. This module defines:
#   - LIBHDFS3_INCLUDE_DIR, directory containing the hdfs.h header
#   - LIBHDFS3_LIBRARY, the libhdfs3 library path
#   - LIBHDFS3_FOUND, the hdfs.h header and the libhdfs3 library have been found


if(NOT LIBHDFS3_FOUND)
    message(STATUS "Searching for libhdfs3")

    find_path(LIBHDFS3_INCLUDE_DIR NAMES hdfs.h PATHS "/usr/include/hdfs")

    find_library(LIBHDFS3_LIBRARY NAMES
        libhdfs3${CMAKE_SHARED_LIBRARY_SUFFIX})

    if(LIBHDFS3_LIBRARY)
	message(STATUS "Found libhdfs3 library: ${LIBHDFS_LIBRARY}")
    else()
        message(STATUS "libhdfs library not found")
    endif()

    if(LIBHDFS3_INCLUDE_DIR)
	message(STATUS "Found hdfs.h header file: ${LIBHDFS_INCLUDE_DIR}")
    else()
        message(STATUS "hdfs.h header file not found")
    endif()

    if(LIBHDFS3_LIBRARY AND LIBHDFS3_INCLUDE_DIR)
        set(LIBHDFS3_FOUND TRUE)
    else()
	set(LIBHDFS3_FOUND FALSE)
    endif()
endif()

if(LIBHDFS3_FIND_REQUIRED AND NOT LIBHDFS3_FOUND)
    message(FATAL_ERROR "Could not find the libhdfs3 library.")
endif()
