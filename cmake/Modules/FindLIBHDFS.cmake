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
# Finds the HDFS native library. This module defines:
#   - LIBHDFS_INCLUDE_DIR, directory containing the hdfs.h header
#   - LIBHDFS_LIBRARY, the libhdfs library path
#   - LIBHDFS_FOUND, the hdfs.h header, libjvm and libhdfs libraries have been found


if(NOT LIBHDFS_FOUND)
    message(STATUS "Searching for libhdfs")
    if(NOT DEFINED ${HADOOP_HOME})
        if (DEFINED ENV{HADOOP_HOME})
            set(HADOOP_HOME "$ENV{HADOOP_HOME}")
	endif()
    endif()
    if( "${HADOOP_HOME}" STREQUAL "" )
	message(STATUS "HADOOP_HOME not specified")
    else()
	message(STATUS "HADOOP_HOME is set to ${HADOOP_HOME}")
        list(APPEND POSSILE_PATHS
             "${HADOOP_HOME}"
             "${HADOOP_HOME}/lib"
             "${HADOOP_HOME}/lib/native"
	     "${HADOOP_HOME}/include"
	     "/usr/include")
    endif()

    find_path(LIBHDFS_INCLUDE_DIR NAMES hdfs.h PATHS ${POSSILE_PATHS} NO_DEFAULT_PATH)

    find_library(LIBHDFS_LIBRARY NAMES
        libhdfs${CMAKE_SHARED_LIBRARY_SUFFIX}
	libhdfs${CMAKE_STATIC_LIBRARY_SUFFIX}
	PATHS ${POSSILE_PATHS}
        NO_DEFAULT_PATH)

    if(LIBHDFS_INCLUDE_DIR)
	message(STATUS "Found hdfs.h header file: ${LIBHDFS_INCLUDE_DIR}")
    else()
        message(STATUS "hdfs.h header file not found")
    endif()

    if(LIBHDFS_LIBRARY)
	message(STATUS "Found libhdfs library: ${LIBHDFS_LIBRARY}")
    else()
        message(STATUS "libhdfs library not found")
    endif()

    if(JAVA_JVM_LIBRARY AND LIBHDFS_LIBRARY AND LIBHDFS_INCLUDE_DIR)
        set(LIBHDFS_FOUND TRUE)
    else()
	set(LIBHDFS_FOUND FALSE)
    endif()
endif()

if(LIBHDFS_FIND_REQUIRED AND NOT LIBHDFS_FOUND)
    message(FATAL_ERROR "Could not find the libhdfs library.")
endif()
