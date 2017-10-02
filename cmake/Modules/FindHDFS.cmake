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
#   - HDFS_INCLUDE_DIR, directory containing the hdfs.h header
#   - HDFS_LIBRARY, the libhdfs library path
#   - HDFS_FOUND, the hdfs.h header, libjvm and libhdfs libraries have been found


if(NOT HDFS_FOUND)
    message("Searching for libhdfs")
    if(NOT DEFINED ${HADOOP_HOME})
        if (DEFINED ENV{HADOOP_HOME})
            set(HADOOP_HOME "$ENV{HADOOP_HOME}")
	endif()
    endif()
    if( "${HADOOP_HOME}" STREQUAL "" )
        message("HADOOP_HOME not specified")
    else()
        message("HADOOP_HOME is set to ${HADOOP_HOME}")
        list(APPEND POSSILE_PATHS
             "${HADOOP_HOME}"
             "${HADOOP_HOME}/lib"
             "${HADOOP_HOME}/lib/native"
	     "${HADOOP_HOME}/include"
	     "/usr/include")
    endif()

    message("Searching for JVM and JNI paths")
    find_package(JNI)

    if(JAVA_JVM_LIBRARY)
        message(STATUS "Found libjvm library: ${JAVA_JVM_LIBRARY}")
    else()
        message(STATUS "libjvm library not found")
    endif()

    message("Exploring these paths to find libhdfs and hdfs.h: ${POSSILE_PATHS}.")

    find_path(HDFS_INCLUDE_DIR NAMES hdfs.h PATHS ${POSSILE_PATHS} NO_DEFAULT_PATH)

    find_library(HDFS_LIBRARY NAMES
        libhdfs${CMAKE_SHARED_LIBRARY_SUFFIX}
	libhdfs${CMAKE_STATIC_LIBRARY_SUFFIX}
        PATHS ${POSSILE_PATHS}
        NO_DEFAULT_PATH)

    if(HDFS_SHARED_LIB)
	message(STATUS "Found libhdfs library: ${HDFS_LIBRARY}")
    else()
        message(STATUS "libhdfs library not found")
    endif()

    if(HDFS_INCLUDE_DIR)
        message(STATUS "Found hdfs.h header file: ${HDFS_INCLUDE_PATH}")
    else()
        message(STATUS "hdfs.h header file not found")
    endif()

    if(JAVA_JVM_LIBRARY AND HDFS_LIBRARY AND HDFS_INCLUDE_DIR)
        set(HDFS_FOUND TRUE)
    else()
        set(HDFS_FOUND FALSE)
    endif()
endif()

if(HDFS_FIND_REQUIRED AND NOT HDFS_FOUND)
    message(FATAL_ERROR "Could not find the libhdfs library.")
endif()
