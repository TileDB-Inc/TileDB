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
#   - HDFS_INCLUDE_DIR, directory containing headers
#   - HDFS_LIBRARIES, the HDFS library path
#   - JVM_LIBRARIES, the JVM library path
#   - HDFS_FOUND, whether HDFS has been found


if (NOT HDFS_FOUND)
  MESSAGE("-- Searching for jvm")
  IF ( "${JAVA_HOME}" STREQUAL "" )
     MESSAGE("---JAVA_HOME not specified")
  ELSE ()
     LIST (APPEND POSSILE_PATHS_JVM
          "${JAVA_HOME}"
          "${JAVA_HOME}/lib/amd64/server/"
        )
  ENDIF()

  MESSAGE("--  Exploring these paths to find libjvm: ${POSSILE_PATHS_JVM}.")

  FIND_FILE (JVM_LIBRARIES NAMES libjvm.so PATHS ${POSSILE_PATHS_JVM} NO_DEFAULT_PATH)


  MESSAGE("-- Searching for libhdfs")
  IF ( "${HADOOP_HOME}" STREQUAL "" )
     MESSAGE("---HADOOP_HOME not specified")
  ELSE ()
     LIST (APPEND POSSILE_PATHS
          "${HADOOP_HOME}"
          "${HADOOP_HOME}/lib"
          "${HADOOP_HOME}/lib/native"
          "${HADOOP_HOME}/include"
        )
  ENDIF()

  MESSAGE("--  Exploring these paths to find libhdfs and hdfs.h: ${POSSILE_PATHS}.")

  FIND_PATH (HDFS_INCLUDE_DIR NAMES hdfs.h PATHS ${POSSILE_PATHS} NO_DEFAULT_PATH)
  FIND_LIBRARY (HDFS_LIBRARIES NAMES hdfs PATHS ${POSSILE_PATHS} NO_DEFAULT_PATH)

  if(JVM_LIBRARIES AND HDFS_LIBRARIES AND HDFS_INCLUDE_DIR)
    message(STATUS "Found JVM libraries: ${JVM_LIBRARIES}")
    message(STATUS "Found HDFS libraries: ${HDFS_LIBRARIES}")
    message(STATUS "Found HDFS include: ${HDFS_INCLUDE_DIR}")
    set(HDFS_FOUND TRUE)
  else()
    set(HDFS_FOUND FALSE)
  endif()
ENDIF()


if(HDFS_FIND_REQUIRED AND NOT HDFS_FOUND)
  message(FATAL_ERROR "Could not find the HDFS native library.")
endif()
