#
# external/blosc/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017-2021 TileDB, Inc.
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
# Precondition
#   Expects caller to be <top>/tiledb/CMakeLists.txt. CMake (as of 3.20) does
#   not have a way of retrieving the caller's directories, nor of passing
#   argument for the callee in add_subdirectory(). Here we avoid premature
#   modularization and hard-code the caller.
# Outputs, all variables set in parent scope
#   TileDB_blosc_SOURCES
#   TileDB_blosc_INCLUDE_DIRS
#   TileDB_blosc_COMPILE_OPTIONS

set(SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

list(APPEND SOURCES
  ${SOURCE_DIR}/src/shuffle.c
  ${SOURCE_DIR}/src/shuffle-generic.c
  ${SOURCE_DIR}/src/bitshuffle-stub.c
)
if(COMPILER_SUPPORTS_AVX2)
  list(APPEND SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/shuffle-avx2.c
    )
endif()
try_compile(SSE2_DETECTED ${CMAKE_CURRENT_BINARY_DIR} ${SOURCE_DIR}/cmake/detect-sse2.c)
if(SSE2_DETECTED)
  list(APPEND SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/shuffle-sse2.c
    )
endif()


# Blosc requires a pthread library. There's a substitute one available for Windows for
# use with Visual C++, which doesn't have it. The following checks if pthreads is
# already enable, and adds the substitute Windows pthread library for Visual C++.
# Variables used for checking are set by CMake FindThreads module.
if (CMAKE_USE_PTHREADS_INIT AND NOT MSVC)
  message(DEBUG "Using system-provided pthread")
elseif(CMAKE_USE_WIN32_THREADS_INIT OR MSVC)
  message(DEBUG "Using blosc-provided pthread substitute for Win32")
  list(APPEND SOURCES
    # ${SOURCE_DIR}/src/win32/pthread.c
  )
else()
  message(FATAL_ERROR "Thread package found, but not a supported one")
endif()

# The blosc files are external and we ignore their warnings.
# -w is common to gcc and MSVC
set(TILEDB_BLOSC_COMPILE_OPTIONS "-w")
# If we found pthreads, activate it by defining PTHREAD_AVAILABLE.
if (CMAKE_USE_PTHREADS_INIT AND NOT MSVC)
  string(APPEND TILEDB_BLOSC_COMPILE_OPTIONS ";-DPTHREAD_AVAILABLE")
endif()

#
# Set the outputs
#
# CMake makes it difficult to have modular build files. We're trying hard
# anyway. We can't get good encapsulation, but that doesn't mean we can't have
# any.
#
# We can't return our results as an object library because (a) it's too new and
# (b) it doesn't even work as we need it to. As of CMake 3.20, when targetA
# depends on an object library target objB, $<TARGET_OBJECTS:objB> is not
# incorporated into $<TARGET_OBJECTS:targetA>, meaning objB isn't encapsulated.
# It would be an unacceptable maintenance burden to add objB everywhere
# $<TARGET_OBJECTS:targetA> is used. So instead of using a library, we set
# variables TileDB_blosc_SOURCES and TileDB_blosc_INCLUDE_DIRS in the parent
# scope, where they are added to existing lists.
#
# blosc requires it's own, distinct compilation options. If we were able to use
# a library, that would be the end of it. Instead we need to set compilation
# options in the caller context. A draft of the present code had used the
# following statement to set these properties in the absence of using a library.
#
#   set_source_files_properties(
#    ${SOURCES}
#    DIRECTORY ${CMAKE_SOURCE_DIR}/tiledb
#    PROPERTIES COMPILE_OPTIONS "${TILEDB_BLOSC_COMPILE_OPTIONS}"
#
# The DIRECTORY option was introduced in CMake 3.18, too new for ubuntu-20.04,
# which ships with 3.16. Note that even then there's no "DIRECTORY PARENT"
# syntax available. Instead we return a variable holding the options and then
# set them in the caller.
#
# There's one cheat here. We're not passing a link option here. At most it would
# be for pthread, and that's already being linked.
#
set(TileDB_blosc_SOURCES ${SOURCES} PARENT_SCOPE)
set(TileDB_blosc_INCLUDE_DIRS ${SOURCE_DIR}/include PARENT_SCOPE)
set(TileDB_blosc_COMPILE_OPTIONS ${TILEDB_BLOSC_COMPILE_OPTIONS} PARENT_SCOPE)
set(TileDB_blosc_LINK_OPTIONS ${CMAKE_THREAD_LIBS_INIT} PARENT_SCOPE)
