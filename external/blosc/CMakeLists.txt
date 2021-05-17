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
#   Expects caller to be <top>/tiledb/CMakeLists.txt. CMake (as of 3.20) does not have a way of retrieving the caller's
#   directories, nor of passing argument for the callee in add_subdirectory(). Here we avoid premature modularization
#   and hard-code the caller.
# Outputs
#   library TileDB-blosc-modified

set(SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

list(APPEND SOURCES
  ${SOURCE_DIR}/src/shuffle.c
  ${SOURCE_DIR}/src/shuffle-generic.c
  ${SOURCE_DIR}/src/bitshuffle-stub.c
)
try_compile(AVX2_DETECTED ${CMAKE_CURRENT_BINARY_DIR} ${SOURCE_DIR}/cmake/detect-avx2.c)
if(AVX2_DETECTED)
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

#
# Blosc requires a pthread library. There's a substitute one available for
# Windows for use with Visual C++, which doesn't have it. Other compilers do,
# however, and we use it if it's available. This avoids duplicate link symbols
# in case something else has a pthread dependency.
#
find_package(Threads REQUIRED)
message("Threads_FOUND=${Threads_FOUND}")
message("CMAKE_THREAD_LIBS_INIT=${CMAKE_THREAD_LIBS_INIT}")
message("CMAKE_USE_WIN32_THREADS_INIT=${CMAKE_USE_WIN32_THREADS_INIT}")
message("CMAKE_USE_PTHREADS_INIT=${CMAKE_USE_PTHREADS_INIT}")
message("CMAKE_HP_PTHREADS_INIT=${CMAKE_HP_PTHREADS_INIT}")
message("CMAKE_HAVE_PTHREAD_H=${CMAKE_HAVE_PTHREAD_H}")

if (CMAKE_USE_PTHREADS_INIT)
  message("Using system-provided pthread")
elseif(CMAKE_USE_WIN32_THREADS_INIT)
  message("Using blosc-provided pthread substitute for Win32")
  list(APPEND SOURCES
    ${SOURCE_DIR}/src/win32/pthread.c
  )
else()
  message(FATAL_ERROR "Thread package found, but not a supported one")
endif()

# The blosc files are external and we ignore their warnings.
# -w is common to gcc and MSVC
set(TILEDB_BLOSC_COMPILE_FLAGS "-w")
# If we found pthreads, activate it by defining PTHREAD_AVAILABLE.
if (CMAKE_USE_PTHREADS_INIT)
  string(APPEND TILEDB_BLOSC_COMPILE_FLAGS " -DPTHREAD_AVAILABLE")
endif()
set_source_files_properties(
  ${SOURCES}
  PROPERTIES COMPILE_FLAGS ${TILEDB_BLOSC_COMPILE_FLAGS}
)

#
# Set up imported interface-library that acts as the output of this file
#
add_library(TileDB-blosc-modified OBJECT ${SOURCES})
target_link_libraries(TileDB-blosc-modified INTERFACE Threads::Threads)
target_include_directories(TileDB-blosc-modified PUBLIC ${SOURCE_DIR}/include)
