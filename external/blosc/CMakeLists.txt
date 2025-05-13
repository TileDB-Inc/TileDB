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

set(SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})

add_library(blosc
  OBJECT
  ${SOURCE_DIR}/src/shuffle.c
  ${SOURCE_DIR}/src/shuffle-generic.c
  ${SOURCE_DIR}/src/bitshuffle-stub.c
)
target_include_directories(blosc PUBLIC ${SOURCE_DIR}/include)

if(COMPILER_SUPPORTS_AVX2)
  target_sources(blosc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/shuffle-avx2.c)
endif()

try_compile(SSE2_DETECTED ${CMAKE_CURRENT_BINARY_DIR} ${SOURCE_DIR}/cmake/detect-sse2.c)
if(SSE2_DETECTED)
  target_sources(blosc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/shuffle-sse2.c)
endif()


# Blosc requires a pthread library. There's a substitute one available for Windows for
# use with Visual C++, which doesn't have it. The following checks if pthreads is
# already enable, and adds the substitute Windows pthread library for Visual C++.
# Variables used for checking are set by CMake FindThreads module.
if (CMAKE_USE_PTHREADS_INIT AND NOT MSVC)
  message(DEBUG "Using system-provided pthread")
elseif(CMAKE_USE_WIN32_THREADS_INIT OR MSVC)
  message(DEBUG "Using blosc-provided pthread substitute for Win32")
  target_sources(blosc PRIVATE ${SOURCE_DIR}/src/win32/pthread.c)
else()
  message(FATAL_ERROR "Thread package found, but not a supported one")
endif()

# The blosc files are external and we ignore their warnings.
# -w is common to gcc and MSVC
target_compile_options(blosc PUBLIC "-w")
# If we found pthreads, activate it by defining PTHREAD_AVAILABLE.
if (CMAKE_USE_PTHREADS_INIT AND NOT MSVC)
  target_compile_options(blosc PUBLIC "-DPTHREAD_AVAILABLE")
endif()
