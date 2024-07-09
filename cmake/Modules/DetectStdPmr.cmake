#
# DetectStdPmr.cmake
#
#
# The MIT License
#
# Copyright (c) 2024 TileDB, Inc.
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
# Detect whether polymorphic allocators are available on the system.

# Special case for macOS when the CMAKE_OSX_DEPLOYMENT_TARGET is set to anything
# less than 14. For some reason, std::pmr is still detectable, but the resulting
# binary dies with a dyld missing symbol error.

if (APPLE AND CMAKE_OSX_DEPLOYMENT_TARGET LESS 14)
  set(TILEDB_USE_CPP17_PMR ON)
  message(STATUS "Using vendored cpp17::pmr for polymorphic allocators because of macOS deployment target ${CMAKE_OSX_DEPLOYMENT_TARGET} < 14")
  return()
endif()

# Otherwise, if we're not building a targeted macOS version, we just detect
# whether std::pmr is available.

if (CMAKE_VERSION VERSION_LESS "3.25")
try_compile(
  TILEDB_CAN_COMPILE_STD_PMR
  "${CMAKE_CURRENT_BINARY_DIR}"
  "${CMAKE_SOURCE_DIR}/cmake/inputs/detect_std_pmr.cc"
  )
else()
# CMake 3.25+ has a better signature for try_compile.
try_compile(
  TILEDB_CAN_COMPILE_STD_PMR
  SOURCES "${CMAKE_SOURCE_DIR}/cmake/inputs/detect_std_pmr.cc"
)
endif()

if (TILEDB_CAN_COMPILE_STD_PMR)
  message(STATUS "Using std::pmr for polymorphic allocators")
else()
  set(TILEDB_USE_CPP17_PMR ON)
  message(STATUS "Using vendored cpp17::pmr for polymorphic allocators")
endif()
