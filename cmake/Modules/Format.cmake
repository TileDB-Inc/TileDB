#
# Format.cmake
#
# The MIT License
#
# Copyright (c) 2023 TileDB, Inc.
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

############################################################
# "make format" and "make check-format" targets
############################################################

set(SCRIPTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/scripts")

find_package(ClangTools)
if (NOT ${CLANG_FORMAT_FOUND})
  find_program(CLANG_FORMAT_BIN NAMES clang-format-17)
  if(CLANG_FORMAT_BIN)
    set(CLANG_FORMAT_FOUND TRUE)
  endif()
endif()
if (${CLANG_FORMAT_FOUND})
  message(STATUS "clang hunt, found ${CLANG_FORMAT_BIN}")
  # runs clang format and updates files in place.

  add_custom_target(format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 1)

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format ${SCRIPTS_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 0)
else()
  message(STATUS "was unable to find clang-format")
endif()
