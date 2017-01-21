#
# FindLZ4.cmake
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
# Finds the LZ4 library. This module defines:
#   - LZ4_INCLUDE_DIR, directory containing headers
#   - LZ4_LIBRARIES, the LZ4 library path
#   - LZ4_FOUND, whether LZ4 has been found

# Find header files  
if(LZ4_SEARCH_HEADER_PATHS)
  find_path( 
      LZ4_INCLUDE_DIR lz4.h 
      PATHS ${LZ4_SEARCH_HEADER_PATHS}   
      NO_DEFAULT_PATH
  )
else()
  find_path(LZ4_INCLUDE_DIR lz4.h)
endif()

# Find library
if(LZ4_SEARCH_LIB_PATH)
  find_library(
      LZ4_LIBRARIES NAMES liblaz4.a liblz4${SHARED_LIB_SUFFIX}
      PATHS ${LZ4_SEARCH_LIB_PATH}$
      NO_DEFAULT_PATH
  )
else()
  find_library(LZ4_LIBRARIES NAMES liblz4.a liblz4${SHARED_LIB_SUFFIX})
endif()

if(LZ4_INCLUDE_DIR AND LZ4_LIBRARIES)
  message(STATUS "Found LZ4: ${LZ4_LIBRARIES}")
  set(LZ4_FOUND TRUE)
else()
  set(LZ4_FOUND FALSE)
endif()

if(LZ4_FIND_REQUIRED AND NOT LZ4_FOUND)
  message(FATAL_ERROR "Could not find the LZ4 library.")
endif()

