#
# FindBzip2.cmake
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
# Finds the Bzip2 library. This module defines:
#   - BZIP2_INCLUDE_DIR, directory containing headers
#   - BZIP2_LIBRARIES, the Bzip2 library path
#   - BZIP2_FOUND, whether Bzip2 has been found

# Find header files  
if(BZIP2_SEARCH_HEADER_PATHS)
  find_path( 
      BZIP2_INCLUDE_DIR bzip2.h 
      PATHS ${BZIP2_SEARCH_HEADER_PATHS}   
      NO_DEFAULT_PATH
  )
else()
  find_path(BZIP2_INCLUDE_DIR bzlib.h)
endif()

# Find library
if(BZIP2_SEARCH_LIB_PATH)
  find_library(
      BZIP2_LIBRARIES NAMES bz2
      PATHS ${BZIP2_SEARCH_LIB_PATH}$
      NO_DEFAULT_PATH
  )
else()
  find_library(BZIP2_LIBRARIES NAMES bz2)
endif()

if(BZIP2_INCLUDE_DIR AND BZIP2_LIBRARIES)
  message(STATUS "Found Bzip2: ${BZIP2_LIBRARIES}")
  set(BZIP2_FOUND TRUE)
else()
  set(BZIP2_FOUND FALSE)
endif()

if(BZIP2_FIND_REQUIRED AND NOT BZIP2_FOUND)
  message(FATAL_ERROR "Could not find the Bzip2 library.")
endif()

