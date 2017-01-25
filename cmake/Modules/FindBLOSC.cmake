#
# FindBLOSC.cmake
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
# Finds the Blosc library. This module defines:
#   - BLOSC_INCLUDE_DIR, directory containing headers
#   - BLOSC_LIBRARIES, the Blosc library path
#   - BLOSC_FOUND, whether Blosc has been found

# Find header files  
if(BLOSC_SEARCH_HEADER_PATHS)
  find_path( 
      BLOSC_INCLUDE_DIR blosc.h 
      PATHS ${BLOSC_SEARCH_HEADER_PATHS}   
      NO_DEFAULT_PATH
  )
else()
  find_path(BLOSC_INCLUDE_DIR blosc.h)
endif()

# Find library
if(BLOSC_SEARCH_LIB_PATH)
  find_library(
      BLOSC_LIBRARIES NAMES blosc
      PATHS ${BLOSC_SEARCH_LIB_PATH}$
      NO_DEFAULT_PATH
  )
else()
  find_library(BLOSC_LIBRARIES NAMES blosc)
endif()

if(BLOSC_INCLUDE_DIR AND BLOSC_LIBRARIES)
  message(STATUS "Found Blosc: ${BLOSC_LIBRARIES}")
  set(BLOSC_FOUND TRUE)
else()
  set(BLOSC_FOUND FALSE)
endif()

if(BLOSC_FIND_REQUIRED AND NOT BLOSC_FOUND)
  message(FATAL_ERROR "Could not find the Blosc library.")
endif()

