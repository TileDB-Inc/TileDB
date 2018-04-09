#
# tiledb/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2017-2018 TileDB, Inc.
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

add_subdirectory(cli)

# Include main header directory.
set(TILEDB_CORE_INCLUDE_DIR "${CMAKE_SOURCE_DIR}")
include_directories(BEFORE ${TILEDB_CORE_INCLUDE_DIR})
# Also include C/C++ API header directories so that those files
# can be directly included (required for installed headers to locate
# dependent headers).
include_directories(BEFORE
  "${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/c_api"
  "${TILEDB_CORE_INCLUDE_DIR}/tiledb/sm/cpp_api")

# Gather the core source files
file(GLOB_RECURSE TILEDB_CORE_HEADERS "sm/*.h")
file(GLOB_RECURSE TILEDB_CORE_SOURCES "sm/*.cc")

# Gather the external source files
file(GLOB_RECURSE TILEDB_CORE_EXT_HEADERS 
       "${CMAKE_SOURCE_DIR}/external/include/spdlog/*.h"
       "${CMAKE_SOURCE_DIR}/external/include/md5/*"
)
file(GLOB_RECURSE TILEDB_CORE_EXT_SOURCES 
       "${CMAKE_SOURCE_DIR}/external/src/md5/*"
)

# Compile sources with PIC
add_library(TILEDB_CORE_OBJECTS OBJECT 
    ${TILEDB_CORE_SOURCES} 
    ${TILEDB_CORE_EXT_SOURCES} 
    ${TILEDB_CORE_HEADERS} 
    ${TILEDB_CORE_EXT_HEADERS}
)
set_property(TARGET TILEDB_CORE_OBJECTS PROPERTY POSITION_INDEPENDENT_CODE ON)
target_include_directories(
    TILEDB_CORE_OBJECTS PRIVATE
    "${CMAKE_SOURCE_DIR}/external/include"
)

# C++ library, allow the VISIBLITY_PRESET for each c++ target to work
cmake_policy(SET CMP0063 NEW) 

# Create static library
add_library(tiledb_static STATIC $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)
if (WIN32)
  # On Windows we must name the static library something else to avoid
  # name clash with the DLL's "import library" .lib file.
  set_target_properties(tiledb_static PROPERTIES OUTPUT_NAME "tiledbstatic")
else()
  set_target_properties(tiledb_static PROPERTIES OUTPUT_NAME "tiledb")
endif()

# Create dynamic library
add_library(tiledb_shared SHARED $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)
set_target_properties(tiledb_shared PROPERTIES OUTPUT_NAME "tiledb")
target_link_libraries(tiledb_shared ${TILEDB_LIB_DEPENDENCIES})

# Link Windows API libraries
if (WIN32)
  set(WIN32_LIBS
    Shlwapi
  )
  foreach (LIB ${WIN32_LIBS})
    find_library(${LIB} $LIB)
    target_link_libraries(tiledb_static ${LIB})
    target_link_libraries(tiledb_shared ${LIB})
  endforeach()
endif()

# Install libraries.
# Note on Windows, the DLL counts as "runtime" and should go into bin.
install(
    TARGETS tiledb_static tiledb_shared ${CLI_TARGETS}
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
file(GLOB TILEDB_HEADERS "sm/c_api/*.h" "sm/cpp_api/*.h" "sm/cpp_api/tiledb")
install(FILES ${TILEDB_HEADERS} DESTINATION "include/tiledb/")
