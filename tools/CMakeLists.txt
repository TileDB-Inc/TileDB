#
# tools/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2018-2021 TileDB, Inc.
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

if(TILEDB_VCPKG)
  find_package(Clipp CONFIG REQUIRED)
else()
  find_package(Clipp_EP REQUIRED)
endif()

add_executable(tiledb EXCLUDE_FROM_ALL
  src/commands/help_command.cc
  src/commands/info_command.cc
  src/main/tiledb.cc
  $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>
)

# List of all tool targets
set(TILEDB_TOOLS tiledb)

# Set up link and include directories for all tools
foreach(TOOL ${TILEDB_TOOLS})
  target_link_libraries(${TOOL} PRIVATE
    TILEDB_CORE_OBJECTS_ILIB
    Clipp::Clipp
  )
  target_include_directories(${TOOL} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
endforeach()

# Add custom target 'tools' to build all tools.
add_custom_target(tools ALL DEPENDS ${TILEDB_TOOLS})