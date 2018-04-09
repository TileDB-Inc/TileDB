#
# tiledb/cli/CMakeLists.txt
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

set(CLI_TARGETS)

function(build_cli TARGET)
    add_executable(tiledb-${TARGET} EXCLUDE_FROM_ALL ${TARGET}.cc)
    if (WIN32)
        target_link_libraries(tiledb-${TARGET} tiledb_shared)
    else()
        target_link_libraries(tiledb-${TARGET} tiledb_shared -lpthread)
    endif()
    set(CLI_TARGETS ${CLI_TARGETS} tiledb-${TARGET})
endfunction()


include_directories(${PROJECT_SOURCE_DIR}/external/include/)

build_cli(consolidate)
build_cli(info)
build_cli(query)

add_custom_target(tiledb-cli DEPENDS tiledb_shared)
add_dependencies(tiledb-cli ${CLI_TARGETS} tiledb_shared)
