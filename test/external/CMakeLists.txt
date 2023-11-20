#
# test/external/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2022 TileDB, Inc.
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

if(NOT TILEDB_CRC32)
  message("skipping target unit_link_crc32, found NOT TILEDB_CRC32")
else()
  message("adding target unit_link_crc32, have TILEDB_CRC32")

  add_executable(unit_link_crc32 EXCLUDE_FROM_ALL src/crc32_link_test.cc)

  find_package(Crc32c CONFIG REQUIRED
    PATHS
        ${TILEDB_EP_INSTALL_PREFIX}/lib/cmake
    ${TILEDB_DEPS_NO_DEFAULT_PATH}
    )

  target_link_libraries(unit_link_crc32 PRIVATE Crc32c::crc32c)

  target_include_directories(unit_link_crc32 PRIVATE ${TILEDB_EP_INSTALL_PREFIX} ${TILEDB_EP_INSTALL_PREFIX}/include )
endif()
