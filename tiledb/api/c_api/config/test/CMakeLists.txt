#
# tiledb/api/c_api/config/test/CMakeLists.txt
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

add_executable(unit_capi_config EXCLUDE_FROM_ALL)
find_package(Catch_EP REQUIRED)
target_link_libraries(unit_capi_config PUBLIC Catch2::Catch2WithMain)

# Sources for code under test
target_link_libraries(unit_capi_config PUBLIC capi_config_stub)

# Sources for tests
target_sources(unit_capi_config PUBLIC
    unit_capi_config.cc
    )

add_test(
    NAME "unit_capi_config"
    COMMAND $<TARGET_FILE:unit_capi_config>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
