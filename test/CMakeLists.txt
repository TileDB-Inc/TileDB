#
# test/CMakeLists.txt
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

# Include TileDB core header directories
file(GLOB TILEDB_CORE_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/core/include/*")

# Include libS3 headers if enabled
if(USE_S3)
  set(TILEDB_CORE_INCLUDE_DIRS ${TILEDB_CORE_INCLUDE_DIRS} "${CMAKE_SOURCE_DIR}/external/libs3/inc")
endif()

# catch unit-test library
add_library(
    catch_main EXCLUDE_FROM_ALL
    OBJECT "src/unit.cc"
)

set_target_properties(catch_main PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
)

target_include_directories(
    catch_main PRIVATE 
    ${CMAKE_SOURCE_DIR}/external/catch
)

# Gather the test source files
file(GLOB TILEDB_TEST_SOURCES "src/unit-*.cc")
if(USE_HDFS)
  file(GLOB TILEDB_HDFS_TEST_SOURCES "src/hdfs-unit-*.cc")
  set(TILEDB_TEST_SOURCES ${TILEDB_SET_SOURCES} ${TILEDB_TEST_HDFS_SOURCES})
endif()
if(USE_S3)
  file(GLOB TILEDB_S3_TEST_SOURCES "src/s3-unit-*.cc")
  set(TILEDB_TEST_SOURCES ${TILEDB_TEST_SOURCES} ${TILEDB_S3_TEST_SOURCES})
endif()

# unit test executable
set(TILEDB_UNITTEST_TARGET_NAME "tiledb_unit")
add_executable(
    ${TILEDB_UNITTEST_TARGET_NAME} EXCLUDE_FROM_ALL
    $<TARGET_OBJECTS:catch_main>
    ${TILEDB_TEST_SOURCES})

set_target_properties(
    ${TILEDB_UNITTEST_TARGET_NAME} PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
)

target_link_libraries(
    ${TILEDB_UNITTEST_TARGET_NAME}
    tiledb_static
    ${TILEDB_LIB_DEPENDENCIES}
)

if(USE_S3)
  target_link_libraries(
    ${TILEDB_UNITTEST_TARGET_NAME} libs3)
endif()

target_include_directories(
    ${TILEDB_UNITTEST_TARGET_NAME} PRIVATE
    ${TILEDB_CORE_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/external/catch
)

add_test(
    NAME ${TILEDB_UNITTEST_TARGET_NAME}
    COMMAND ${TILEDB_UNITTEST_TARGET_NAME} --durations=yes
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/test)

# Add custom target 'check'
add_custom_target(
    check COMMAND ${CMAKE_CTEST_COMMAND} -V 
    DEPENDS ${TILEDB_UNITTEST_TARGET_NAME}
)
