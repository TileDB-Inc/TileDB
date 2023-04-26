#
# CMakeLists.txt
#
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

include(Catch2)

set (SIFT_TEST_DIR ${CMAKE_BINARY_DIR}/src/test)
message(STATUS "Data files for testing will be extracted to ${SIFT_TEST_DIR}")

configure_file(config.h.in config.h)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_library(sift_test INTERFACE)
target_link_libraries(sift_test INTERFACE Catch2::Catch2WithMain TileDB::tiledb_shared flat_blas)

add_executable(unit_sift_array unit_sift_array.cc)
target_link_libraries(unit_sift_array PRIVATE sift_test)

add_executable(unit_sift_db unit_sift_db.cc)
target_link_libraries(unit_sift_db PRIVATE Catch2::Catch2WithMain)

add_executable(unit_defs unit_defs.cc)
target_link_libraries(unit_defs PRIVATE Catch2::Catch2WithMain)

add_executable(unit_gemm unit_gemm.cc)
target_link_libraries(unit_gemm PRIVATE sift_test)


# Create the subdirectory for the extracted files
file(MAKE_DIRECTORY ${SIFT_TEST_DIR}/siftsmall)

# Download and extract the tar file
add_custom_command(
  OUTPUT ${SIFT_TEST_DIR}/siftsmall.tar.gz
  COMMAND wget -O ${SIFT_TEST_DIR}/siftsmall.tar.gz ftp://ftp.irisa.fr/local/texmex/corpus/siftsmall.tar.gz
)

add_custom_command(
  OUTPUT ${SIFT_TEST_DIR}/siftsmall/
  COMMAND ${CMAKE_COMMAND} -E tar xzf ${SIFT_TEST_DIR}/siftsmall.tar.gz # -C ${SIFT_TEST_DIR}
  DEPENDS ${SIFT_TEST_DIR}/siftsmall.tar.gz
)

add_custom_target(download_siftsmall DEPENDS ${SIFT_TEST_DIR}/siftsmall/)

# Add the targets that depend on siftsmall
# add_executable(target1 ...)
# add_executable(target2 ...)

# # Exclude the targets from the default build and set dependencies
# set_target_properties(target1 PROPERTIES 
#   EXCLUDE_FROM_ALL TRUE
#   EXCLUDE_FROM_DEFAULT_BUILD TRUE
# )

# set_target_properties(target2 PROPERTIES 
#   EXCLUDE_FROM_ALL TRUE
#   EXCLUDE_FROM_DEFAULT_BUILD TRUE
# )

add_dependencies(unit_sift_array download_siftsmall)
add_dependencies(unit_sift_db download_siftsmall)

# file(MAKE_DIRECTORY siftsmall)

# add_custom_command(
#     OUTPUT my_subdirectory/myfile
#     COMMAND wget https://example.com/myfile.tar.gz && tar -xzf myfile.tar.gz -C my_subdirectory/
# )

# add_custom_target(
#     extract_myfile
#     DEPENDS my_subdirectory/myfile
# )

# add_dependencies(target1 extract_myfile)
# add_dependencies(target2 extract_myfile)


# set (SIFT_DIR ${CMAKE_SOURCE_DIR}/external/data/siftsmall)

# add_custom_command(
#     TARGET unit_sift_db POST_BUILD
#     COMMAND ${CMAKE_COMMAND} -E copy_directory
#         ${SIFT_DIR}
#         "$<TARGET_PROPERTY:unit_sift_db,BINARY_DIR>/siftsmall"
#     )
# add_custom_target(copy_data_files DEPENDS ${EXECUTABLE})
