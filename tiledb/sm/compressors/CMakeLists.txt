#
# tiledb/sm/compressors/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2021-2022 TileDB, Inc.
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
include(common NO_POLICY_SCOPE)

#
# `compressors` object library
#
add_library(compressors OBJECT bzip_compressor.cc dd_compressor.cc dict_compressor.cc gzip_compressor.cc lz4_compressor.cc rle_compressor.cc zstd_compressor.cc util/gzip_wrappers.cc)
target_link_libraries(compressors PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(compressors PUBLIC buffer $<TARGET_OBJECTS:buffer>)
find_package(Bzip2_EP REQUIRED)
find_package(LZ4_EP REQUIRED)
find_package(Zlib_EP REQUIRED)
find_package(Zstd_EP REQUIRED)
target_link_libraries(compressors PUBLIC Bzip2::Bzip2 LZ4::LZ4 Zlib::Zlib Zstd::Zstd)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_compressors EXCLUDE_FROM_ALL)
target_link_libraries(compile_compressors PRIVATE compressors)
target_sources(compile_compressors PRIVATE
        test/compile_compressors_main.cc
)

#
# tiledb crude gzip archiver initially for embedding magic.mgc
#
add_executable(tdb_gzip_embedded_data)
target_sources(tdb_gzip_embedded_data PRIVATE
        util/tdb_gzip_embedded_data.cc 
)

target_link_libraries(
    tdb_gzip_embedded_data PUBLIC 
    filter
    compressors
    )
    
target_include_directories(tdb_gzip_embedded_data PRIVATE
        $<TARGET_PROPERTY:TILEDB_CORE_OBJECTS,INCLUDE_DIRECTORIES>
)

if (TRUE) #(TILEDB_BUILDING_WEBP)
  #
  # perform webp build and link test for some webp routines
  #
  find_package(Webp_EP REQUIRED)
  add_executable(link_webp_test1)
  if (WIN32)
    target_link_libraries(link_webp_test1 PRIVATE WebP::webp WebP::webpdecoder WebP::webpdemux WebP::webpmux)
  else() #if (NOT WIN32)
    # On Linux, must explicitly link -lpthread -ldl in order for static linking
    # to libzstd or libcurl to work.
    target_link_libraries(link_webp_test1 WebP::webp WebP::webpdecoder WebP::webpdemux WebP::webpmux pthread dl)
  endif()
  target_sources(
      link_webp_test1 
      PRIVATE
      test/link_webp_test1.cc
      )
      
  add_test(NAME "test_tiledb_webp" 
    COMMAND "$<TARGET_FILE:link_webp_test1>" 
    # DEPENDS "$<TARGET_FILE:link_webp_test1>"
    WORKING_DIRECTORY  ${CMAKE_CURRENT_SOURCE_DIR}
  )
endif()

# Link to Threads::Threads if library or flag needed to enable threading.
find_package(Threads REQUIRED)

if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries(tdb_gzip_embedded_data PRIVATE Threads::Threads)
endif()
# On Linux, must explicitly link -ldl for static linking to libzstd.
if (NOT WIN32)
  target_link_libraries(tdb_gzip_embedded_data PRIVATE dl)
endif()

if (TILEDB_TESTS)
    add_executable(unit_compressors EXCLUDE_FROM_ALL)
    target_link_libraries(unit_compressors PRIVATE compressors)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_compressors PUBLIC Catch2::Catch2)

    # Sources for tests
    target_sources(unit_compressors PUBLIC test/main.cc test/unit_dict_compressor.cc)

    add_test(
        NAME "unit_compressors"
        COMMAND $<TARGET_FILE:unit_compressors>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
