#
# tiledb/sm/misc/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021 TileDB, Inc.
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

include(common NO_POLICY_SCOPE)

#
# `cancelable_tasks` object library
#
add_library(cancelable_tasks OBJECT cancelable_tasks.cc)
target_link_libraries(cancelable_tasks PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(cancelable_tasks PUBLIC thread_pool $<TARGET_OBJECTS:thread_pool>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_cancelable_tasks EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_cancelable_tasks)
target_link_libraries(compile_cancelable_tasks PRIVATE cancelable_tasks)
target_sources(compile_cancelable_tasks PRIVATE test/compile_cancelable_tasks_main.cc)

#
# `constants` object library
#
add_library(constants OBJECT constants.cc)
target_link_libraries(constants PUBLIC baseline $<TARGET_OBJECTS:baseline>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_constants EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_constants)
target_link_libraries(compile_constants PRIVATE constants)
target_sources(compile_constants PRIVATE test/compile_constants_main.cc)

#
# `math` object library
#
add_library(math OBJECT tdb_math.cc)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_math EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_math)
target_link_libraries(compile_math PRIVATE math)
target_sources(compile_math PRIVATE test/compile_math_main.cc)

#
# `misc_types` object library
#
add_library(misc_types OBJECT types.cc)
target_link_libraries(misc_types PUBLIC range $<TARGET_OBJECTS:range>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_misc_types EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_misc_types)
target_link_libraries(compile_misc_types PRIVATE misc_types)
target_sources(compile_misc_types PRIVATE test/compile_misc_types_main.cc)

#
# `parse_arguments` object library
#
add_library(parse_argument OBJECT parse_argument.cc)
target_link_libraries(parse_argument PUBLIC baseline $<TARGET_OBJECTS:baseline>)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_parse_argument EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_parse_argument)
target_link_libraries(compile_parse_argument PRIVATE parse_argument)
target_sources(compile_parse_argument PRIVATE test/compile_parse_argument_main.cc)

#
# `time` object library
#
add_library(time OBJECT tdb_time.cc)
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_time EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_time)
target_link_libraries(compile_time PRIVATE time)
target_sources(compile_time PRIVATE test/compile_time_main.cc)

#
# `uuid` object library
#
add_library(uuid OBJECT uuid.cc)
target_link_libraries(uuid PUBLIC baseline $<TARGET_OBJECTS:baseline>)
if(WIN32)
    if(MSVC)
        find_library(RPCRT4_LIBRARY rpcrt4)
        message(STATUS "Found Win32 lib bcrypt: ${RPCRT4_LIBRARY}")
        target_link_libraries(uuid PUBLIC ${RPCRT4_LIBRARY})
    else()
        message(STATUS "Linking to Win32 lib rpcrt4")
        target_link_libraries(uuid PUBLIC -lrpcrt4)
    endif()
else()
    find_package(OpenSSL_EP REQUIRED)
    target_link_libraries(uuid PUBLIC OpenSSL::Crypto)
endif()
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_uuid EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_uuid)
target_link_libraries(compile_uuid PRIVATE uuid)
target_sources(compile_uuid PRIVATE test/compile_uuid_main.cc)

if (TILEDB_TESTS)
  # simple unit test of magic.mgc embedded data vs external data
  find_package(Magic_EP REQUIRED)
  if(NOT EXISTS ${libmagic_DICTIONARY})
    message(FATAL_ERROR "Failed to find libmagic 'magic.mgc' file.")
  endif()
  add_executable(unit_mgc_dict EXCLUDE_FROM_ALL)
  target_sources(unit_mgc_dict PRIVATE
          test/unit_mgc_dict.cc
          mgc_dict.cc
  )
  target_link_libraries(unit_mgc_dict PRIVATE
    compressors
    filter
    libmagic
    )

  target_compile_options(unit_mgc_dict PRIVATE -DTILEDB_PATH_TO_MAGIC_MGC=\"${libmagic_DICTIONARY}\")
  target_include_directories(unit_mgc_dict
    PRIVATE
      ${CMAKE_CURRENT_BINARY_DIR}
     "${libmagic_INCLUDE_DIR}"
  )

  add_executable(unit_misc EXCLUDE_FROM_ALL)
  target_link_libraries(unit_misc PRIVATE math)
  find_package(Catch_EP REQUIRED)
  target_link_libraries(unit_misc PUBLIC Catch2::Catch2WithMain)
  target_include_directories(unit_misc
    PRIVATE
    "${CMAKE_SOURCE_DIR}"
  )

  # Sources for tests
  target_sources(unit_misc PUBLIC test/main.cc test/unit_math.cc test/unit_hilbert.cc)

  add_test(
      NAME "unit_misc"
      COMMAND $<TARGET_FILE:unit_misc>
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )
endif()
