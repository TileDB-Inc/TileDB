#
# tiledb/sm/vfs/CMakeLists.txt
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
#

include(common NO_POLICY_SCOPE)

#
# `vfs` object library
#
add_library(vfs OBJECT vfs.cc mem_filesystem.cc path_win.cc posix.cc win.cc uri.cc)
target_link_libraries(vfs PUBLIC baseline $<TARGET_OBJECTS:baseline>)
target_link_libraries(vfs PUBLIC buffer $<TARGET_OBJECTS:buffer>)
target_link_libraries(vfs PUBLIC cancelable_tasks $<TARGET_OBJECTS:cancelable_tasks>)
target_link_libraries(vfs PUBLIC config $<TARGET_OBJECTS:config>)
target_link_libraries(vfs PUBLIC math $<TARGET_OBJECTS:math>)
target_link_libraries(vfs PUBLIC stats $<TARGET_OBJECTS:stats>)
target_link_libraries(vfs PUBLIC stringx $<TARGET_OBJECTS:stringx>)
target_link_libraries(vfs PUBLIC thread_pool $<TARGET_OBJECTS:thread_pool>)
if(WIN32)
    if(MSVC)
        find_library(SHLWAPI_LIBRARY shlwapi)
        message(STATUS "Found Win32 lib shlwapi: ${SHLWAPI_LIBRARY}")
        target_link_libraries(vfs PUBLIC ${SHLWAPI_LIBRARY})
    else()
        message(STATUS "Linking to Win32 lib shlwapi")
        target_link_libraries(vfs PUBLIC -lshlwapi)
    endif()
endif()
#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_vfs EXCLUDE_FROM_ALL)
add_dependencies(all_link_complete compile_vfs)
target_link_libraries(compile_vfs PRIVATE vfs)
target_sources(compile_vfs PRIVATE test/compile_vfs_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_vfs EXCLUDE_FROM_ALL)
    target_link_libraries(unit_vfs PRIVATE vfs)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_vfs PUBLIC Catch2::Catch2WithMain)
    target_include_directories(unit_vfs PUBLIC ${TILEDB_EP_INSTALL_PREFIX}/include)

    # Sources for tests
    target_sources(unit_vfs PUBLIC test/main.cc test/unit_uri.cc)

    add_test(
        NAME "unit_vfs"
        COMMAND $<TARGET_FILE:unit_vfs>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
