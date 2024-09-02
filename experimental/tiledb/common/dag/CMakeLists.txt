#
# experimental/tiledb/common/dag/CMakeLists.txt
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


#
# Define helper macro for many header only object libraries
#
macro (dag_add_header_only_object_library OBJLIBNAME)

  list(APPEND ${OBJLIBNAME}_SOURCES
      ${OBJLIBNAME}.cc
  )
  gather_sources(${${OBJLIBNAME}_SOURCES})
  #
  # Object library for other units to depend upon
  #
  add_library(${OBJLIBNAME} OBJECT ${${OBJLIBNAME}_SOURCES})
  #
  # Test-compile of object library ensures link-completeness
  #
  add_executable(compile_${OBJLIBNAME} EXCLUDE_FROM_ALL)
  add_dependencies(all_link_complete compile_${OBJLIBNAME})

# message("==== ${OBJLIBNAME}")
# message("====---- compile_${OBJLIBNAME} PRIVATE ${OBJLIBNAME}")

  target_link_libraries(compile_${OBJLIBNAME} PRIVATE ${OBJLIBNAME})
  target_sources(compile_${OBJLIBNAME} PRIVATE test/compile_${OBJLIBNAME}_main.cc)
endmacro (dag_add_header_only_object_library)


#
# Define helper macro for many catch-based header-only tests
#
macro (dag_add_header_only_unit_test TESTNAME)
    #
    # Define unit test
    #
    add_executable(unit_${TESTNAME} EXCLUDE_FROM_ALL)
    #
    # Required libraries
    #
    target_link_libraries(unit_${TESTNAME} PUBLIC Catch2::Catch2WithMain)
    #
    # Define sources for test
    #
    target_sources(unit_${TESTNAME} PUBLIC
        main.cc
        unit_${TESTNAME}.cc
    )
    #
    # Sources for code elsewhere required for tests
    #
    target_sources(unit_${TESTNAME} PUBLIC ${DEPENDENT_SOURCES})
    #
    # Add unit test to global suite of unit tests
    #
    add_test(
        NAME "unit_${TESTNAME}"
        COMMAND $<TARGET_FILE:unit_${TESTNAME}> --durations=yes
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_dependencies(all_unit_tests "unit_${TESTNAME}")
endmacro(dag_add_header_only_unit_test)

if (TILEDB_TESTS)
    #
    # All unit tests depend on Catch
    #
    find_package(Catch2 REQUIRED)
endif()

add_subdirectory(data_block)
add_subdirectory(edge)
add_subdirectory(execution)
add_subdirectory(nodes)
add_subdirectory(ports)
add_subdirectory(state_machine)
add_subdirectory(utility)
add_subdirectory(graph)
