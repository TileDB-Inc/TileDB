#
# cmake/unit_test.cmake
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

# -------------------------------------------------------
#
# Environment `unit_test`
#
# -------------------------------------------------------
#
# Properties:
# - target: The distinguishing name of a CMake target for a unit test executable
#     The actual target is named `unit_<target>`.
# - sources: A list of sources for the target.
#
# Common properties from `environment-this-functions.cmake`:
# - object_library_dependencies: Object libraries the target depends upon
# - link_library_dependencies: Ordinary libraries the target depends upon
# - compile_definitions:
#

# -------------------------------------------------------
# Preliminaries
# -------------------------------------------------------
include_guard()
include(environment)
include(environment-this-functions)

#
# Module-scope log level
#
set(TileDB_Environment_unit_test_Log_Level VERBOSE)
message(${TileDB_Environment_unit_test_Log_Level} ">>> unit_test.cmake begin >>>")

#
# The environment unit_test
#
commence(define_environment "unit_test")
    set_this_property(commence_hook TileDB_Environment_unit_test_begin)
    set_this_property(conclude_hook TileDB_Environment_unit_test_end)
conclude(define_environment)


# -------------------------------------------------------
# Commence
# -------------------------------------------------------
#
# The body of `commence_hook` for unit_test checks for errors.
#
# Parameters:
# - [out] Result
# - [in] Unit_Test_Name
#
function(TileDB_Environment_unit_test_begin_body Result Unit_Test_Name)
    #
    # The unit test name must be valid for use in environments
    #
    validate_environment_name(${Unit_Test_Name} Valid)
    if (NOT ${Valid})
        set(${Result} false PARENT_SCOPE)
        return()
    endif()
    #
    # The unit test name must not already be a target
    #
    if (TARGET "unit_${Unit_Test_Name}")
        message(SEND_ERROR "`unit_${Unit_Test_Name}` is already a target and cannot be redefined as a unit test")
        set(${Result} false PARENT_SCOPE)
        return()
    endif()
    # Otherwise we're OK
    set(${Result} true PARENT_SCOPE)
endfunction()

#
# The commence_hook macro for unit_test
#
# Parameters:
# - [out] Result
# - [in] Unit_Test_Name
#
macro(TileDB_Environment_unit_test_begin Result Unit_Test_Name)
    set(TileDB_Environment_unit_test_begin_Log_Level VERBOSE)
    message(${TileDB_Environment_unit_test_begin_Log_Level} ">>> unit_test commence begin >>>")
    message(${TileDB_Environment_unit_test_begin_Log_Level} "  Unit_Test_Name=${Unit_Test_Name}")

    # Reuse the out-param for calling the body function
    TileDB_Environment_unit_test_begin_body(Result ${Unit_Test_Name} ${ARGN})
    if (${Result})
        set_this_property(unit_test "unit_${Unit_Test_Name}")
        set(${Result} true)
    else()
        set(${Result} false)
    endif()

    # ----------------------------------
    # code generation
    # ----------------------------------
    # Target-based commands must work within the environment.
    add_executable("unit_${Unit_Test_Name}" EXCLUDE_FROM_ALL)
    # ----------------------------------

    message(${TileDB_Environment_unit_test_begin_Log_Level} "<<< unit_test commence end <<<")
endmacro()

# -------------------------------------------------------
# Conclude
# -------------------------------------------------------

#
# Parameter
# - [out] Result

function(TileDB_Environment_unit_test_end_body Result)
    # At this time there's no validation that might cause the unit test
    # environment to fail
    set(${Result} true)
endfunction()

#
# conclude_hook for unit_test
#
# Parameters: none
#
macro(TileDB_Environment_unit_test_end)
    set(TileDB_Environment_unit_test_end_Log_Level VERBOSE)
    message(${TileDB_Environment_unit_test_end_Log_Level} ">>> unit_test conclude begin >>>")

    TileDB_Environment_unit_test_end_body(TileDB_Environment_unit_test_end_Result)

    get_this_property(unit_test TileDB_Environment_unit_test_end_Unit_Test)
    get_this_property(sources TileDB_Environment_unit_test_end_Sources)
    get_this_property(object_library_dependencies TileDB_Environment_unit_test_end_OL_Dependencies)
    get_this_property(link_library_dependencies TileDB_Environment_unit_test_end_Link_Dependencies)
    get_this_property(compile_definitions TileDB_Environment_unit_test_end_Compile_Definitions)

    message(${TileDB_Environment_unit_test_end_Log_Level} "  unit test target=${TileDB_Environment_unit_test_end_Unit_Test}")
    message(${TileDB_Environment_unit_test_end_Log_Level} "  sources=${TileDB_Environment_unit_test_end_Sources}")
    message(${TileDB_Environment_unit_test_end_Log_Level} "  depends on object libraries=${TileDB_Environment_unit_test_end_OL_Dependencies}")
    message(${TileDB_Environment_unit_test_end_Log_Level} "  depends on link libraries=${TileDB_Environment_unit_test_end_Link_Dependencies}")
    message(${TileDB_Environment_unit_test_end_Log_Level} "  has compile definitions=${TileDB_Environment_unit_test_end_Compile_Definitions}")
    # ----------------------------------
    # code generation
    # ----------------------------------
    # Sources, library dependencies, compile definitions
    target_sources(${TileDB_Environment_unit_test_end_Unit_Test} PRIVATE ${TileDB_Environment_unit_test_end_Sources})
    # Catch2 is always a dependency of our unit tests
    find_package(Catch2 REQUIRED)
    target_link_libraries(${TileDB_Environment_unit_test_end_Unit_Test} PUBLIC tdb_Catch2WithMain)
    foreach(Object_Library IN LISTS TileDB_Environment_unit_test_end_OL_Dependencies)
        target_link_libraries(${TileDB_Environment_unit_test_end_Unit_Test} PUBLIC ${Object_Library})
    endforeach()
    foreach(Link_Library IN LISTS TileDB_Environment_unit_test_end_Link_Dependencies)
        target_link_libraries(${TileDB_Environment_unit_test_end_Unit_Test} PUBLIC ${Link_Library})
    endforeach()
    foreach(Compile_Definition IN LISTS TileDB_Environment_unit_test_end_Compile_Definitions)
        target_compile_definitions(${TileDB_Environment_unit_test_end_Unit_Test} PUBLIC ${Compile_Definition})
    endforeach()

    # test declaration
    add_test(
        NAME "${TileDB_Environment_unit_test_end_Unit_Test}"
        COMMAND $<TARGET_FILE:${TileDB_Environment_unit_test_end_Unit_Test}>
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )

    # Append the current link-complete compile target to the complete list of them
    put_into(ACCUMULATOR unit_test_targets LIST ${TileDB_Environment_unit_test_end_Unit_Test})
    # ----------------------------------

    message(${TileDB_Environment_unit_test_end_Log_Level} "<<< unit_test conclude end <<<")
endmacro()

verify_outside_of_environment()
message(${TileDB_Environment_unit_test_Log_Level} "<<< unit_test.cmake end <<<")
