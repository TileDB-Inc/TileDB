#
# cmake/object_library.cmake
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
# Environment `object_library`
#
# -------------------------------------------------------
#
# Properties:
# - target: The name of a CMake object library target
# - sources: A list of sources for the target.
#   - Must not be empty
# - object_library_dependencies: Other object libraries the target depends upon
# - link_library_dependencies: Ordinary libraries the target depends upon
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
set(TileDB_Environment_object_library_Log_Level VERBOSE)
message(${TileDB_Environment_object_library_Log_Level} ">>> object_library.cmake begin >>>")

#
# The environment object_library
#
commence(define_environment "object_library")
    set_this_property(commence_hook TileDB_Environment_object_library_begin)
    set_this_property(conclude_hook TileDB_Environment_object_library_end)
conclude(define_environment)


# -------------------------------------------------------
# Commence
# -------------------------------------------------------
#
# The body of `commence_hook` for object_library checks for errors.
#
# Parameters:
# - [out] Result
# - [in] Library_Name
#
function(TileDB_Environment_object_library_begin_body Result Library_Name)
    #
    # The library name must be valid for use in environments
    #
    validate_environment_name(${Library_Name} Valid)
    if (NOT ${Valid})
        set(${Result} false PARENT_SCOPE)
        return()
    endif()
    #
    # The library name must not already be a target
    #
    if (TARGET ${Library_Name})
        message(SEND_ERROR "`${Library_Name}` is already a target and cannot be redefined as an object library")
        set(${Result} false PARENT_SCOPE)
        return()
    endif()
    # Otherwise we're OK
    set(${Result} true PARENT_SCOPE)
endfunction()

#
# The commence_hook macro for object_library
#
# Parameters:
# - [out] Result
# - [in] Library_Name
#
macro(TileDB_Environment_object_library_begin Result Library_Name)
    set(TileDB_Environment_object_library_begin_Log_Level VERBOSE)
    message(${TileDB_Environment_object_library_begin_Log_Level} ">>> object-library commence begin >>>")
    message(${TileDB_Environment_object_library_begin_Log_Level} "  Library_Name=${Library_Name}")

    # Reuse the out-param for calling the body function
    TileDB_Environment_object_library_begin_body(Result ${Library_Name} ${ARGN})
    if (${Result})
        set_this_property(library ${Library_Name})
        set(${Result} true)
    else()
        set(${Result} false)
    endif()

    # ----------------------------------
    # code generation
    # ----------------------------------
    # Target-based commands must work within the environment.
    add_library(${Library_Name} OBJECT)
    # ----------------------------------

    message(${TileDB_Environment_object_library_begin_Log_Level} "<<< object-library commence end <<<")
endmacro()

# -------------------------------------------------------
# Conclude
# -------------------------------------------------------

# Parameter
# - [out] Result

function(TileDB_Environment_object_library_end_body Result)
    get_this_property(sources Sources)
    list(LENGTH Sources N)
    if (N LESS 1)
        get_this_property(library Target)
        message(SEND_ERROR "Object library \"${Target}\" has no sources declared")
        set(${Result} false)
        return()
    endif()
    set(${Result} true)
endfunction()

#
# conclude_hook for object_library
#
# Parameters: none
#
macro(TileDB_Environment_object_library_end)
    set(TileDB_Environment_object_library_end_Log_Level VERBOSE)
    message(${TileDB_Environment_object_library_end_Log_Level} ">>> object-library conclude begin >>>")

    TileDB_Environment_object_library_end_body(TileDB_Environment_object_library_end_Result)

    get_this_property(library TileDB_Environment_object_library_end_Library)
    get_this_property(sources TileDB_Environment_object_library_end_Sources)
    get_this_property(object_library_dependencies TileDB_Environment_object_library_end_OL_Dependencies)
    get_this_property(link_library_dependencies TileDB_Environment_object_library_end_Link_Dependencies)
    get_this_property(compile_definitions TileDB_Environment_object_library_end_Compile_Definitions)

    message(${TileDB_Environment_object_library_end_Log_Level} "  library=${TileDB_Environment_object_library_end_Library}")
    message(${TileDB_Environment_object_library_end_Log_Level} "  sources=${TileDB_Environment_object_library_end_Sources}")
    message(${TileDB_Environment_object_library_end_Log_Level} "  depends on object libraries=${TileDB_Environment_object_library_end_OL_Dependencies}")
    message(${TileDB_Environment_object_library_end_Log_Level} "  depends on link libraries=${TileDB_Environment_object_library_end_Link_Dependencies}")
    message(${TileDB_Environment_object_library_end_Log_Level} "  has compile definitions=${TileDB_Environment_object_library_end_Compile_Definitions}")

    set(TileDB_Environment_object_library_end_Compile "compile_${TileDB_Environment_object_library_end_Library}")
    # ----------------------------------
    # code generation
    # ----------------------------------
    # Sources and object library dependencies, compile definitions
    target_sources(${TileDB_Environment_object_library_end_Library} PRIVATE ${TileDB_Environment_object_library_end_Sources})
    foreach(Object_Library IN LISTS TileDB_Environment_object_library_end_OL_Dependencies)
        target_link_libraries(${TileDB_Environment_object_library_end_Library} PUBLIC ${Object_Library} $<TARGET_OBJECTS:${Object_Library}>)
    endforeach()
    foreach(Link_Library IN LISTS TileDB_Environment_object_library_end_Link_Dependencies)
        target_link_libraries(${TileDB_Environment_object_library_end_Library} PUBLIC ${Link_Library})
    endforeach()
    foreach(Compile_Definition IN LISTS TileDB_Environment_object_library_end_Compile_Definitions)
        target_compile_definitions(${TileDB_Environment_object_library_end_Library} PUBLIC ${Compile_Definition})
    endforeach()
    #
    # All object libraries ought to depend upon the `configuration_definitions`
    # library, but at present it's premature for that
    #
    #target_link_libraries(${TileDB_Environment_object_library_end_Library} PUBLIC configuration_definitions)

    # Compile test
    add_executable(${TileDB_Environment_object_library_end_Compile} EXCLUDE_FROM_ALL)
    target_link_libraries(${TileDB_Environment_object_library_end_Compile} PRIVATE ${TileDB_Environment_object_library_end_Library})
    # Link assert since it is used everywhere (good)
    target_link_libraries(${TileDB_Environment_object_library_end_Compile} PRIVATE assert)
    # There must be a file named `test/compile_<object_library>_main.cc` for each object library.
    # TODO: Add explicit check for missing `..._main.cc` file.
    target_sources(${TileDB_Environment_object_library_end_Compile} PRIVATE
        test/${TileDB_Environment_object_library_end_Compile}_main.cc $<TARGET_OBJECTS:${TileDB_Environment_object_library_end_Library}>
        )
    # Append the current link-complete compile target to the complete list of them
    put_into(ACCUMULATOR object_library_compile_targets LIST ${TileDB_Environment_object_library_end_Compile})
    # ----------------------------------

    message(${TileDB_Environment_object_library_end_Log_Level} "<<< object-library conclude end <<<")
endmacro()

verify_outside_of_environment()
message(${TileDB_Environment_object_library_Log_Level} "<<< object_library.cmake end <<<")

