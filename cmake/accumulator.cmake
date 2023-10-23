#
# cmake/accumulator.cmake
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
############################################################
#
# An accumulator is a list that's appended to incrementally. Accumulators are
# stored as a property in directory scope. Entries can be put into the
# accumulator from within the directory itself or from within any of its
# subdirectories.
#
# CMake has an assymetry in directory properties. An inherited property may
# be read from a subdirectory without specifying the directory, but writing
# without so specifying will write to the scope of the current directory. Thus
# we record the directory where the value is defined in order to write to it.
#
############################################################

include_guard()

#
# define_accumulator( <accumulator> )
#
# define_accumulator() creates an accumulator in the scope of the directory
# in which it's called.
#
function(define_accumulator ACC_NAME)
    define_property(DIRECTORY PROPERTY
        ACCUMULATOR_VALUE_${ACC_NAME} INHERITED
        BRIEF_DOCS "The value of an accumulator."
        FULL_DOCS "See BRIEF_DOCS"
    )
    define_property(
        DIRECTORY PROPERTY ACCUMULATOR_DIR_${ACC_NAME} INHERITED
        BRIEF_DOCS "The directory in whose scope an accumulator is defined."
        FULL_DOCS "See BRIEF_DOCS"
    )
    set_directory_properties(PROPERTIES "ACCUMULATOR_DIR_${ACC_NAME}" ${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

#
# put_into( ACCUMULATOR <accumulator> LIST <elements> )
#
function(put_into)
    cmake_parse_arguments(ARGP "" "ACCUMULATOR" "LIST" ${ARGN})
    get_directory_property(ACC_DIR "ACCUMULATOR_DIR_${ARGP_ACCUMULATOR}")
    set(ACCUMULATOR_PROPERTY_NAME "ACCUMULATOR_VALUE_${ARGP_ACCUMULATOR}")
    foreach(ELEMENT IN LISTS ARGP_LIST)
        # Accumulate list elements on the property in the directory where
        # the accumulator is declared. If we were to set the property without
        # the directory declaration, it would set it on a property in the
        # current directory.
        set_property(DIRECTORY ${ACC_DIR} APPEND PROPERTY ${ACCUMULATOR_PROPERTY_NAME} ${ELEMENT})
    endforeach()
endfunction()

#
# retrieve_from( <variable> ACCUMULATOR <accumulator> )
#
function(retrieve_from)
    cmake_parse_arguments(PARSE_ARGV 1 ARGP "" "ACCUMULATOR" "")
    get_directory_property(ACC_VALUE "ACCUMULATOR_VALUE_${ARGP_ACCUMULATOR}")
    set(${ARGV0} ${ACC_VALUE} PARENT_SCOPE)
endfunction()

#
# reset_accumulator( ACCUMULATOR <accumulator> )
#
function(reset_accumulator)
    cmake_parse_arguments(PARSE_ARGV 1 ARGP "" "ACCUMULATOR" "")
    set_directory_properties(PROPERTIES "ACCUMULATOR_VALUE_${ARGP_ACCUMULATOR}" "")
endfunction()