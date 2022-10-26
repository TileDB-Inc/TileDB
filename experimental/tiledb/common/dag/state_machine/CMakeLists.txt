#
# experimental/tiledb/common/state_machine/CMakeLists.txt
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

include(common NO_POLICY_SCOPE)

message(STATUS "+++++++ in state_machine")

list(APPEND FSM_SOURCES
    fsm.cc
)
gather_sources(${FSM_SOURCES})


#
# Object library for other units to depend upon
#
add_library(fsm OBJECT ${FSM_SOURCES})
target_link_libraries(fsm PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_fsm EXCLUDE_FROM_ALL)
target_link_libraries(compile_fsm PRIVATE fsm)
target_sources(compile_fsm PRIVATE test/compile_fsm_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_fsm EXCLUDE_FROM_ALL)
    target_link_libraries(unit_fsm PUBLIC fsm)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_fsm PUBLIC Catch2::Catch2)
    target_link_libraries(unit_fsm PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_fsm PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_fsm PUBLIC
            test/main.cc
            test/unit_fsm.cc
            )

    add_test(
            NAME "unit_fsm"
            COMMAND $<TARGET_FILE:unit_fsm> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_dependencies(check unit_fsm)
message(STATUS "+++++++ done unit_fsm")


endif()

#
# Object library for other units to depend upon
#
#add_library(policies OBJECT ${POLICIES_SOURCES})
#target_link_libraries(policies PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_policies EXCLUDE_FROM_ALL)
#target_link_libraries(compile_policies PRIVATE policies)
target_sources(compile_policies PRIVATE test/compile_policies_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_policies EXCLUDE_FROM_ALL)
#    target_link_libraries(unit_policies PUBLIC policies)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_policies PUBLIC Catch2::Catch2)
#    target_link_libraries(unit_policies PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_policies PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_policies PUBLIC
            test/main.cc
            test/unit_policies.cc
            )

    add_test(
            NAME "unit_policies"
            COMMAND $<TARGET_FILE:unit_policies> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_dependencies(check unit_policies)
message(STATUS "+++++++ done unit_policies")


endif()


#
# Object library for other units to depend upon
#
#add_library(item_mover OBJECT ${ITEM_MOVER_SOURCES})
#target_link_libraries(item_mover PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_item_mover EXCLUDE_FROM_ALL)
#target_link_libraries(compile_item_mover PRIVATE item_mover)
target_sources(compile_item_mover PRIVATE test/compile_item_mover_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_item_mover EXCLUDE_FROM_ALL)
#    target_link_libraries(unit_item_mover PUBLIC item_mover)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_item_mover PUBLIC Catch2::Catch2)
#    target_link_libraries(unit_item_mover PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_item_mover PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_item_mover PUBLIC
            test/main.cc
            test/unit_item_mover.cc
            )

    add_test(
            NAME "unit_item_mover"
            COMMAND $<TARGET_FILE:unit_item_mover> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_dependencies(check unit_item_mover)

message(STATUS "+++++++ done unit_item_mover")


endif()



#
# Object library for other units to depend upon
#
#add_library(stop OBJECT ${STOP_SOURCES})
#target_link_libraries(stop PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
#add_executable(compile_stop EXCLUDE_FROM_ALL)
#target_link_libraries(compile_stop PRIVATE stop)
#target_sources(compile_stop PRIVATE test/compile_stop_main.cc)

if (TILEDB_TESTS)
    add_executable(unit_stop EXCLUDE_FROM_ALL)
#    target_link_libraries(unit_stop PUBLIC stop)
    find_package(Catch_EP REQUIRED)
    target_link_libraries(unit_stop PUBLIC Catch2::Catch2)
#    target_link_libraries(unit_stop PUBLIC $<TARGET_OBJECTS:thread_pool>)

    # Sources for code elsewhere required for tests
    target_sources(unit_stop PUBLIC ${DEPENDENT_SOURCES})

    # Sources for tests
    target_sources(unit_stop PUBLIC
            test/main.cc
            test/unit_stop.cc
            )

    add_test(
            NAME "unit_stop"
            COMMAND $<TARGET_FILE:unit_stop> --durations=yes
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_dependencies(check unit_stop)
endif()



