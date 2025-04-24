# Step 1: Setup our Cargo environment

set(CARGO_ENV "MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}" "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}")

set(CARGO "cargo") # TODO: Add standard program search support here

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_PROFILE_ARG "")
else ()
    set(CARGO_PROFILE_ARG "--release")
endif ()

set(CARGO_MANIFEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml")

set(CXX_INCLUDES "${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/rust/")
set(WRAP_RUST_INCLUDES "${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/wrap_rust/src/")
set(WRAP_RUST_SOURCE "${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/wrap_rust/src/lib.rs.cc")
set(WRAP_RUST_LIB "${CMAKE_CURRENT_BINARY_DIR}/debug/libwrap_rust.a")

# Step 2: Use cxx to generate the C++ code necessary to created
# a shared library that Rust can link to.

add_custom_command(
  OUTPUT ${WRAP_RUST_SOURCE} ${WRAP_RUST_LIB}
  COMMAND
    ${CARGO_ENV}
    ${CARGO}
    build
    ${CARGO_PROFILE_FLAG}
    "--manifest-path" ${CARGO_MANIFEST_PATH}
    "-pwrap_rust"
  USES_TERMINAL
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# Step 3: Create a shared library containing the parts of core that we want
# Rust to have access to.

set(FILTER_PIPELINE_LIB "filter_pipeline_objlib")
add_library(
    ${FILTER_PIPELINE_LIB}
    SHARED
    EXCLUDE_FROM_ALL
    ../filter_test_support.cc
    ../filtered_tile_checker.cc
    ../oxidize.cc
    ${WRAP_RUST_SOURCE}
)
target_link_libraries(
    ${FILTER_PIPELINE_LIB}
    PUBLIC
        filter_pipeline
        tiledb_test_support_lib
        ${WRAP_RUST_LIB}
)
target_include_directories(${FILTER_PIPELINE_LIB} PRIVATE ${CXX_INCLUDES} ${WRAP_RUST_INCLUDES})
target_include_directories(${FILTER_PIPELINE_LIB}
  PRIVATE
    $<TARGET_PROPERTY:Catch2::Catch2,INTERFACE_INCLUDE_DIRECTORIES>
)

# Step 4: Build a test binary that links to the shared library we just created

add_custom_command(
  OUTPUT a_test_binary
  COMMAND
    ${CARGO_ENV}
    "SEARCH_PATH=${CMAKE_CURRENT_BINARY_DIR}"
    ${CARGO}
    "test"
    ${CARGO_PROFILE_FLAG}
    "--no-run"
    "--manifest-path" ${CARGO_MANIFEST_PATH}
    "-pwrap_cpp"
    ${CARGO}
  USES_TERMINAL
  DEPENDS ${FILTER_PIPELINE_LIB}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target(
    xkcd_boop
    DEPENDS a_test_binary
)

# set(CRATE_H "${CMAKE_CURRENT_BINARY_DIR}/${CRATE}.h")
# set(CRATE_CXX "${CMAKE_CURRENT_BINARY_DIR}/${CRATE}.cc")
# set(CRATE_LIB "${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/lib${CRATE}.a")

# set(CARGO_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR})
# set(CARGO_COMMAND_ENV "MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}" "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}")

# add_custom_command(
#   OUTPUT ${CRATE_H} ${CRATE_CXX} ${CRATE_LIB}
#   COMMAND ${CARGO_COMMAND_ENV} cargo build ${CARGO_BUILD_FLAGS} -p ${CRATE}
#   COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/${CRATE}/src/lib.rs.h  ${CRATE_H}
#   COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/${CRATE}/src/lib.rs.cc ${CRATE_CXX}
#   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
# )

# aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/${CRATE}/src CRATE_SOURCE_FILES)

# add_custom_target(${CRATE}_generated
#     DEPENDS
#     ${CRATE_H}
#     ${CRATE_CXX}
#     ${CRATE_LIB}
#     SOURCES ${CRATE_SOURCE_FILES}
# )

# add_library(${CRATE} STATIC ${CRATE_CXX})
# add_dependencies(${CRATE} ${CRATE}_generated)
# target_link_libraries(${CRATE} pthread dl ${CRATE_LIB})

# include_directories(${CMAKE_CURRENT_BINARY_DIR}/include)

# target_include_directories(${CRATE} PRIVATE ${CMAKE_BINARY_DIR})



# # Get all propreties that cmake supports
# if(NOT CMAKE_PROPERTY_LIST)
#     execute_process(COMMAND cmake --help-property-list OUTPUT_VARIABLE CMAKE_PROPERTY_LIST)

#     # Convert command output into a CMake list
#     string(REGEX REPLACE ";" "\\\\;" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
#     string(REGEX REPLACE "\n" ";" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
#     list(REMOVE_DUPLICATES CMAKE_PROPERTY_LIST)
# endif()

# function(print_properties)
#     message("CMAKE_PROPERTY_LIST = ${CMAKE_PROPERTY_LIST}")
# endfunction()

# function(print_target_properties target)
#     if(NOT TARGET ${target})
#         message(STATUS "There is no target named '${target}'")
#         return()
#     endif()

#     foreach(property ${CMAKE_PROPERTY_LIST})
#         string(REPLACE "<CONFIG>" "${CMAKE_BUILD_TYPE}" property ${property})

#         # Fix https://stackoverflow.com/questions/32197663/how-can-i-remove-the-the-location-property-may-not-be-read-from-target-error-i
#         if(property STREQUAL "LOCATION" OR property MATCHES "^LOCATION_" OR property MATCHES "_LOCATION$")
#             continue()
#         endif()

#         get_property(was_set TARGET ${target} PROPERTY ${property} SET)
#         if(was_set)
#             get_target_property(value ${target} ${property})
#             message("${target} ${property} = ${value}")
#         endif()
#     endforeach()
# endfunction()

# print_target_properties(filter_pipeline)


# ##############
# # cargo test #
# ##############
# # `cargo test` produces an executable binary which executes all of the `#[test]`
# # functions contained in the Rust crate. This has to be linked with all of the
# # requisite tiledb object files, as well as the bridge archive.
# set(CRATE_TESTS ${CRATE}_sys)

# # First build an archive that contains all of the tiledb objects
# # needed to link unit test
# # NB: this has to be a shared object so that it resolves all object libraries,
# # thanks cmake
# add_library(${CRATE}_sys SHARED EXCLUDE_FROM_ALL
#   ${CRATE_CXX}
#   ../oxidize.cc)
# target_link_libraries(${CRATE}_sys
#   PUBLIC
#   filter_pipeline)
# target_include_directories(${CRATE}_sys PRIVATE ${CMAKE_BINARY_DIR})
# target_include_directories(${CRATE}_sys
#   PRIVATE
#     $<TARGET_PROPERTY:Catch2::Catch2,INTERFACE_INCLUDE_DIRECTORIES>
# )


# # print_target_properties(${CRATE}_sys)


# # Then add a build command for the `cargo test` binary
# # The emitted binary has a hash appended and there is currently no means
# # to not do that, so part of this command canonicalizes the binary name.
# add_custom_command(
#   OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/deps/${CRATE_TESTS}_tests
#   # 0) sanity
#   COMMAND echo "HELLO WORLD 0"
#   COMMAND echo "$<TARGET_OBJECTS:filter_pipeline>"
#   COMMAND echo "$<JOIN:$<TARGET_OBJECTS:filter_pipeline>,:>"
#   # 1) clear out any existing binaries
#   COMMAND echo "HELLO WORLD 1"
#   COMMAND find ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/deps -name "${CRATE_TESTS}*" -type f -executable | xargs rm -f
#   # 2) build the tests
#   COMMAND echo "HELLO WORLD 2"
#   COMMAND echo "CRATE_SEARCH_PATH=${CRATE_SEARCH_PATH}" "CRATE_SHARED_OBJECTS=${CRATE_SHARED_OBJECTS}"
#   COMMAND ${CARGO_COMMAND_ENV} cargo build -vvv ${CARGO_BUILD_FLAGS} --tests --lib -p ${CRATE_TESTS}
#   # COMMAND ${CMAKE_COMMAND} -E env "CRATE_SEARCH_PATH=${CRATE_SEARCH_PATH}" "CRATE_SHARED_OBJECTS=${CRATE_SHARED_OBJECTS}" cargo build -vvv ${CARGO_BUILD_FLAGS} --tests --lib -p ${CRATE}
#   # 3) canonicalize the path to the test binary
#   COMMAND ln $(find ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/deps -name "${CRATE_TESTS}*" -type f -executable) ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/deps/${CRATE}_tests
#   COMMAND_EXPAND_LISTS
#   VERBATIM
#   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#   DEPENDS ${CRATE}_sys
# )

# # then define target
# add_custom_target(${CRATE}_tests
#   DEPENDS
#   ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/deps/${CRATE_TESTS}_tests
# )




# # TODO: re-run if you change build.rs

# # Test stuff, doesn't seem to work
# # Probably want to just use `cargo test` to test the Rust code anyway?
# # Would be way better if `cargo test` produced a binary with stable output name
# # Problems:
# # 1) need to specify tiledb object libraries to link with
# # 2) cargo test pretty printing
# # 3) cargo test produces multiple binaries

# # Command to build `cargo test --lib` binary.
# # The binary produced has a hash appended so this command must canonicalize its name.
# #add_custom_command(
# #  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/deps/${CRATE}_tests
# #  # clear out any existing binaries
# #  COMMAND echo "HELLO WORLD 1"
# #  COMMAND find ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/deps -name "${CRATE}*" -type f -executable | xargs rm -f
# #  # build the tests
# #  COMMAND echo "HELLO WORLD 2"
# #  COMMAND echo "TARGET OBJECTS" "$<JOIN:$<TARGET_OBJECTS:filter_pipeline>,:>"
# #  # COMMAND echo "LINK LIBRARIES" "$<JOIN:$<LINK_LIBRARIES:filter_pipeline>,:>"
# #  COMMAND echo "HELLO WORLD 3"
# #  COMMAND "CARGO_FFI_LINK_OBJECTS=${CRATE_LIB}:$<JOIN:$<TARGET_OBJECTS:filter_pipeline>,:>" ${CARGO_BUILD_TESTS}
# #  # COMMAND "MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}" "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}" ${CARGO_BUILD_TESTS}
# #  # standardize the name
# #  COMMAND echo "HELLO WORLD 4"
# #  COMMAND ln $(find ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/deps -name "${CRATE}*" -type f -executable) ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/deps/${CRATE}_tests
# #  COMMAND_EXPAND_LISTS
# #  VERBATIM
# #  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
# #)
# #
# #add_custom_target(${CRATE}_tests_old
# #  DEPENDS
# #  ${CRATE_LIB}
# #  ${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/deps/${CRATE}_tests
# #)

# add_test(
#     NAME ${CRATE}_tests
#     COMMAND cargo test ${CARGO_BUILD_FLAGS} --lib -p ${CRATE}
#     WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
# )
# )
# )
