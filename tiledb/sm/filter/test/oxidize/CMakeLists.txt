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

add_test(
  oxidized_filter_pipeline
  COMMAND
    ${CARGO_ENV}
    "TILEDB_ROOT=${CMAKE_SOURCE_DIR}"
    "CXX_ROOT=${CMAKE_CURRENT_BINARY_DIR}/cxxbridge"
    "SEARCH_PATH=${CMAKE_CURRENT_BINARY_DIR}"
    ${CARGO}
    "test"
    ${CARGO_PROFILE_FLAG}
    "--no-run"
    "-pwrap_cpp"
    ${CARGO}
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
)

set_tests_properties(${FILTER_PIPELINE_LIB} PROPERTIES FIXTURES_SETUP filter_pipeline)
set_tests_properties(test PROPERTIES  FIXTURES_REQUIRED filter_pipeline)

add_custom_target(
    xkcd_boop
    DEPENDS a_test_binary
)
