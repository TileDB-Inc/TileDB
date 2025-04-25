
set(CARGO cargo) # ToDo: Auto-discover cargo

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_PROFILE "debug")
else ()
    set(CARGO_PROFILE "release")
endif ()

set(CARGO_MANIFEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml")

set(CARGO_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/target/")
set(CARGO_LIB_DIR "${CMAKE_CURRENT_BINARY_DIR}/target/${CARGO_PROFILE}")

add_custom_command(
    OUTPUT
        ${CARGO_INCLUDE_DIR}/cxxbridge/filter_pipeline_test_rs/src/lib.rs.cc
        ${CARGO_LIB_DIR}/libfilter_pipeline_test_rs.a
    COMMAND
        "MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}"
        "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target"
        ${CARGO}
        build
        "--profile" ${CARGO_PROFILE}
        "--manifest-path" ${CARGO_MANIFEST_PATH}
        "--all-targets"
    USES_TERMINAL
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_library(filter_pipeline_test_rs OBJECT)
target_sources(filter_pipeline_test_rs PRIVATE ${CARGO_INCLUDE_DIR}/cxxbridge/filter_pipeline_test_rs/src/lib.rs.cc)
target_include_directories(filter_pipeline_test_rs INTERFACE ${CARGO_INCLUDE_DIR})
target_link_libraries(filter_pipeline_test_rs ${CARGO_LIB_DIR}/libfilter_pipeline_test_rs.a)
