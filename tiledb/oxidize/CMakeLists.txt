find_program(CARGO cargo)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_PROFILE "dev")
    set(CARGO_TARGET "debug")
else ()
    set(CARGO_PROFILE "release")
    set(CARGO_TARGET "release")
endif ()

set(CARGO_MANIFEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml")

set(CARGO_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/target")
set(CARGO_LIB_DIR "${CMAKE_CURRENT_BINARY_DIR}/target/${CARGO_TARGET}")

set(OXIDIZE_SOURCES "${CARGO_MANIFEST_PATH}")

list(APPEND OXIDIZE_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/arithmetic/build.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/arithmetic/Cargo.toml"
    "${CMAKE_CURRENT_SOURCE_DIR}/arithmetic/src/lib.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/bundle_rs/Cargo.toml"
    "${CMAKE_CURRENT_SOURCE_DIR}/bundle_rs/src/lib.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/datatype/Cargo.toml"
    "${CMAKE_CURRENT_SOURCE_DIR}/datatype/src/lib.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/expr/build.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/expr/Cargo.toml"
    "${CMAKE_CURRENT_SOURCE_DIR}/expr/src/lib.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/expr/src/logical_expr.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/expr/src/offsets.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/expr/src/physical_expr.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/expr/src/record_batch.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/expr/src/schema.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/Cargo.toml"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/src/lib.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/src/sm/array_schema/mod.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/src/sm/enums/mod.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/src/sm/misc/mod.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/src/sm/mod.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/src/sm/query/ast/mod.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/src/sm/query/mod.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/src/sm/query/readers/mod.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/src/sm/tile/mod.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/oxidize/build.rs"
)

add_custom_command(
    OUTPUT
        "${CMAKE_CURRENT_BINARY_DIR}/target/build_rs.cc"
        "${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}bundle_rs${CMAKE_STATIC_LIBRARY_SUFFIX}"
    COMMAND
        ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target"
        ${CARGO}
        build
        "-q"
        "--profile" ${CARGO_PROFILE}
        "--manifest-path" ${CARGO_MANIFEST_PATH}
        "-p"
        "bundle_rs"
    USES_TERMINAL
    DEPENDS
        ${OXIDIZE_SOURCES}
    WORKING_DIRECTORY
        ${CMAKE_CURRENT_BINARY_DIR}
)

add_library(oxidize OBJECT ${CMAKE_CURRENT_BINARY_DIR}/target/build_rs.cc)

target_include_directories(
    oxidize
    PUBLIC
        ${CMAKE_SOURCE_DIR}
        ${CARGO_INCLUDE_DIR}
)

target_link_libraries(
    oxidize
    PUBLIC
        ${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}bundle_rs${CMAKE_STATIC_LIBRARY_SUFFIX}
)
