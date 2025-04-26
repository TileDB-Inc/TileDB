
set(CARGO cargo) # ToDo: Auto-discover cargo

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_PROFILE "debug")
else ()
    set(CARGO_PROFILE "release")
endif ()

set(CARGO_MANIFEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml")

set(CARGO_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/target")
set(CARGO_LIB_DIR "${CMAKE_CURRENT_BINARY_DIR}/target/${CARGO_PROFILE}")

set(TILEDB_CRATES
    test_filter_pipeline_rs
)

set(RUST_GEN_CPPS)
set(RUST_GEN_LIBS)
foreach(CRATE ${TILEDB_CRATES})
    set(RUST_GEN_CPPS
        ${RUST_GEN_CPPS}
        "${CARGO_INCLUDE_DIR}/cxxbridge/${CRATE}/src/lib.rs.cc")
    set(RUST_GEN_LIBS
        ${RUST_GEN_LIBS}
        "${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${CRATE}${CMAKE_STATIC_LIBRARY_SUFFIX}")
endforeach()

add_custom_command(
    OUTPUT
        ${RUST_GEN_CPPS}
        ${RUST_GEN_LIBS}
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

# Make sure this is kept up to date with the above definition. This can be
# used to rebuild the Rust cxx generated code.
add_custom_target(
    rust-build
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

foreach(crate cpp lib IN ZIP_LISTS TILEDB_CRATES RUST_GEN_CPPS RUST_GEN_LIBS)
    add_library(${crate} OBJECT ${cpp})
    target_include_directories(${crate} PUBLIC ${CARGO_INCLUDE_DIR})
    target_link_libraries(${crate} ${lib})
endforeach()
