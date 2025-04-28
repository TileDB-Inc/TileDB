if (NOT TILEDB_TESTS)
  message(FATAL_ERROR "Building Rust without tests is not supported.")
endif()

if (NOT TILEDB_RUST)
  add_custom_target(
    rust
    COMMAND
        echo "This build is not configured with Rust support enabled."
  )
  add_custom_target(
    rust-test
    COMMAND
        echo "This build is not configured with Rust support enabled."
  )
  return()
endif()

set(CARGO cargo) # ToDo: Auto-discover cargo

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

set(TILEDB_CRATES
    enumeration
    filter
    filter_pipeline
)

# Keep this list of Rust source files up to date if you want the Rust bindings
# to be auto-generated correctly while writing Rust. You can also use the
# `rust` target to trigger building the bindings as well.
set(RUST_SOURCES
  crates/enumeration/src/lib.rs
  crates/filter/src/lib.rs
  crates/filter_pipeline/src/lib.rs
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
        "-q"
        "--profile" ${CARGO_PROFILE}
        "--manifest-path" ${CARGO_MANIFEST_PATH}
        "--all-targets"
    USES_TERMINAL
    DEPENDS ${RUST_SOURCES}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# Make sure this is kept up to date with the above definition. This can be
# used to rebuild the Rust cxx generated code.
add_custom_target(
    rust
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

# Note: Object libraries are "{crate_name}_rs" to avoid clashing with the
# existing object libraries.
foreach(crate cpp lib IN ZIP_LISTS TILEDB_CRATES RUST_GEN_CPPS RUST_GEN_LIBS)
    add_library(${crate}_rs OBJECT ${cpp})
    target_include_directories(
        ${crate}_rs
        PUBLIC
            ${CARGO_INCLUDE_DIR}
            ${CMAKE_SOURCE_DIR}
            ${CMAKE_BINARY_DIR}/tiledb/
    )
    target_link_libraries(${crate}_rs ${lib})
endforeach()

add_subdirectory(cpp)
