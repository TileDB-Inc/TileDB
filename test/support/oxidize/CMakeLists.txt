set(TILEDB_CRATES
    arithmetic
)

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

foreach(crate IN LISTS TILEDB_CRATES)
    add_custom_command(
        OUTPUT
            "${CMAKE_CURRENT_BINARY_DIR}/${crate}.h"
            "${CARGO_INCLUDE_DIR}/cxxbridge/${crate}/src/lib.rs.cc"
            "${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${crate}${CMAKE_STATIC_LIBRARY_SUFFIX}"
        COMMAND
            "MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}"
            "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target"
            ${CARGO}
            build
            "-q"
            "--profile" ${CARGO_PROFILE}
            "--manifest-path" ${CARGO_MANIFEST_PATH}
            "-p" ${crate}
        COMMAND
            cp "${CARGO_INCLUDE_DIR}/cxxbridge/${crate}/src/lib.rs.h" "${CMAKE_CURRENT_BINARY_DIR}/${crate}.h"
        USES_TERMINAL
        DEPENDS
            ${CMAKE_CURRENT_SOURCE_DIR}/${crate}/src/lib.rs
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )
endforeach()

# Note: Object libraries are "{crate_name}_rs" to avoid clashing with the
# existing object libraries.
foreach(crate IN LISTS TILEDB_CRATES)
    add_library(${crate}_rs OBJECT "${CARGO_INCLUDE_DIR}/cxxbridge/${crate}/src/lib.rs.cc")
    target_include_directories(
        ${crate}_rs
        PUBLIC
            ${CARGO_INCLUDE_DIR}
            ${CMAKE_SOURCE_DIR}
            ${CMAKE_BINARY_DIR}/tiledb/
    )
    target_link_libraries(${crate}_rs ${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${crate}${CMAKE_STATIC_LIBRARY_SUFFIX})
endforeach()

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${CARGO_INCLUDE_DIR} *.h)
