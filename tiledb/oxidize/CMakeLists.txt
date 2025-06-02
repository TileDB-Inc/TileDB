find_program(CARGO cargo REQUIRED)

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

set(CARGO_VERBOSITY "-q")


# Declare platform-specific parameters
if (WIN32)
    # A Rust deficiency is that it only links with the release CRT.
    # Fortunately it is possible to work around this by linking the debug runtime library
    # using CFLAGS and CXXFLAGS.
    #
    # https://github.com/rust-lang/rust/issues/39016#issuecomment-853964918
    set(TILEDB_OXIDIZE_CFLAGS "-MDd")

    # It is also necessary for some reason to specify the system libraries we need
    # to link with.  This should probably be obtained more scientfically
    # (see `corrosion`'s use of `cargo rustc` for example) but this does the job.
    #
    # https://github.com/corrosion-rs/corrosion/blob/715c235daef4b8ee67278f12256334ad3dd4c4ae/cmake/FindRust.cmake#L155
    set(TILEDB_OXIDIZE_LINK_LIBS "kernel32.lib;advapi32.lib;ws2_32.lib;ntdll.lib;userenv.lib")
endif ()


# Declares targets for building a Rust crate.

# Inputs:
#   NAME crate_name
#   SOURCES source1 source2 ...
#
# Outputs:
#   ${crate_name}_rs object library target
#   Building this target produces the object library and also the `#[cxx::bridge]` files,
#   which can be included in C++ source files via `#include "tiledb/oxidize/${crate_name}.h".
function(add_crate)
    set(options)
    set(oneValueArgs NAME)
    set(multiValueArgs BRIDGES SOURCES)
    cmake_parse_arguments(ADD_CRATE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    if (NOT ADD_CRATE_NAME)
        message(SEND_ERROR "Function add_crate requires a NAME argument")
    endif()

    if (NOT ADD_CRATE_SOURCES AND NOT ADD_CRATE_BRIDGES)
        message(SEND_ERROR "Function add_crate requires SOURCES argument, BRIDGES argument, or both")
    endif()

    # Set up the `#[cxx::bridge]` file paths.
    # Each crate can have one or more `BRIDGES` which are assumed to live in `src/`.
    # This sets up the following mappings from bridge to header file:
    # - `src/lib.rs` => `tiledb/oxidize/${ADD_CRATE_NAME}.h`
    # - `src/module/mod.rs` => `tiledb/oxidize/module.h`
    # - `src/module.rs` => `tiledb/oxidize/module.h`
    set(ADD_CRATE_BRIDGES_CC_OUT "")
    set(ADD_CRATE_BRIDGES_H_OUT "")
    set(ADD_CRATE_BRIDGES_H_DST "")
    foreach (bridge ${ADD_CRATE_BRIDGES})
        if (${bridge} MATCHES "src/lib.rs")
            set(bridge_out "${ADD_CRATE_NAME}")
        elseif (${bridge} MATCHES "src(\/.*)*\/(.*)\/mod.rs")
            set(bridge_out "${ADD_CRATE_NAME}/${CMAKE_MATCH_1}/${CMAKE_MATCH_2}")
        elseif (${bridge} MATCHES "src(\/.*)*\/(.*).rs")
            set(bridge_out "${ADD_CRATE_NAME}/${CMAKE_MATCH_1}/${CMAKE_MATCH_2}")
        else ()
            message(SEND_ERROR "Unable to set 'bridge_out': unexpected pattern in bridge file path: ${bridge}")
        endif ()
        list(APPEND ADD_CRATE_BRIDGES_CC_OUT "${CARGO_INCLUDE_DIR}/cxxbridge/${ADD_CRATE_NAME}/${bridge}.cc")
        list(APPEND ADD_CRATE_BRIDGES_H_OUT "${CARGO_INCLUDE_DIR}/cxxbridge/${ADD_CRATE_NAME}/${bridge}.h")
        list(APPEND ADD_CRATE_BRIDGES_H_DST "${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/${bridge_out}.h")
    endforeach ()
    list(TRANSFORM ADD_CRATE_BRIDGES PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/${ADD_CRATE_NAME}/)
    list(TRANSFORM ADD_CRATE_SOURCES PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/${ADD_CRATE_NAME}/)

    # Build the crate and produce `#[cxx::bridge]` output.
    add_custom_command(
        OUTPUT
            ${ADD_CRATE_BRIDGES_CC_OUT}
            ${ADD_CRATE_BRIDGES_H_OUT}
            "${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${ADD_CRATE_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
        COMMAND
            ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target" "CFLAGS=${TILEDB_OXIDIZE_CFLAGS}" "CXXFLAGS=${TILEDB_OXIDIZE_CFLAGS}"
            ${CARGO}
            build
            ${CARGO_VERBOSITY}
            "--profile" ${CARGO_PROFILE}
            "--manifest-path" ${CARGO_MANIFEST_PATH}
            "-p" ${ADD_CRATE_NAME}
        USES_TERMINAL
        DEPENDS
            ${ADD_CRATE_BRIDGES}
            ${ADD_CRATE_SOURCES}
        WORKING_DIRECTORY
            ${CMAKE_CURRENT_BINARY_DIR}
    )

    # Copy `#[cxx::bridge]` output to the target include directory with the sanitized name
    foreach (bridge_dst_h bridge_out_h IN ZIP_LISTS ADD_CRATE_BRIDGES_H_DST ADD_CRATE_BRIDGES_H_OUT)
        add_custom_command(
            OUTPUT
                ${bridge_dst_h}
            COMMAND
                ${CMAKE_COMMAND} -E copy ${bridge_out_h} ${bridge_dst_h}
            DEPENDS
                ${bridge_out_h}
            WORKING_DIRECTORY
                ${CMAKE_CURRENT_BINARY_DIR}
        )
    endforeach ()

    add_custom_target(${ADD_CRATE_NAME}_h DEPENDS ${ADD_CRATE_BRIDGES_H_DST})

    # And finally the object library to link with
    #
    # Note: Object libraries are "{crate_name}_rs" to avoid clashing with the
    # existing object libraries.
    add_library(${ADD_CRATE_NAME}_rs OBJECT EXCLUDE_FROM_ALL ${ADD_CRATE_BRIDGES_CC_OUT})
    target_include_directories(
        ${ADD_CRATE_NAME}_rs
        PUBLIC
            ${CMAKE_SOURCE_DIR}
            ${TILEDB_OXIDIZE_INCLUDE_DIR}
    )
    target_include_directories(
        ${ADD_CRATE_NAME}_rs
        PRIVATE
            ${CARGO_INCLUDE_DIR}
    )
    target_link_libraries(
        ${ADD_CRATE_NAME}_rs
        ${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${ADD_CRATE_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${TILEDB_OXIDIZE_LINK_LIBS}
    )
    add_dependencies(${ADD_CRATE_NAME}_rs ${ADD_CRATE_NAME}_h)
endfunction ()


# and now, the crates themselves!

add_crate(
    NAME
        arithmetic
    BRIDGES
        src/lib.rs
    SOURCES
        Cargo.toml
)

# `make clean` cleans up all cargo output and sanitized include paths
set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${CARGO_INCLUDE_DIR} ${TILEDB_OXIDIZE_INCLUDE_DIR})
