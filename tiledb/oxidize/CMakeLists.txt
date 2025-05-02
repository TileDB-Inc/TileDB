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


# Declares targets for building a Rust crate.

# Inputs:
#   NAME crate_name
#   SOURCES source1 source2 ...
#
# Outputs:
#   ${crate_name}_rs object library target
#   Building this target produces the object library and also the `#[cxx::bridge]` files.
function(add_crate)
    set(options)
    set(oneValueArgs NAME)
    set(multiValueArgs SOURCES)
    cmake_parse_arguments(ADD_CRATE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    if (NOT ADD_CRATE_NAME)
        message(SEND_ERROR "Function add_crate requires a NAME argument")
    endif()

    if (NOT ADD_CRATE_SOURCES)
        message(SEND_ERROR "Function add_crate requires one or more SOURCES arguments")
    endif()

    add_custom_command(
        OUTPUT
            "${CMAKE_CURRENT_BINARY_DIR}/${ADD_CRATE_NAME}.h"
            "${CARGO_INCLUDE_DIR}/cxxbridge/${ADD_CRATE_NAME}/src/lib.rs.cc"
            "${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${ADD_CRATE_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
        COMMAND
            ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target"
            ${CARGO}
            build
            "-q"
            "--profile" ${CARGO_PROFILE}
            "--manifest-path" ${CARGO_MANIFEST_PATH}
            "-p" ${ADD_CRATE_NAME}
        COMMAND
            ${CMAKE_COMMAND} -E copy "${CARGO_INCLUDE_DIR}/cxxbridge/${ADD_CRATE_NAME}/src/lib.rs.h" "${CMAKE_CURRENT_BINARY_DIR}/${ADD_CRATE_NAME}.h"
        USES_TERMINAL
        DEPENDS
            ${ADD_CRATE_SOURCES}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )

    # Note: Object libraries are "{crate_name}_rs" to avoid clashing with the
    # existing object libraries.
    add_library(${ADD_CRATE_NAME}_rs OBJECT EXCLUDE_FROM_ALL "${CARGO_INCLUDE_DIR}/cxxbridge/${ADD_CRATE_NAME}/src/lib.rs.cc")
    target_include_directories(
        ${ADD_CRATE_NAME}_rs
        PUBLIC
            ${CARGO_INCLUDE_DIR}
            ${CMAKE_SOURCE_DIR}
            ${CMAKE_BINARY_DIR}/tiledb/
    )
    target_link_libraries(
        ${ADD_CRATE_NAME}_rs
        ${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${ADD_CRATE_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}
    )
endfunction ()


# and now, the crates themselves!

add_crate(
    NAME arithmetic
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/arithmetic/Cargo.toml ${CMAKE_CURRENT_SOURCE_DIR}/arithmetic/src/lib.rs
)

add_crate(
    NAME array_schema
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/expr/Cargo.toml ${CMAKE_CURRENT_SOURCE_DIR}/expr/src/lib.rs
)

add_crate(
    NAME expr
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/expr/Cargo.toml ${CMAKE_CURRENT_SOURCE_DIR}/expr/src/lib.rs
)

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${CARGO_INCLUDE_DIR} *.h)
