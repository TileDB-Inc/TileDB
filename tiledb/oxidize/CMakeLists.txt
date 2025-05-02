# assumes cargo is found via `Modules/FindCargo.cmake`

include(oxidize)

add_crate(
    NAME
        arithmetic
    BRIDGES
        src/lib.rs
    SOURCES
        Cargo.toml
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
