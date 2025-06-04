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
