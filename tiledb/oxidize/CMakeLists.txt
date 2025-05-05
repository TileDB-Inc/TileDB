# assumes cargo is found via `Modules/FindCargo.cmake`

# and now, the crates themselves!

add_crate(
    NAME arithmetic
    BRIDGES src/lib.rs
    SOURCES Cargo.toml
)

add_crate(
    NAME expr
    BRIDGES
        src/lib.rs
    SOURCES
        Cargo.toml
        src/record_batch.rs
)

add_crate(
    NAME oxidize
    BRIDGES
        src/sm/array_schema/mod.rs
        src/sm/enums/mod.rs
        src/sm/misc/mod.rs
        src/sm/query/ast/mod.rs
        src/sm/query/readers/mod.rs
        src/sm/tile/mod.rs
    SOURCES
        Cargo.toml
)

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${CARGO_INCLUDE_DIR} *.h)
