# assumes cargo is found via `Modules/FindCargo.cmake`

# and now, the crates themselves!

add_crate(
    NAME arithmetic
    SOURCES Cargo.toml arithmetic/src/lib.rs
)

add_crate(
    NAME expr
    SOURCES Cargo.toml src/lib.rs
)

add_crate(
    NAME oxidize
    SOURCES
        Cargo.toml
        src/lib.rs
        src/sm/mod.rs
        src/sm/array_schema/mod.rs
        src/sm/enums/mod.rs
        src/sm/query/mod.rs
        src/sm/query/ast/mod.rs
)

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${CARGO_INCLUDE_DIR} *.h)
