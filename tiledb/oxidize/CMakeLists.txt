# assumes cargo is found via `Modules/FindCargo.cmake`
include(oxidize)


corrosion_import_crate(MANIFEST_PATH Cargo.toml IMPORTED_CRATES corrosion_debug)


cxxbridge(
  NAME
    arithmetic
  SOURCES
    lib.rs
)

cxxbridge(
  NAME
    arrow
  SOURCES
    lib.rs
)

cxxbridge(
  NAME
    expr
  SOURCES
    lib.rs
)

cxxbridge(
  NAME
    oxidize
  SOURCES
    common/memory_tracker.rs
    sm/array_schema/mod.rs
    sm/enums/mod.rs
    sm/query/readers/mod.rs
    sm/query/ast/mod.rs
    sm/misc/mod.rs
    sm/tile/mod.rs
) 

cxxbridge(
  NAME
    test-support
  SOURCES
    oxidize.cc
    lib.rs
)

oxidize(
  NAME
    tiledb_core_objects
  EXPORT
    arrow
    expr
    oxidize
)

oxidize(
  NAME
    unit_arithmetic
  EXPORT
    arithmetic
)

oxidize(
  NAME
    unit_query_condition
  SOURCES
    lib.rs
  EXPORT
    arrow
    expr
    oxidize
    test-support
)

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${TILEDB_OXIDIZE_INCLUDE_DIR})
