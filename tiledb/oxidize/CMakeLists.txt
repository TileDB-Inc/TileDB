# assumes cargo is found via `Modules/FindCargo.cmake`
include(oxidize)


if (TILEDB_RUST)
  corrosion_import_crate(MANIFEST_PATH Cargo.toml IMPORTED_CRATES corrosion_debug)
endif ()


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
    cxx-interface
  SOURCES
    array_schema.cc
    common/memory_tracker.rs
    sm/array_schema/mod.rs
    sm/buffer.rs
    sm/enums/mod.rs
    sm/query/readers/mod.rs
    sm/query/ast/mod.rs
    sm/misc/mod.rs
    sm/tile/mod.rs
)

cxxbridge(
  NAME
    query-predicates
  SOURCES
    lib.rs
)

cxxbridge(
  NAME
    test-support-cxx-interface
  PATH
    test-support/cxx-interface
  SOURCES
    array_schema.cc
    query_ast.cc
    result_tile.cc
    lib.rs
)

oxidize(
  NAME
    tiledb_core_objects
  EXPORT
    arrow
    cxx-interface
    query-predicates
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
    cxx-interface
    query-predicates
    test-support-cxx-interface
)

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${TILEDB_OXIDIZE_INCLUDE_DIR})
