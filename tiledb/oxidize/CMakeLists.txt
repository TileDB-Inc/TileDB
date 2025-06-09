# assumes cargo is found via `Modules/FindCargo.cmake`


# FIXME: need to not do this if done already
# so that you can build offline
include(FetchContent)
set(FETCHCONTENT_UPDATES_DISCONNECTED TRUE)
FetchContent_Declare(
  Corrosion
  GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.5 # Optionally specify a commit hash, version tag or branch here
)
# Set any global configuration variables such as `Rust_TOOLCHAIN` before this line!
FetchContent_MakeAvailable(Corrosion)

corrosion_import_crate(MANIFEST_PATH Cargo.toml IMPORTED_CRATES corrosion_debug)

message(NOTICE "corrosion_debug = ${corrosion_debug}")

set(Rust_CARGO_TARGET "x86_64-unknown-linux-gnu") # FIXME

if ("${Rust_CARGO_TARGET}" STREQUAL "")
  message(SEND_ERROR "Variable Rust_CARGO_TARGET is not set")
endif ()

set(CORROSION_BUILD_DIR ${CMAKE_BINARY_DIR}/cargo/build/${Rust_CARGO_TARGET})
set(CARGO_INSTALL_ROOT ${CMAKE_BINARY_DIR}/cargo/install)
set(CARGO_INSTALL_BIN ${CARGO_INSTALL_ROOT}/bin)

message(NOTICE "CORROSION_BUILD_DIR=${CORROSION_BUILD_DIR}")
message(NOTICE "TILEDB_OXIDIZE_INCLUDE_DIR=${TILEDB_OXIDIZE_INCLUDE_DIR}")

#[[
add_custom_command(
  OUTPUT
    ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/arrow.h
    ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/expr.h
    ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/rust.h
  COMMAND
    ${CMAKE_COMMAND} -E copy ${CORROSION_OUTPUT_DIR}/cxxbridge/tiledb-arrow/src/lib.rs.h ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/arrow.h
  COMMAND
    ${CMAKE_COMMAND} -E copy ${CORROSION_OUTPUT_DIR}/cxxbridge/tiledb-expr/src/lib.rs.h ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/expr.h
  COMMAND
    ${CMAKE_COMMAND} -E copy ${CORROSION_OUTPUT_DIR}/cxxbridge/rust/cxx.h ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/rust.h
  DEPENDS
    oxidize_tiledb_core_objects
  WORKING_DIRECTORY
    ${CMAKE_CURRENT_BINARY_DIR}
)

add_custom_target(oxidize_tiledb_core_objects_h DEPENDS ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/arrow.h)

add_library(oxidize_tiledb_core_objects_cc OBJECT
  ${CORROSION_OUTPUT_DIR}/cxxbridge/tiledb-arrow/src/lib.rs.cc
  ${CORROSION_OUTPUT_DIR}/cxxbridge/tiledb-expr/src/lib.rs.cc
  ${CORROSION_OUTPUT_DIR}/cxxbridge/tiledb-oxidize/src/sm/array_schema/mod.rs.cc
  ${CORROSION_OUTPUT_DIR}/cxxbridge/tiledb-oxidize/src/sm/enums/mod.rs.cc
  ${CORROSION_OUTPUT_DIR}/cxxbridge/tiledb-oxidize/src/sm/query/readers/mod.rs.cc
  ${CORROSION_OUTPUT_DIR}/cxxbridge/tiledb-oxidize/src/sm/query/ast/mod.rs.cc
  ${CORROSION_OUTPUT_DIR}/cxxbridge/tiledb-oxidize/src/sm/misc/mod.rs.cc
  ${CORROSION_OUTPUT_DIR}/cxxbridge/tiledb-oxidize/src/sm/tile/mod.rs.cc
)
add_dependencies(oxidize_tiledb_core_objects_cc oxidize_tiledb_core_objects_h)
target_include_directories(oxidize_tiledb_core_objects_cc PUBLIC ${CMAKE_SOURCE_DIR} ${TILEDB_OXIDIZE_INCLUDE_DIR})
target_link_libraries(oxidize_tiledb_core_objects_cc PUBLIC oxidize_tiledb_core_objects)
#]]

set(OXIDIZE_RUST_H ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/rust.h)

execute_process(
  COMMAND
    ${CARGO} install cxxbridge-cmd --root ${CARGO_INSTALL_ROOT}
)
execute_process(
  COMMAND
    ${CMAKE_COMMAND} -E make_directory ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize
)
execute_process(
  COMMAND
    ${CARGO_INSTALL_BIN}/cxxbridge --header
  OUTPUT_FILE
    ${OXIDIZE_RUST_H}
)

function(oxidize_bridge)
  set(options)
  set(oneValueArgs CRATE)
  set(multiValueArgs SOURCES)
  cmake_parse_arguments(OXIDIZE_BRIDGE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  set(OXIDIZE_BRIDGE_SOURCES_CC_OUT "")
  set(OXIDIZE_BRIDGE_SOURCES_H_OUT "")
  foreach (bridge IN LISTS OXIDIZE_BRIDGE_SOURCES)
    list(APPEND OXIDIZE_BRIDGE_SOURCES_CC_OUT "${bridge}.cc")
    list(APPEND OXIDIZE_BRIDGE_SOURCES_H_OUT "${bridge}.h")
  endforeach ()

  add_library(${OXIDIZE_BRIDGE_CRATE}_generated INTERFACE)
  add_dependencies(${OXIDIZE_BRIDGE_CRATE}_generated ${OXIDIZE_BRIDGE_CRATE}_h)
  set_target_properties(${OXIDIZE_BRIDGE_CRATE}_generated PROPERTIES INTERFACE_PRECOMPILE_HEADERS "${OXIDIZE_BRIDGE_SOURCES_H_OUT}")
  set_target_properties(${OXIDIZE_BRIDGE_CRATE}_generated PROPERTIES INTERFACE_SOURCES "${OXIDIZE_BRIDGE_SOURCES_CC_OUT}")

  set(${OXIDIZE_BRIDGE_CRATE}_SOURCES ${OXIDIZE_BRIDGE_CRATE_CC_OUT})
endfunction()

function(oxidize_bundle)
  set(options)
  set(oneValueArgs NAME)
  set(multiValueArgs EXPORT)
  cmake_parse_arguments(OXIDIZE_BUNDLE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  if (NOT OXIDIZE_BUNDLE_NAME)
    message(SEND_ERROR "Function oxidize_bundle requires a NAME argument")
  endif ()

  if (NOT OXIDIZE_BUNDLE_EXPORT)
    message(SEND_ERROR "Function oxidize_bundle requies EXPORT argument")
  endif ()

  message(NOTICE "BEGIN ${OXIDIZE_BUNDLE_NAME}")

  set(OXIDIZE_BUNDLE_STATICLIB "${OXIDIZE_BUNDLE_NAME}")
  set(OXIDIZE_BUNDLE_INCLUDE_DIR "${TILEDB_OXIDIZE_INCLUDE_DIR}/${OXIDIZE_BUNDLE_NAME}")

  set(OXIDIZE_BUNDLE_EXPORT_H "")
  set(APPEND OXIDIZE_BUNDLE_EXPORT_CC "")
  foreach (export ${OXIDIZE_BUNDLE_EXPORT})
    set(bridge_generated_dir "${CORROSION_BUILD_DIR}/cxxbridge/tiledb-${export}/src")

    get_target_property(export_H ${export}_generated INTERFACE_PRECOMPILE_HEADERS)
    foreach (bridge_h ${export_H})
      if (${bridge_h} MATCHES "lib.rs.h")
        set(bridge_dst "${export}")
      elseif (${bridge_h} MATCHES "(.*)\/mod.rs.h")
        set(bridge_dst "${export}/${CMAKE_MATCH_1}")
      elseif (${bridge} MATCHES "(.*\/)*(.*).rs.h")
        set(bridge_dst "${export}/${CMAKE_MATCH_1}/${CMAKE_MATCH_2}")
      else ()
        message(SEND_ERROR "Unable to set 'bridge_dst': unexpected pattern in bridge file path: ${bridge}")
      endif ()

      set(bundle_h_dst "${OXIDIZE_BUNDLE_INCLUDE_DIR}/tiledb/oxidize/${bridge_dst}.h")
      get_filename_component(bundle_h_dst_dirname ${bundle_h_dst} DIRECTORY)

      message(NOTICE "bridge_h = ${bridge_h}, bridge_dst = ${bridge_dst}, bundle_h_dst = ${bundle_h_dst}")

      add_custom_command(
        OUTPUT
          "${bundle_h_dst}"
        COMMAND
          ${CMAKE_COMMAND} -E make_directory ${bundle_h_dst_dirname}
        COMMAND
          ${CMAKE_COMMAND} -E create_symlink ${bridge_generated_dir}/${bridge_h} ${bundle_h_dst}
        DEPENDS
          ${OXIDIZE_BUNDLE_STATICLIB}
        WORKING_DIRECTORY
          ${CMAKE_CURRENT_BINARY_DIR}
      )

      list(APPEND OXIDIZE_BUNDLE_EXPORT_H "${bundle_h_dst}")

      message(NOTICE "OXIDIZE_BUNDLE_EXPORT_H = ${OXIDIZE_BUNDLE_EXPORT_H}")
    endforeach ()

    get_target_property(export_SOURCES ${export}_generated INTERFACE_SOURCES)
    foreach (export_cc ${export_SOURCES})
      set(bundle_cc_dst ${CORROSION_BUILD_DIR}/cxxbridge/tiledb-${export}/src/${export_cc})

      add_custom_command(
        OUTPUT
          "${bundle_cc_dst}"
        COMMAND
          ${CMAKE_COMMAND} -E echo "File '${export_cc}' is built by target '${OXIDIZE_BUNDLE_STATICLIB}'."
        DEPENDS
          ${OXIDIZE_BUNDLE_STATICLIB}
        WORKING_DIRECTORY
          ${CMAKE_CURRENT_BINARY_DIR}
      )

      list(APPEND OXIDIZE_BUNDLE_EXPORT_CC ${bundle_cc_dst})

      message(NOTICE "OXIDIZE_BUNDLE_EXPORT_CC = ${OXIDIZE_BUNDLE_EXPORT_CC}")
    endforeach ()
  endforeach ()

  message(NOTICE "EXPORT_H = ${OXIDIZE_BUNDLE_EXPORT_H}")
  message(NOTICE "EXPORT_CC = ${OXIDIZE_BUNDLE_EXPORT_CC}")

  add_custom_target(${OXIDIZE_BUNDLE_NAME}_h DEPENDS ${OXIDIZE_BUNDLE_EXPORT_H})
  add_custom_target(${OXIDIZE_BUNDLE_NAME}_cc DEPENDS ${OXIDIZE_BUNDLE_EXPORT_CC})

  set(OXIDIZE_BUNDLE_LIB "${OXIDIZE_BUNDLE_NAME}_cxxbridge")

  add_library(${OXIDIZE_BUNDLE_LIB} OBJECT ${OXIDIZE_BUNDLE_EXPORT_CC})
  add_dependencies(${OXIDIZE_BUNDLE_LIB} ${OXIDIZE_BUNDLE_NAME}_h ${OXIDIZE_BUNDLE_NAME}_cc ${OXIDIZE_BUNDLE_STATICLIB})
  target_include_directories(${OXIDIZE_BUNDLE_LIB} PUBLIC ${CMAKE_SOURCE_DIR} ${TILEDB_OXIDIZE_INCLUDE_DIR} ${OXIDIZE_BUNDLE_INCLUDE_DIR})
  target_link_libraries(${OXIDIZE_BUNDLE_LIB} PUBLIC assert_header ${OXIDIZE_BUNDLE_STATICLIB})
endfunction ()

oxidize_bridge(
  CRATE
    arithmetic
  SOURCES
    lib.rs
)

oxidize_bridge(
  CRATE
    arrow
  SOURCES
    lib.rs
)

oxidize_bridge(
  CRATE
    expr
  SOURCES
    lib.rs
)

oxidize_bridge(
  CRATE
    oxidize
  SOURCES
    sm/array_schema/mod.rs
    sm/enums/mod.rs
    sm/query/readers/mod.rs
    sm/query/ast/mod.rs
    sm/misc/mod.rs
    sm/tile/mod.rs
) 

oxidize_bundle(
  NAME
    oxidize_tiledb_core_objects
  EXPORT
    arrow
    expr
    oxidize
)

oxidize_bundle(
  NAME
    oxidize_unit_arithmetic
  EXPORT
    arithmetic
)

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${TILEDB_OXIDIZE_INCLUDE_DIR})
