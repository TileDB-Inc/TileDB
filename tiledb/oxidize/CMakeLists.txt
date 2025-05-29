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
    set(multiValueArgs BRIDGES SOURCES)
    cmake_parse_arguments(ADD_CRATE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    if (NOT ADD_CRATE_NAME)
        message(SEND_ERROR "Function add_crate requires a NAME argument")
    endif()

    if (NOT ADD_CRATE_SOURCES)
        message(SEND_ERROR "Function add_crate requires one or more SOURCES arguments")
    endif()

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
        # TODO: account for src/thing.rs or src/module_name/thing.rs
        # TODO: error
        message(SEND_ERROR "TODO")
      endif ()
      list(APPEND ADD_CRATE_BRIDGES_CC_OUT "${CARGO_INCLUDE_DIR}/cxxbridge/${ADD_CRATE_NAME}/${bridge}.cc")
      list(APPEND ADD_CRATE_BRIDGES_H_OUT "${CARGO_INCLUDE_DIR}/cxxbridge/${ADD_CRATE_NAME}/${bridge}.h")
      list(APPEND ADD_CRATE_BRIDGES_H_DST "${CMAKE_CURRENT_BINARY_DIR}/${bridge_out}.h")
    endforeach ()
    list(TRANSFORM ADD_CRATE_BRIDGES PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/${ADD_CRATE_NAME}/)
    list(TRANSFORM ADD_CRATE_SOURCES PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/${ADD_CRATE_NAME}/)

    add_custom_command(
        OUTPUT
            ${ADD_CRATE_BRIDGES_CC_OUT}
            ${ADD_CRATE_BRIDGES_H_OUT}
            "${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${ADD_CRATE_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
        COMMAND
            ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target"
            ${CARGO}
            build
            "-q"
            "--profile" ${CARGO_PROFILE}
            "--manifest-path" ${CARGO_MANIFEST_PATH}
            "-p" ${ADD_CRATE_NAME}
        USES_TERMINAL
        DEPENDS
            ${ADD_CRATE_BRIDGES}
            ${ADD_CRATE_SOURCES}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )

  # FIXME: this is not run
  foreach (bridge_dst_h bridge_out_h IN ZIP_LISTS ADD_CRATE_BRIDGES_H_DST ADD_CRATE_BRIDGES_H_OUT)
      add_custom_command(
        OUTPUT
          ${bridge_dst_h}
        COMMAND
          ${CMAKE_COMMAND} -E copy ${bridge_out_h} ${bridge_dst_h}
        DEPENDS
          ${bridge_out_h}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      )
    endforeach ()

    # Note: Object libraries are "{crate_name}_rs" to avoid clashing with the
    # existing object libraries.
    add_library(${ADD_CRATE_NAME}_rs OBJECT EXCLUDE_FROM_ALL ${ADD_CRATE_BRIDGES_CC_OUT})
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
    BRIDGES src/lib.rs
    SOURCES Cargo.toml
)

add_crate(
    NAME expr
    BRIDGES
        src/lib.rs
    SOURCES
        Cargo.toml
        src/logical_expr.rs
        src/offsets.rs
        src/physical_expr.rs
        src/record_batch.rs
        src/schema.rs
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
