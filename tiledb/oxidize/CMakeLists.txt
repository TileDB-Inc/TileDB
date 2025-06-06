# assumes cargo is found via `Modules/FindCargo.cmake`


# FIXME: need to not do this if done already
# so that you can build offline
include(FetchContent)
FetchContent_Declare(
  Corrosion
  GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.5 # Optionally specify a commit hash, version tag or branch here
)
# Set any global configuration variables such as `Rust_TOOLCHAIN` before this line!
FetchContent_MakeAvailable(Corrosion)

corrosion_import_crate(MANIFEST_PATH Cargo.toml)

set(Rust_CARGO_TARGET "x86_64-unknown-linux-gnu") # FIXME

if ("${Rust_CARGO_TARGET}" STREQUAL "")
  message(SEND_ERROR "Variable Rust_CARGO_TARGET is not set")
endif ()

set(CORROSION_BUILD_DIR ${CMAKE_BINARY_DIR}/cargo/build/${Rust_CARGO_TARGET})
set(CARGO_INSTALL_ROOT ${CMAKE_BINARY_DIR}/cargo/install)
set(CARGO_INSTALL_BIN ${CARGO_INSTALL_ROOT}/bin)

message(NOTICE "CORROSION_OUTPUT_DIR=${CORROSION_OUTPUT_DIR}")
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

add_custom_command(
  OUTPUT
    ${OXIDIZE_RUST_H}
  COMMAND
    ${CARGO} install cxxbridge-cmd --root ${CARGO_INSTALL_ROOT}
  COMMAND
    ${CARGO_INSTALL_BIN}/cxxbridge --header > "${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/rust.h"
  WORKING_DIRECTORY
    ${CMAKE_BINARY_DIR}
)

function(oxidize_bridge)
  set(options)
  set(oneValueArgs CRATE)
  set(multiValueArgs SOURCES)
  cmake_parse_arguments(OXIDIZE_BRIDGE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  set(OXIDIZE_BRIDGE_SOURCES_CC_OUT "")
  set(OXIDIZE_BRIDGE_SOURCES_H_OUT "")
  set(OXIDIZE_BRIDGE_SOURCES_H_DST "")
  foreach (bridge IN LISTS OXIDIZE_BRIDGE_SOURCES)
    if (${bridge} MATCHES "lib.rs")
      set(bridge_dst "${OXIDIZE_BRIDGE_CRATE}")
    elseif (${bridge} MATCHES "(.*)\/*(.*)\/mod.rs")
      set(bridge_dst "${OXIDIZE_BRIDGE_CRATE}/${CMAKE_MATCH_1}/${CMAKE_MATCH_2}")
    elseif (${bridge} MATCHES "(.*\/)*(.*).rs")
      set(bridge_dst "${OXIDIZE_BRIDGE_CRATE}/${CMAKE_MATCH_1}/${CMAKE_MATCH_2}")
    else ()
      message(SEND_ERROR "Unable to set 'bridge_dst': unexpected pattern in bridge file path: ${bridge}")
    endif ()
    list(APPEND OXIDIZE_BRIDGE_SOURCES_CC_OUT "${CORROSION_BUILD_DIR}/cxxbridge/tiledb-${OXIDIZE_BRIDGE_CRATE}/src/${bridge}.cc")
    list(APPEND OXIDIZE_BRIDGE_SOURCES_H_OUT "${CORROSION_BUILD_DIR}/cxxbridge/tiledb-${OXIDIZE_BRIDGE_CRATE}/src/${bridge}.h")
    list(APPEND OXIDIZE_BRIDGE_SOURCES_H_DST "${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/${bridge_dst}.h")
  endforeach ()

  message(NOTICE "CRATE: ${OXIDIZE_BRIDGE_CRATE}")
  message(NOTICE "CC_OUT: ${OXIDIZE_BRIDGE_SOURCES_CC_OUT}")
  #message(NOTICE "H_OUT: ${OXIDIZE_BRIDGE_SOURCES_H_OUT}")
  #message(NOTICE "H_DST: ${OXIDIZE_BRIDGE_SOURCES_H_DST}")

  foreach(bridge_dst_h bridge_out_h IN ZIP_LISTS OXIDIZE_BRIDGE_SOURCES_H_DST OXIDIZE_BRIDGE_SOURCES_H_OUT)
    #message(NOTICE "bridge_dst_h = ${bridge_dst_h}, bridge_out_h = ${bridge_out_h}")
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
  endforeach()

  add_custom_target(${OXIDIZE_BRIDGE_CRATE}_h DEPENDS ${OXIDIZE_RUST_H} ${OXIDIZE_BRIDGE_SOURCES_H_DST})

  # does not work, undefined references to what appears to be rlib symbols?
  #[[
  add_library(${OXIDIZE_BRIDGE_CRATE}_bridge OBJECT ${OXIDIZE_BRIDGE_SOURCES_CC_OUT})
  add_dependencies(${OXIDIZE_BRIDGE_CRATE}_bridge ${OXIDIZE_BRIDGE_CRATE}_h)
  target_include_directories(${OXIDIZE_BRIDGE_CRATE}_bridge PUBLIC ${CMAKE_SOURCE_DIR} ${TILEDB_OXIDIZE_INCLUDE_DIR})
  #]]

  # does not work, propagate include directory?
  #[[
  add_library(${OXIDIZE_BRIDGE_CRATE}_bridge INTERFACE)
  add_dependencies(${OXIDIZE_BRIDGE_CRATE}_bridge ${OXIDIZE_BRIDGE_CRATE}_h)
  set_target_properties(${OXIDIZE_BRIDGE_CRATE}_bridge PROPERTIES INTERFACE_SOURCES "${OXIDIZE_BRIDGE_SOURCES_CC_OUT}")
  target_include_directories(${OXIDIZE_BRIDGE_CRATE}_bridge INTERFACE ${CMAKE_SOURCE_DIR} ${TILEDB_OXIDIZE_INCLUDE_DIR})
  #]]

  add_library(${OXIDIZE_BRIDGE_CRATE}_bridge INTERFACE)
  add_dependencies(${OXIDIZE_BRIDGE_CRATE}_bridge ${OXIDIZE_BRIDGE_CRATE}_h)
  set_target_properties(${OXIDIZE_BRIDGE_CRATE}_bridge PROPERTIES INTERFACE_SOURCES "${OXIDIZE_BRIDGE_SOURCES_CC_OUT}")

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

  set(OXIDIZE_BUNDLE_EXPORT_H "")
  set(OXIDIZE_BUNDLE_EXPORT_LIBS "${OXIDIZE_BUNDLE_NAME}")
  set(APPEND OXIDIZE_BUNDLE_EXPORT_CC "")
  foreach (export ${OXIDIZE_BUNDLE_EXPORT})
    list(APPEND OXIDIZE_BUNDLE_EXPORT_H ${export}_h)
    #[[
    list(APPEND OXIDIZE_BUNDLE_EXPORT_LIBS ${export}_bridge)
    #]]
    #[[
    foreach (export_object $<TARGET_OBJECTS:${export}_bridge>)
      list(APPEND OXIDIZE_BUNDLE_EXPORT_CC ${export_object})
    endforeach ()
    #]]
    get_target_property(export_SOURCES ${export}_bridge INTERFACE_SOURCES)
    foreach (export_cc ${export_SOURCES})
      list(APPEND OXIDIZE_BUNDLE_EXPORT_CC ${export_cc})
    endforeach ()
  endforeach ()

  message(NOTICE "EXPORT_H = ${OXIDIZE_BUNDLE_EXPORT_H}")
  message(NOTICE "EXPORT_LIBS = ${OXIDIZE_BUNDLE_EXPORT_LIBS}")
  message(NOTICE "EXPORT_CC = ${OXIDIZE_BUNDLE_EXPORT_CC}")

  # does not work, undefined references to what appears to be rlib symbols?
  #[[
  add_library(${OXIDIZE_BUNDLE_NAME}_cc INTERFACE)
  #target_link_libraries(${OXIDIZE_BUNDLE_NAME}_cc INTERFACE ${OXIDIZE_BUNDLE_NAME} ${OXIDIZE_BUNDLE_EXPORT_LIBS})
  target_include_directories(${OXIDIZE_BUNDLE_NAME}_cc INTERFACE ${CMAKE_SOURCE_DIR} ${TILEDB_CMAKE_INCLUDE})
  add_dependencies(${OXIDIZE_BUNDLE_NAME}_cc ${OXIDIZE_BUNDLE_EXPORT_H})
  set_target_properties(${OXIDIZE_BUNDLE_NAME}_cc PROPERTIES INTERFACE_SOURCES "${OXIDIZE_BUNDLE_EXPORT_CC}")
  #]]

  # does not work, propagate include directory?
  #[[
  add_library(${OXIDIZE_BUNDLE_NAME}_cc INTERFACE)
  add_dependencies(${OXIDIZE_BUNDLE_NAME}_cc ${OXIDIZE_BUNDLE_EXPORT_H})
  target_link_libraries(${OXIDIZE_BUNDLE_NAME}_cc INTERFACE ${OXIDIZE_BUNDLE_EXPORT_LIBS})
  target_include_directories(${OXIDIZE_BUNDLE_NAME}_cc INTERFACE ${CMAKE_SOURCE_DIR} ${TILEDB_CMAKE_INCLUDE})
  set_target_properties(${OXIDIZE_BUNDLE_NAME}_cc PROPERTIES INTERFACE_LINK_LIBRARIES ${OXIDIZE_BUNDLE_NAME})
  #]]

  add_library(${OXIDIZE_BUNDLE_NAME}_cc OBJECT ${OXIDIZE_BUNDLE_EXPORT_CC})
  add_dependencies(oxidize_tiledb_core_objects_cc ${OXIDIZE_BUNDLE_EXPORT_H})
  target_include_directories(oxidize_tiledb_core_objects_cc PUBLIC ${CMAKE_SOURCE_DIR} ${TILEDB_OXIDIZE_INCLUDE_DIR})
  target_link_libraries(oxidize_tiledb_core_objects_cc PUBLIC oxidize_tiledb_core_objects)
endfunction ()

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

#[[
oxidize_bridge(
  CRATE
    arithmetic
  SOURCES
    lib.rs
)
]]#

oxidize_bundle(
  NAME
    oxidize_tiledb_core_objects
  EXPORT
    arrow
    expr
    oxidize
)

#[[
oxidize_bundle(
  NAME
    oxidize_unit_arithmetic
  EXPORT
    arithmetic
)
]]#




#[[

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


#
# Occasionally it will be necessary for C++ code to include just the Rust
# standard library functions exposed by CXX. Declare variables for a
# sanitized include path.
#
set(RUST_H_OUT "${CMAKE_BINARY_DIR}/tiledb/oxidize/target/cxxbridge/rust/cxx.h")
set(RUST_H_DST "${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/rust.h")


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
#   Building this target produces the object library and also the `#[cxx::bridge]` files.
function(add_crate)
    set(options)
    set(oneValueArgs NAME)
    set(multiValueArgs BRIDGES DEPENDS SOURCES)
    cmake_parse_arguments(ADD_CRATE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    if (NOT ADD_CRATE_NAME)
        message(SEND_ERROR "Function add_crate requires a NAME argument")
    endif()

    if (NOT ADD_CRATE_SOURCES AND NOT ADD_CRATE_BRIDGES)
        message(SEND_ERROR "Function add_crate requires SOURCES argument, BRIDGES argument, or both")
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
            message(SEND_ERROR "Unable to set 'bridge_out': unexpected pattern in bridge file path: ${bridge}")
        endif ()
        list(APPEND ADD_CRATE_BRIDGES_CC_OUT "${CARGO_INCLUDE_DIR}/cxxbridge/tiledb-${ADD_CRATE_NAME}/${bridge}.cc")
        list(APPEND ADD_CRATE_BRIDGES_H_OUT "${CARGO_INCLUDE_DIR}/cxxbridge/tiledb-${ADD_CRATE_NAME}/${bridge}.h")
        list(APPEND ADD_CRATE_BRIDGES_H_DST "${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/${bridge_out}.h")
    endforeach ()
    list(TRANSFORM ADD_CRATE_BRIDGES PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/${ADD_CRATE_NAME}/)
    list(TRANSFORM ADD_CRATE_SOURCES PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/${ADD_CRATE_NAME}/)

    add_custom_command(
        OUTPUT
            ${ADD_CRATE_BRIDGES_CC_OUT}
            ${ADD_CRATE_BRIDGES_H_OUT}
            ${RUST_H_OUT}
            "${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${ADD_CRATE_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
        COMMAND
            ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target" "CFLAGS=${TILEDB_OXIDIZE_CFLAGS}" "CXXFLAGS=${TILEDB_OXIDIZE_CFLAGS}"
            ${CARGO}
            build
            ${CARGO_VERBOSITY}
            "--profile" ${CARGO_PROFILE}
            "--manifest-path" ${CARGO_MANIFEST_PATH}
            "-p" "tiledb-${ADD_CRATE_NAME}"
        USES_TERMINAL
        DEPENDS
            ${ADD_CRATE_BRIDGES}
            ${ADD_CRATE_SOURCES}
        WORKING_DIRECTORY
            ${CMAKE_CURRENT_BINARY_DIR}
    )

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

    add_custom_target(${ADD_CRATE_NAME}_h DEPENDS ${ADD_CRATE_BRIDGES_H_DST} ${RUST_H_DST})

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
        ${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}tiledb_${ADD_CRATE_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${TILEDB_OXIDIZE_LINK_LIBS}
    )
    add_dependencies(${ADD_CRATE_NAME}_rs ${ADD_CRATE_NAME}_h ${ADD_CRATE_DEPENDS})
endfunction ()

# now the sanitized rust header
add_custom_command(
    OUTPUT
        ${RUST_H_DST}
    COMMAND
        ${CMAKE_COMMAND} -E copy ${RUST_H_OUT} ${RUST_H_DST}
    DEPENDS
        ${RUST_H_OUT}
    WORKING_DIRECTORY
        ${CMAKE_CURRENT_BINARY_DIR}
)


# and now, the crates themselves!

add_crate(
    NAME
        arithmetic
    BRIDGES
        src/lib.rs
    SOURCES
        Cargo.toml
)

add_crate(
    NAME
        arrow
    BRIDGES
        src/lib.rs
    SOURCES
        Cargo.toml
        src/offsets.rs
        src/record_batch.rs
        src/schema.rs
)

add_crate(
    NAME
        expr
    DEPENDS
        arrow_rs
    BRIDGES
        src/lib.rs
    SOURCES
        Cargo.toml
        src/logical_expr.rs
        src/physical_expr.rs
)

add_crate(
    NAME
        oxidize
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

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${CARGO_INCLUDE_DIR} ${TILEDB_OXIDIZE_INCLUDE_DIR})

#]]

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${TILEDB_OXIDIZE_INCLUDE_DIR})
