set(CRATE cxx_rust_std)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CARGO_CMD cargo build -p ${CRATE})
  set(TARGET_DIR "debug")
else ()
  set(CARGO_CMD cargo build --release -p ${CRATE})
  set(TARGET_DIR "release")
endif ()

set(CRATE_H "${CMAKE_CURRENT_BINARY_DIR}/rust_std.h")
set(CRATE_CXX "${CMAKE_CURRENT_BINARY_DIR}/rust_std.cc")
set(CRATE_LIB "${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/lib${CRATE}.a")

add_custom_command(
  OUTPUT ${CRATE_H} ${CRATE_CXX} ${CRATE_LIB}
  COMMAND MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET} CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} RUSTFLAGS="${RUST_FLAGS}" ${CARGO_CMD}
  COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/${CRATE}/src/lib.rs.h  ${CRATE_H}
  COMMAND cp ${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/${CRATE}/src/lib.rs.cc ${CRATE_CXX}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  # FIXME: this needs to re-run if any of the Rust sources changes
)

add_custom_target(cxx_rust_std_generated
    DEPENDS
    ${CRATE_H}
    ${CRATE_CXX}
    ${CRATE_LIB}
)

add_library(cxx_rust_std STATIC ${CRATE_CXX})
add_dependencies(cxx_rust_std cxx_rust_std_generated)
target_link_libraries(cxx_rust_std pthread dl ${CRATE_LIB})

add_test(
    NAME cxx_rust_std
    COMMAND CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} cargo test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/include)
