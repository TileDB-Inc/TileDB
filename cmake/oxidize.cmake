#
# cmake/oxidize.cmake
#
# The MIT License
#
# Copyright (c) 2025 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
###############################################################################
#
# Provides functions and definitions for packaging Rust crates into a shared
# library or executable.
#
# The design of the build system is motivated by the following:
#
# 1) This constraint upon Rust FFI described at https://cxx.rs/build/other.html
#    > If you need to link multiple Rust subsystems, you will need to
#    > generate a single staticlib perhaps using lots of extern crate
#    > statements to include multiple Rust rlibs.
#    > Multiple Rust staticlib files are likely to conflict.
#
# 2) Our desire to build a large number of small standalone unit test
#    executables.
#
# 3) Those standalone unit test executables would like to share internal
#    Rust crates and dependencies.
#
# To enable a union of these constraints, we separate the crates which declare
# the `#[cxx::bridge]` modules from the crates which export symbols as a
# static library.
#
# For each top-level build target which integrates Rust, we define a
# Rust `staticlib` crate, e.g. `tiledb_core_objects_rs`.
# These crates re-export the symbols of any of the needed crates with
# `#[cxx::bridge]` modules, and may contain bridges of their own.
#
# The `rlib` bridge crates may be re-used by an arbitrary number of
# these `staticlib` crates. But each top-level target may link only
# one `staticlib`.
#
# The functions defined in this CMake module encode this relationship:
#
# The `cxxbridge` function declares the Rust source files which contain
# `#[cxx::bridge]` modules within that crate and the set of accompanying
# C++ source files (if any).  This function does not generate any targets
# which are intended to be built, it just sets some metadata properties
# which are used by the `oxidize` function.
#
# The `oxidize` function declares the set of compilation units which
# package everything together to call the Rust code exported by the
# named Rust `staticlib` crate.  These compilation units are:
# - C++ source files
# - Rust source files with `#[cxx::bridge]` modules
# - Rust `cxxbridge` crate dependencies
# This function creates targets which sanitize the paths of generated C++
# source and header files, and an object library which links everything
# toegether and propagates the headers via include path.
#
# This layout is influenced by our use of the Corrosion open-source project
# which imports a Rust `Cargo.toml` file to into CMake. This generates targets
# for `staticlib`s and executables, but not intermediate `rlib`. This requires
# our `oxidize` function to do most of the work. A positive benefit of this
# is that Cargo is in charge of resolving dependencies inside the Rust
# workspace, not the build configuration produced by CMake. And a positive
# consequence of *that* is that we do not need to declare targets for any
# Rust code beyond what is already described.
#
###############################################################################

include_guard()


if (TILEDB_RUST)
  #
  # Fetch Corrosion cmake functions.
  # Corrosion provides functions which declare targets to build
  # Rust static libraries and executables.
  #
  include(FetchContent)
  set(FETCHCONTENT_UPDATES_DISCONNECTED TRUE)
  FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.5 # Optionally specify a commit hash, version tag or branch here
  )
  # Set any global configuration variables such as `Rust_TOOLCHAIN` before this line!
  FetchContent_MakeAvailable(Corrosion)


  #
  # Identify the Corrosion build directory.
  # This is needed to add it to include paths.
  # See https://corrosion-rs.github.io/corrosion/usage.html#global-corrosion-options
  #
  if (CMAKE_VS_PLATFORM_NAME)
    set(CORROSION_BUILD_DIR "${CMAKE_BINARY_DIR}/cargo/build/${CMAKE_VS_PLATFORM_NAME}")
  else ()
    execute_process(
      COMMAND
        rustc --version --verbose
      OUTPUT_VARIABLE
        RUSTC_VERSION
      RESULT_VARIABLE
        RUSTC_VERSION_STATUS
    )
    if (NOT RUSTC_VERSION_STATUS EQUAL "0")
      message(SEND_ERROR "Error identifying Corrosion build directory: error executing 'rustc --version --verbose'")
    endif()
    if (RUSTC_VERSION MATCHES "host: ([a-zA-Z0-9_\\-]*)\n")
      set(CORROSION_BUILD_DIR "${CMAKE_BINARY_DIR}/cargo/build/${CMAKE_MATCH_1}")
    else ()
      message(SEND_ERROR "Error identifying Corrosion build directory: did not find 'host' in 'rustc --version --verbose'")
    endif ()
  endif ()

  #
  # `tiledb/oxidize/rust.h`
  #
  # The `cxx` crate which we use can cause circular dependencies of classes.
  # One usually solves this in C++ with forward declarations in one of the
  # header files.
  #
  # The following commands generate `rust.h` which contains
  # the standalone "Rust API" generated by `cxx`. This can be used
  # to forward-declare Box, Slice, etc.
  #
  # Shell direction is not guaranteed to work with custom build commands,
  # so we populate a copy of the file at build configuration time and then
  # copy it to the include directory at build time.
  #
  set(OXIDIZE_RUST_H ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/rust.h)
  set(OXIDIZE_RUST_H_SRC ${CMAKE_CURRENT_BINARY_DIR}/rust.h)

  set(CARGO_INSTALL_ROOT ${CMAKE_BINARY_DIR}/cargo/install)
  set(CARGO_INSTALL_BIN ${CARGO_INSTALL_ROOT}/bin)

  # pin version of cxxbridge due to https://github.com/dtolnay/cxx/issues/1436 build errors on MacOS
  set(CXXBRIDGE_VERSION 1.0.138)

  execute_process(
    COMMAND
      ${CARGO} install cxxbridge-cmd --version ${CXXBRIDGE_VERSION} --root ${CARGO_INSTALL_ROOT}
  )
  execute_process(
    COMMAND
      ${CMAKE_COMMAND} -E make_directory ${TILEDB_OXIDIZE_INCLUDE_DIR}/tiledb/oxidize
  )
  execute_process(
    COMMAND
      ${CARGO_INSTALL_BIN}/cxxbridge --header
    OUTPUT_FILE
      ${OXIDIZE_RUST_H_SRC}
  )

  add_custom_command(
    OUTPUT
      ${OXIDIZE_RUST_H}
    COMMAND
      ${CMAKE_COMMAND} -E copy ${OXIDIZE_RUST_H_SRC} ${OXIDIZE_RUST_H}
    WORKING_DIRECTORY
      ${CMAKE_CURRENT_BINARY_DIR}
  )

  add_custom_target(
    rust_h
    DEPENDS
      ${OXIDIZE_RUST_H}
  )



  # Declares a Rust `rlib` crate with one or more `#[cxx::bridge]` modules.
  #
  # Inputs:
  #   NAME crate_name
  #   SOURCES source1 source2 ...
  #
  # Outputs:
  #   A fake interface library target `__${CXXBRIDGE_NAME}_generated` with properties
  #   `INTERFACE_SOURCES` set to the list of supplied source files.
  #   These source files are either:
  #   - `.cc` files which reside in `${CXXBRIDGE_NAME}/cc`
  #   - `.rs` files which contain `#[cxx::bridge]` modules and thus will have `.cc` files generated
  #     as part of the build process. Each `.rs` declared should have a bridge expansion in the
  #     crate's `build.rs`.
  #
  function(cxxbridge)
    set(options)
    set(oneValueArgs NAME)
    set(multiValueArgs SOURCES)
    cmake_parse_arguments(CXXBRIDGE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    add_library(__${CXXBRIDGE_NAME}_generated INTERFACE)
    set_target_properties(__${CXXBRIDGE_NAME}_generated PROPERTIES INTERFACE_SOURCES "${CXXBRIDGE_SOURCES}")
  endfunction()

  # Declares a Rust `staticlib` which exports the bindings of one or more `cxxbridge` crates.
  #
  # Preconditions:
  #   A target `${OXIDIZE_NAME}_rs` invokes `cargo` to build the Rust crate.
  #
  # Inputs:
  #   NAME oxidize_name
  #   SOURCES cc1 cc2 rs1 rs2 ...
  #   EXPORT cxxbridge1 cxxbridge2 ...
  #
  # Outputs:
  #   A top-level target for a library `${OXIDIZE_NAME}_oxidize` which links with
  #   `${OXIDIZE_NAME}_rs` and each of the `.cc` files:
  #   - supplied directly from the SOURCES argument
  #   - generated by expanding `#[cxx::bridge]` modules in the `.rs` files in the SOURCES argument
  #   - generated by expanding the `#[cxx::bridge]` modules for each crate in the EXPORTS argument.
  #
  #   Header files are placed in `${TILEDB_OXIDIZE_INCLUDE_DIR}/${OXIDIZE_NAME}/tiledb/oxidize`
  #   which is added to the include path of downstream targets which link with this library.
  #   This enables C++ sources to use sanitized paths `#include "tiledb/oxidize/<bridge>.h"`.
  #
  function(oxidize)
    set(options)
    set(oneValueArgs NAME CRATE)
    set(multiValueArgs EXPORT SOURCES)
    cmake_parse_arguments(OXIDIZE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    if (NOT OXIDIZE_NAME)
      message(SEND_ERROR "Function 'oxidize' requires a NAME argument")
    endif ()

    if (NOT OXIDIZE_EXPORT)
      message(SEND_ERROR "Function 'oxidize' requies EXPORT argument")
    endif ()

    if (NOT OXIDIZE_CRATE)
      string(REGEX REPLACE "_" "-" OXIDIZE_CRATE ${OXIDIZE_NAME})
    endif ()

    set(OXIDIZE_STATICLIB "${OXIDIZE_NAME}_rs")
    set(OXIDIZE_INCLUDE_DIR "${TILEDB_OXIDIZE_INCLUDE_DIR}/${OXIDIZE_NAME}")

    set(OXIDIZE_H "")
    set(OXIDIZE_CC "")

    set(OXIDIZE_BRIDGE_CRATE "")
    set(OXIDIZE_BRIDGE_RS "")
    foreach (export ${OXIDIZE_EXPORT})
      get_target_property(export_RS __${export}_generated INTERFACE_SOURCES)
      foreach (source ${export_RS})
        if (${source} MATCHES "(.*).rs")
          list(APPEND OXIDIZE_BRIDGE_CRATE "${export}")
          list(APPEND OXIDIZE_BRIDGE_RS ${source})
        elseif (${source} MATCHES "(.*).cc")
          list(APPEND OXIDIZE_CC ${CMAKE_CURRENT_SOURCE_DIR}/${export}/cc/${source})
        else ()
          message(SEND_ERROR "Invalid source in cxxbridge '${export}': ${source}")
        endif ()
      endforeach ()
    endforeach ()

    foreach (bridge ${OXIDIZE_SOURCES})
      list(APPEND OXIDIZE_BRIDGE_CRATE "${OXIDIZE_CRATE}")
      list(APPEND OXIDIZE_BRIDGE_RS ${bridge})
    endforeach ()

    foreach (crate bridge IN ZIP_LISTS OXIDIZE_BRIDGE_CRATE OXIDIZE_BRIDGE_RS)
      if (${bridge} MATCHES "lib.rs")
        set(bridge_sanitize_relpath "${crate}")
      elseif (${bridge} MATCHES "(.*)\/mod.rs")
        set(bridge_sanitize_relpath "${crate}/${CMAKE_MATCH_1}")
      elseif (${bridge} MATCHES "(.*\/)*(.*).rs")
        set(bridge_sanitize_relpath "${crate}/${CMAKE_MATCH_1}/${CMAKE_MATCH_2}")
      else ()
        message(SEND_ERROR "Unable to set 'bridge_dst': unexpected pattern in bridge file path: ${bridge_h}")
      endif ()

      set(bridge_generated_dir "${CORROSION_BUILD_DIR}/cxxbridge/tiledb-${crate}")

      set(bridge_h_src "${bridge_generated_dir}/src/${bridge}.h")
      set(bridge_cc_src "${bridge_generated_dir}/src/${bridge}.cc")

      set(bridge_h_dst "${OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/${bridge_sanitize_relpath}.h")
      set(bridge_cc_dst "${OXIDIZE_INCLUDE_DIR}/tiledb/oxidize/${bridge_sanitize_relpath}.cc")
      string(REPLACE "-" "_" bridge_h_dst ${bridge_h_dst})
      string(REPLACE "-" "_" bridge_cc_dst ${bridge_cc_dst})

      cmake_path(GET bridge_h_dst PARENT_PATH bridge_sanitize_dirname)

      add_custom_command(
        OUTPUT
          "${bridge_h_dst}"
        COMMAND
          ${CMAKE_COMMAND} -E make_directory ${bridge_sanitize_dirname}
        COMMAND
          ${CMAKE_COMMAND} -E create_symlink ${bridge_h_src} ${bridge_h_dst}
        DEPENDS
          rust_h
          ${OXIDIZE_STATICLIB}
        WORKING_DIRECTORY
          ${CMAKE_CURRENT_BINARY_DIR}
      )

      add_custom_command(
        OUTPUT
          "${bridge_cc_dst}"
        COMMAND
          ${CMAKE_COMMAND} -E make_directory ${bridge_sanitize_dirname}
        COMMAND
          ${CMAKE_COMMAND} -E create_symlink ${bridge_cc_src} ${bridge_cc_dst}
        DEPENDS
          rust_h
          ${OXIDIZE_STATICLIB}
        WORKING_DIRECTORY
          ${CMAKE_CURRENT_BINARY_DIR}
      )

      list(APPEND OXIDIZE_H "${bridge_h_dst}")
      list(APPEND OXIDIZE_CC "${bridge_cc_dst}")
    endforeach ()

    add_custom_target(${OXIDIZE_NAME}_h DEPENDS ${OXIDIZE_H})
    add_custom_target(${OXIDIZE_NAME}_cc DEPENDS ${OXIDIZE_CC})

    set(OXIDIZE_LIB "${OXIDIZE_NAME}_oxidize")

    add_library(${OXIDIZE_LIB} OBJECT ${OXIDIZE_CC})
    add_dependencies(${OXIDIZE_LIB} ${OXIDIZE_NAME}_h rust_h ${OXIDIZE_NAME}_cc ${OXIDIZE_STATICLIB})
    target_include_directories(${OXIDIZE_LIB} PUBLIC ${CMAKE_SOURCE_DIR} ${TILEDB_OXIDIZE_INCLUDE_DIR} ${OXIDIZE_INCLUDE_DIR})
    target_link_libraries(${OXIDIZE_LIB} PUBLIC assert_header ${OXIDIZE_STATICLIB})
  endfunction ()



  # `make clean` cleans up all cargo output and sanitized include paths
  set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${CARGO_INCLUDE_DIR} ${TILEDB_OXIDIZE_INCLUDE_DIR})


else ()
  #
  # Declare top-level targets which are empty INTERFACE libraries.
  # This allows targets to declare linkage without needing an `if (TILEDB_RUST)`.
  #
  function(cxxbridge)
  endfunction ()

  function(oxidize)
    set(options)
    set(oneValueArgs NAME)
    set(multiValueArgs EXPORT SOURCES)
    cmake_parse_arguments(OXIDIZE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    set(OXIDIZE_LIB "${OXIDIZE_NAME}_oxidize")
    add_library(${OXIDIZE_LIB} INTERFACE)
  endfunction()


endif()
