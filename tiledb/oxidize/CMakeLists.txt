find_program(CARGO cargo)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_PROFILE "dev")
    set(CARGO_TARGET "debug")
else ()
    set(CARGO_PROFILE "release")
    set(CARGO_TARGET "release")
endif ()

message(NOTICE "CARGO_PROFILE: ${CARGO_PROFILE}")
message(NOTICE "CARGO_TARGET: ${CARGO_TARGET}")

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
    set(multiValueArgs SOURCES)
    cmake_parse_arguments(ADD_CRATE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    if (NOT ADD_CRATE_NAME)
        message(SEND_ERROR "Function add_crate requires a NAME argument")
    endif()

    if (NOT ADD_CRATE_SOURCES)
        message(SEND_ERROR "Function add_crate requires one or more SOURCES arguments")
    endif()

    add_custom_command(
        OUTPUT
            "${CMAKE_CURRENT_BINARY_DIR}/${ADD_CRATE_NAME}.h"
            "${CARGO_INCLUDE_DIR}/cxxbridge/${ADD_CRATE_NAME}/src/lib.rs.cc"
            "${CARGO_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${ADD_CRATE_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
        COMMAND
            ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target"
            ${CARGO}
            build
            "--quiet"
            "--profile" ${CARGO_PROFILE}
            "--manifest-path" ${CARGO_MANIFEST_PATH}
            "-p" ${ADD_CRATE_NAME}
        COMMAND
            ${CMAKE_COMMAND} -E copy "${CARGO_INCLUDE_DIR}/cxxbridge/${ADD_CRATE_NAME}/src/lib.rs.h" "${CMAKE_CURRENT_BINARY_DIR}/${ADD_CRATE_NAME}.h"
        USES_TERMINAL
        DEPENDS
            ${ADD_CRATE_SOURCES}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )

    if (TILEDB_RUST)
        #
        # Identify system static libs to link with.
        #
        execute_process(
            COMMAND
                ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}/target"
                ${CARGO}
                rustc
                --profile ${CARGO_PROFILE}
                -p ${ADD_CRATE_NAME}
                --color never
                --
                --print native-static-libs
            WORKING_DIRECTORY
                ${CMAKE_CURRENT_SOURCE_DIR}
            RESULT_VARIABLE
                ADD_CRATE_STATIC_LIBS_STDOUT
            ERROR_VARIABLE
                ADD_CRATE_STATIC_LIBS_STDERR
        )
        set(ADD_CRATE_STATIC_LIBS_LIST "")

        if (ADD_CRATE_STATIC_LIBS_STOUT)
            message(FATAL_ERROR "Unable to identify native-static-libs for crate ${ADD_CRATE_NAME}: ${ADD_CRATE_STATIC_LIBS_STDOUT}")
            message(FATAL_ERROR "Note: stderr was: ${ADD_CRATE_STATIC_LIBS_STDERR}")
            return ()
        else ()
            if (ADD_CRATE_STATIC_LIBS_STDERR MATCHES "note: native-static-libs: ([^\r\n]+)\r?\n")
                string(REPLACE " " ";" ADD_CRATE_STATIC_LIBS_LINK_FLAGS ${CMAKE_MATCH_1})

                message(NOTICE "${ADD_CRATE_NAME}_rs match: ${CMAKE_MATCH_1}")
                message(NOTICE "${ADD_CRATE_NAME}_rs replace: ${ADD_CRATE_STATIC_LIBS_LINK_FLAGS}")

                foreach (ADD_CRATE_STATIC_LIB_LINK_FLAG ${ADD_CRATE_STATIC_LIBS_LINK_FLAGS})
                    string(REGEX REPLACE "^-l" "" ADD_CRATE_STATIC_LIB ${ADD_CRATE_STATIC_LIB_LINK_FLAG})
                    list(APPEND ADD_CRATE_STATIC_LIBS_LIST ${ADD_CRATE_STATIC_LIB})
                endforeach ()
            else ()
                message(FATAL_ERROR "Unable to identify native-static-libs for crate ${ADD_CRATE_NAME}: regex match failure: found stdout=${ADD_CRATE_STATIC_LIBS_STDOUT}, stderr=${ADD_CRATE_STATIC_LIBS_STDERR}")
                return ()
            endif ()
        endif ()

        # https://github.com/corrosion-rs/corrosion/blob/715c235daef4b8ee67278f12256334ad3dd4c4ae/cmake/FindRust.cmake
        list(FILTER ADD_CRATE_STATIC_LIBS_LIST EXCLUDE REGEX "msvcrt")

        message(NOTICE "${ADD_CRATE_NAME}_rs native-static-libs: ${ADD_CRATE_STATIC_LIBS_STDERR}")
        message(NOTICE "${ADD_CRATE_NAME}_rs link libraries: ${ADD_CRATE_STATIC_LIBS_LIST}")
    endif ()

    # Note: Object libraries are "{crate_name}_rs" to avoid clashing with the
    # existing object libraries.
    add_library(${ADD_CRATE_NAME}_rs OBJECT EXCLUDE_FROM_ALL "${CARGO_INCLUDE_DIR}/cxxbridge/${ADD_CRATE_NAME}/src/lib.rs.cc")
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
        ${ADD_CRATE_STATIC_LIBS_LIST}
    )
endfunction ()


# and now, the crates themselves!

add_crate(
    NAME arithmetic
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/arithmetic/Cargo.toml ${CMAKE_CURRENT_SOURCE_DIR}/arithmetic/src/lib.rs
)

set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${CARGO_INCLUDE_DIR} *.h)
