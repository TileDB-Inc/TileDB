#
# cmake/Modules/Sanitizers.cmake
#
# The MIT License
#
# Copyright (c) 2023 TileDB, Inc.
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
# Sanitizers
#   https://github.com/google/sanitizers
#   https://github.com/google/sanitizers/wiki/AddressSanitizer
#
# GCC
#   all: https://gcc.gnu.org/onlinedocs/gcc/Instrumentation-Options.html
#   All the sanitizer options are here, together with all the similar options gcc offers.
#
# Clang
#   address: https://clang.llvm.org/docs/AddressSanitizer.html
#       Recommends linking with `clang++`. No warning if otherwise at this time.
#   undefined behavior: https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html
#   memory: https://clang.llvm.org/docs/MemorySanitizer.html
#
# MSVC
#   Only supports the address sanitizer
#   https://docs.microsoft.com/en-us/cpp/sanitizers/asan
#   https://docs.microsoft.com/en-us/cpp/sanitizers/asan-building
#   https://devblogs.microsoft.com/cppblog/addresssanitizer-asan-for-windows-with-msvc/
#   https://devblogs.microsoft.com/cppblog/address-sanitizer-for-msvc-now-generally-available/

if(NOT TILEDB_SANITIZER)
    return()
endif()

# The basic sanitizer option is standard across compilers
add_compile_options(-fsanitize=${TILEDB_SANITIZER})

# Check that the sanitizer name is well-formed
if (NOT TILEDB_SANITIZER MATCHES "^[-A-Za-z]*$")
    message(FATAL_ERROR "Bad sanitizer specification \"${sanitizer}\";"
            " permissible characters are only alphabetic and hyphen")
endif()

# Verify that the sanitizer is one that some compiler supports
string(TOLOWER ${TILEDB_SANITIZER} TILEDB_SANITIZER)
if (NOT TILEDB_SANITIZER MATCHES "^address$")
    message(FATAL_ERROR "Unsupported sanitizer ${sanitizer}")
endif()

# Catch has a conflict with ASAN on Windows. Disable the SEH handler in Catch to avoid the conflict.
add_compile_definitions("$<$<CXX_COMPILER_ID:MSVC>:CATCH_CONFIG_NO_WINDOWS_SEH>")
# Microsoft suppresses /INCREMENTAL, but emits a warning, so silence it.
add_link_options("$<$<CXX_COMPILER_ID:MSVC>:/INCREMENTAL:NO>")

# Ordinary gcc/clang behavior.
add_compile_options("$<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-g;-fno-omit-frame-pointer;-fno-optimize-sibling-calls>")
add_link_options("$<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-fsanitize=${TILEDB_SANITIZER}>")
if(TILEDB_SANITIZER STREQUAL "address")
    # There may be problems if clang tries to link the ASAN library statically
    add_link_options("$<$<CXX_COMPILER_ID:Clang>:-shared-libasan>")
endif()

# Validate sanitizer options.
# This must be called after the project() command, where the compiler is known.
macro(validate_sanitizer_options)
    # For known compilers, check that the sanitizer is supported.
    # If we know it's not supported, we'll fail so that we avoid false confidence.
    # If we don't know, we'll warn that it might not work.
    if (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
        if (TILEDB_SANITIZER STREQUAL "address")
            # MSVC support for the address sanitizer began with Visual Studio 2019 Version 16.4
            # and was announced as "fully supported" in version 16.9
            if (MSVC_VERSION LESS 1924)
                message(FATAL_ERROR "MSVC version ${MSVC_VERSION} too early to support address sanitizer." )
            endif()
            if (MSVC_VERSION LESS 1929)
                message(WARNING "MSVC version ${MSVC_VERSION} may only partially support address sanitizer." )
            endif()
        else()
            # MSVC support only the address sanitizer
            message(FATAL_ERROR "MSVC only supports sanitizer \"address\"")
        endif()

        # May also need to explicitly remove /RTC flags

    elseif (NOT (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU"))
        message(WARN "Compiler \"${CMAKE_CXX_COMPILER_ID}\" not explicitly supported; behaving as if GNU")
    endif()
endmacro()
