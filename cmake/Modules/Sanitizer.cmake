#
# cmake/Modules/Sanitizers.cmake
#
# The MIT License
#
# Copyright (c) 2021 TileDB, Inc.
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

include_guard()

#
# target_sanitizer: compile and link a target with a single sanitizer
#
# Syntax mimics that of CMake project commands `target_compile_features` etc.
# Sanitization requires compile and link options to be set, so its name is a
# bit different.
#
# Arguments
#   the_target: CMake target
#   scope: PUBLIC INTERFACE PRIVATE, passed through for all option-setting
#   sanitizer: a single sanitizer use on the target
#
function(target_sanitizer the_target scope sanitizer)
    # The basic sanitizer option is standard across compilers
    target_compile_options(${the_target} ${scope} -fsanitize=${SANITIZER})

    # Check that the sanitizer name is well-formed
    if (NOT sanitizer MATCHES "^[-A-Za-z]*$")
        message(FATAL_ERROR "target_sanitizer: bad sanitizer specification \"${sanitizer}\";"
                " permissible characters are only alphabetic and hyphen")
    endif()

    # Verify that the sanitizer is one that some compiler supports
    string(TOLOWER ${sanitizer} sanitizer)
    if (NOT sanitizer MATCHES "^(address|memory|leak|thread|undefined)$")
        message(FATAL_ERROR "target_sanitizer: unsupported sanitizer ${sanitizer}")
    endif()

    # Validate the scope
    if (NOT scope MATCHES "^(PUBLIC|PRIVATE|INTERFACE)$")
        message(FATAL_ERROR "target_sanitizer: scope \"${scope}\" is not one of PUBLIC, PRIVATE, or INTERFACE.")
    endif()

    # For known compilers, check that the sanitizer is supported.
    # If we know it's not supported, we'll fail so that we avoid false confidence.
    # If we don't know, we'll warn that it might not work.
    if (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
        if (sanitizer MATCHES "^address$")
            # MSVC support for the address sanitizer began with Visual Studio 2019 Version 16.4
            # and was announced as "fully supported" in version 16.9
            if (MSVC_VERSION LESS 1924)
                message(FATAL_ERROR "MSVC version ${MSVC_VERSION} too early to support address sanitizer." )
            endif()
            if (MSVC_VERSION LESS 1929)
                message(WARNING "MSVC version ${MSVC_VERSION} may only partially support address sanitizer." )
            endif()
            # Catch has a conflict with ASAN on Windows. Disable the SEH handler in Catch to avoid the conflict.
            target_compile_definitions(${the_target} ${scope} CATCH_CONFIG_NO_WINDOWS_SEH)
        else()
            # MSVC support only the address sanitizer
            message(FATAL_ERROR "MSVC only supports sanitizer \"address\"")
        endif()
        # Certain compile options are incompatible with ASAN
        # Microsoft suppresses /INCREMENTAL, but emits a warning, so silence it.
        target_link_options(${the_target} ${scope} /INCREMENTAL:NO)

        # May also need to explicitly remove /RTC flags

    elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")  # also matches AppleClang, ARMClang, etc.
        # Ordinary gcc behavior. Factor this out into a subroutine when we need more than twice.
        target_compile_options(${the_target} ${scope} -g -fno-omit-frame-pointer -fno-optimize-sibling-calls)

        # Clang recommends a linker flag as well as a compiler flag
        target_link_options(${the_target} ${scope} -fsanitize=${SANITIZER})
        if (sanitizer MATCHES "^address$")
            # There may be problems if clang tries to link the ASAN library statically
            target_link_options(${the_target} ${scope} -shared-libasan)
        endif()

    elseif (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
        # Ordinary gcc behavior
        target_compile_options(${the_target} ${scope} -g -fno-omit-frame-pointer -fno-optimize-sibling-calls)

    else()
        message(WARN "Compiler \"${CMAKE_CXX_COMPILER_ID}\" not explicitly supported; behaving as if GNU")
    endif()
endfunction()

#
# target_sanitizer_options
#
# There are a number of options for the various sanitizers, notably about error recovery
# and trapping on error.
#
# Limitations: Unimplemented at the present time. For future use.
#
function(target_sanitizer_options the_target sanitizer options)
    message(FATAL_ERROR "target_sanitizer_options: not yet implemented")
endfunction()
