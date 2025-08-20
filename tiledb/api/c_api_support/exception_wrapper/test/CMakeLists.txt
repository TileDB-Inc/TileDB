#
# tiledb/api/c_api_support/exception_wrapper/test/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2022-2023 TileDB, Inc.
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
include(unit_test)

commence(unit_test capi_exception_wrapper)
  this_target_sources(
      unit_capi_exception_wrapper.cc
      unit_capi_error_tree.cc)
  this_target_object_libraries(
      # The exception wrapper is wrapped up with the context, so that's the
      # top-level dependency required for the test.
      capi_context_stub)
conclude(unit_test)

#
# Hook Tests
#
# These following two unit test runners are a pair, one compiled without an API
# hook compiled in, one without. They run the same test, although with different
# expectations about the results. The hook is specified in two different ways:
# * In the ordinary way, with an additional include directory
# * Specifically for testing, with a compile definition.
# The two different ways are used to verify that the hook is activated or not
# as appropriate.
#
# Changing the API hook changes the API object libraries. The API hook is
# perforce a low-level mechanism. It changes the definition of C API calls. The
# exception handler is intertwined with a few of the API classes, notably the
# handles for context and error (and config gets pulled in as a dependency). As
# a result, whereas the without-hook test runner can use an existing object
# library, the with-hook ones needs to recompile an equivalent.
#

# Hook tests without the hook
commence(unit_test capi_ew_without_hook)
  this_target_sources(
      unit_capi_hook.cc
      unit_capi_hook_without.cc)
  this_target_object_libraries(
      capi_context_stub)
conclude(unit_test)

# Hook tests with the hook included.
function (hook_test hookdef)
  commence(unit_test capi_ew_with_${hookdef})
    this_target_sources(
        unit_capi_hook.cc
        unit_capi_hook_with_${hookdef}.cc
        ../../../c_api/config/config_api.cc
        ../../../c_api/context/context_api.cc
        ../../../c_api/error/error_api.cc
        ../../../../sm/storage_manager/context.cc
        ../../../../sm/storage_manager/context_resources.cc
    )
    this_target_link_libraries(export)
    this_target_object_libraries(
        exception_wrapper
        rest_client
        storage_manager_stub
        vfs
    )
    target_compile_options(unit_capi_ew_with_${hookdef} PUBLIC "-DWITH_HOOK=\"${hookdef}\"")
    target_include_directories(
        unit_capi_ew_with_${hookdef} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/hook)
  conclude(unit_test)
endfunction ()

hook_test(logger)
hook_test(tracer)
