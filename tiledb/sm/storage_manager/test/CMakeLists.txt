#
# tiledb/sm/storage_manager/test/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2023-2024 TileDB, Inc.
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

include(unit_test)

#
# Test runner `unit_context`
#
commence(unit_test context)
    this_target_sources(unit_job.cc)
    #
    # Temporary substitute for a `job` object library
    #
    this_target_sources(
        ../cancellation_source.cc
        ../context.cc
        ../job.cc
    )
    this_target_object_libraries(context_resources)
    # This is defined in c_api_test_support, but we're using it temporarily here
    # in order to test the code that will allow the stub to go away
    this_target_object_libraries(storage_manager_stub)
conclude(unit_test)
