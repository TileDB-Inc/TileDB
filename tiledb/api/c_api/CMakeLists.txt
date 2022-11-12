#
# tiledb/api/c_api/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2022 TileDB, Inc.
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

include(common NO_POLICY_SCOPE)

#
# The `export` library is a required precursor for C API functions.
#   - Include path for `tiledb_export.h`, required in `api/c_api/external_common.h`
#   - Include path for `c_api` headers
#
add_library(export INTERFACE)
target_compile_definitions(export INTERFACE -DTILEDB_CORE_OBJECTS_EXPORTS)
target_include_directories(export INTERFACE ${TILEDB_EXPORT_HEADER_DIR})
target_include_directories(export INTERFACE ${TILEDB_SOURCE_ROOT}/tiledb/sm/c_api)

#
# Subdirectories
#
# We need to include directories in an order compatible with their dependency
# relationships. The comments below are all the explicit dependencies. The
# transitive dependencies generally contain redundancies with the explicit ones;
# we do not take that as an excuse to omit them.
#
# Dependency Rule: Section `A` depends on section `B` if an API call in section
# `A` uses a handle defined in section `B`. For example, most of the C API
# depends on `context`, as context handles are often the first argument.
#

# `error`: no dependencies
add_subdirectory(error)

# `config`: depends on `error`
add_subdirectory(config)

# `context`: depends on `config`
add_subdirectory(context)

# `filter`: depends on `context`
add_subdirectory(filter)

# `filter_list`: depends on `context`, `filter`
add_subdirectory(filter_list)