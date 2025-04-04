#
# tiledb/api/c_api/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2022-2025 TileDB, Inc.
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
target_compile_definitions(export INTERFACE -Dtiledb_EXPORTS)
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

# `array`: no dependencies
add_subdirectory(array)

# `array_schema`: depends on `context`
add_subdirectory(array_schema)

# `buffer`: no dependencies
add_subdirectory(buffer)

# `buffer_list`: depends on `buffer`
add_subdirectory(buffer_list)

# `current_domain`: depends on `ndrectangle`
add_subdirectory(current_domain)

# `error`: no dependencies
add_subdirectory(error)

# `config`: depends on `error`
add_subdirectory(config)

# `context`: depends on `config`
add_subdirectory(context)

# `datatype`: no dependencies
add_subdirectory(datatype)

# `data_order`: no dependencies
add_subdirectory(data_order)

# `dimension_label`: depends on `context`, `datatype`, `data_order`
add_subdirectory(dimension_label)

# `enumeration`: depends on `buffer`, `constants`, and `context`
add_subdirectory(enumeration)

# `filesystem`: no dependencies
add_subdirectory(filesystem)

# `filter`: depends on `context`
add_subdirectory(filter)

# `filter_list`: depends on `context`, `filter`
add_subdirectory(filter_list)

# `fragment_info`: depends on `context`
add_subdirectory(fragment_info)

# `dimension`: depends on `filter_list`, `datatype`
add_subdirectory(dimension)

# `domain`: depends on `dimension`
add_subdirectory(domain)

# `attribute`: depends on `filter_list`, `datatype`
add_subdirectory(attribute)

# `group`: depends on `config`, `context`
add_subdirectory(group)

# `ndrectangle`: depends on `domain`
add_subdirectory(ndrectangle)

# `object`: no dependencies
add_subdirectory(object)

# NOTE: Only the tiledb_query_type_t enum has been migrated
# to the new C API layout.
#
# `query`: no dependencies yet but will have some
add_subdirectory(query)

# `rest`: no dependencies
add_subdirectory(rest)

# `subarray`: depends on `context`
add_subdirectory(subarray)

# `string`: no dependencies
add_subdirectory(string)

# `vfs`: depends on `context`
add_subdirectory(vfs)

# `query_plan`: no dependencies yet
add_subdirectory(query_plan)

# `query_aggregate`: no dependencies
add_subdirectory(query_aggregate)

# `query_field`: no dependencies
add_subdirectory(query_field)

