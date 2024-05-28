#
# tiledb/sm/array_schema/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2021-2024 TileDB, Inc.
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
include(object_library)

#
# `attribute` object library
#
commence(object_library attribute)
    this_target_sources(attribute.cc)
    this_target_object_libraries(
        baseline
        buffer
        constants
        filter_pipeline
        range
        stringx)
conclude(object_library)

#
# `dimension` object library
#
commence(object_library dimension)
    this_target_sources(dimension.cc)
    this_target_object_libraries(baseline filter_pipeline misc_types range)
conclude(object_library)

#
# `domain` object library
#
commence(object_library domain)
    this_target_sources(domain.cc)
    this_target_object_libraries(datum dimension math)
conclude(object_library)

#
# `enumeration` object library
#
commence(object_library enumeration)
    this_target_sources(enumeration.cc)
    this_target_object_libraries(buffer constants seedable_global_PRNG)
conclude(object_library)

#
# `array_schema` object library
#
commence(object_library array_schema)
    this_target_sources(array_schema.cc dimension_label.cc)
    this_target_object_libraries(
        attribute domain enumeration fragment generic_tile_io time uri_format vfs)
conclude(object_library)

add_test_subdirectory()
