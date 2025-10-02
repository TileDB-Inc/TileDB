#
# tiledb/sm/serialization/test/CMakeLists.txt
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

include(unit_test)

commence(unit_test capnp_utils)
  this_target_sources(main.cc unit_capnp_utils.cc)
  this_target_link_libraries(TILEDB_CORE_OBJECTS TILEDB_CORE_OBJECTS_ILIB)

  # Enable serialization
  target_compile_definitions(unit_capnp_utils PRIVATE -DTILEDB_SERIALIZATION)
conclude(unit_test)

commence(unit_test capnp_array_schema)
  this_target_sources(main.cc unit_capnp_array_schema.cc)
  this_target_link_libraries(TILEDB_CORE_OBJECTS TILEDB_CORE_OBJECTS_ILIB tiledb_test_support_lib tiledb_core_objects_oxidize)

  # Enable serialization
  target_compile_definitions(unit_capnp_array_schema PRIVATE -DTILEDB_SERIALIZATION)
  this_target_link_libraries(tiledb_test_support_lib)
conclude(unit_test)

commence(unit_test capnp_nonempty_domain)
  this_target_sources(main.cc unit_capnp_nonempty_domain.cc)
  this_target_link_libraries(tiledb_test_support_lib)

  # Enable serialization
  target_compile_definitions(unit_capnp_nonempty_domain PRIVATE -DTILEDB_SERIALIZATION)
conclude(unit_test)

commence(unit_test capnp_group)
  this_target_sources(main.cc unit_capnp_group.cc)
  this_target_link_libraries(TILEDB_CORE_OBJECTS TILEDB_CORE_OBJECTS_ILIB tiledb_test_support_lib tiledb_core_objects_oxidize)

  # Enable serialization
  target_compile_definitions(unit_capnp_group PRIVATE -DTILEDB_SERIALIZATION)
conclude(unit_test)
