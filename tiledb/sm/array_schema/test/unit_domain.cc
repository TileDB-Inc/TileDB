/**
 * @file unit_domain.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Tests the  `Domain` class.
 */

#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"

#include <catch2/catch_test_macros.hpp>

#include "src/mem_helpers.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::test;

template <class T, int n>
inline T& dom_buffer_offset(void* p) {
  return *static_cast<T*>(static_cast<void*>(static_cast<char*>(p) + n));
}

TEST_CASE("Domain: Test deserialization", "[domain][deserialize]") {
  char serialized_buffer[72];
  char* p = &serialized_buffer[0];

  // number of dimensions
  uint32_t dim_num = 2;

  // dimension1
  std::string dimension_name1 = "d1";
  uint32_t dimension_name_size1 = static_cast<uint32_t>(dimension_name1.size());
  Datatype type1 = Datatype::INT32;
  uint8_t datatype1 = static_cast<uint8_t>(type1);
  uint32_t cell_val_num1 = 1;
  uint32_t max_chunk_size1 = constants::max_tile_chunk_size;
  uint32_t num_filters1 = 0;
  // domain and tile extent
  uint64_t domain_size1 = 2 * datatype_size(type1);
  uint8_t null_tile_extent1 = 0;
  int32_t tile_extent1 = 16;

  // dimension2
  std::string dimension_name2 = "d2";
  uint32_t dimension_name_size2 = static_cast<uint32_t>(dimension_name2.size());
  Datatype type2 = Datatype::STRING_ASCII;
  uint8_t datatype2 = static_cast<uint8_t>(type2);
  uint32_t cell_val_num2 = constants::var_num;
  uint32_t max_chunk_size2 = constants::max_tile_chunk_size;
  uint32_t num_filters2 = 0;
  // domain and tile extent
  uint64_t domain_size2 = 0;
  uint8_t null_tile_extent2 = 1;

  // set dim_num
  dom_buffer_offset<uint32_t, 0>(p) = dim_num;

  // set dimension1 buffer
  dom_buffer_offset<uint32_t, 4>(p) = dimension_name_size1;
  std::memcpy(
      &dom_buffer_offset<char, 8>(p),
      dimension_name1.c_str(),
      dimension_name_size1);
  dom_buffer_offset<uint8_t, 10>(p) = datatype1;
  dom_buffer_offset<uint32_t, 11>(p) = cell_val_num1;
  dom_buffer_offset<uint32_t, 15>(p) = max_chunk_size1;
  dom_buffer_offset<uint32_t, 19>(p) = num_filters1;
  dom_buffer_offset<uint64_t, 23>(p) = domain_size1;
  dom_buffer_offset<int32_t, 31>(p) = 1;
  dom_buffer_offset<int32_t, 35>(p) = 100;
  dom_buffer_offset<uint8_t, 39>(p) = null_tile_extent1;
  dom_buffer_offset<int32_t, 40>(p) = tile_extent1;

  // set dimension2 buffer
  dom_buffer_offset<uint32_t, 44>(p) = dimension_name_size2;
  std::memcpy(
      &dom_buffer_offset<char, 48>(p),
      dimension_name2.c_str(),
      dimension_name_size2);
  dom_buffer_offset<uint8_t, 50>(p) = datatype2;
  dom_buffer_offset<uint32_t, 51>(p) = cell_val_num2;
  dom_buffer_offset<uint32_t, 55>(p) = max_chunk_size2;
  dom_buffer_offset<uint32_t, 59>(p) = num_filters2;
  dom_buffer_offset<uint64_t, 63>(p) = domain_size2;
  dom_buffer_offset<uint8_t, 71>(p) = null_tile_extent2;

  Deserializer deserializer(&serialized_buffer, sizeof(serialized_buffer));
  FilterPipeline fp;
  auto dom{tiledb::sm::Domain::deserialize(
      deserializer,
      10,
      Layout::ROW_MAJOR,
      Layout::ROW_MAJOR,
      fp,
      tiledb::test::get_test_memory_tracker())};
  CHECK(dom->dim_num() == dim_num);

  auto dim1{dom->dimension_ptr("d1")};
  CHECK(dim1->name() == dimension_name1);
  CHECK(dim1->type() == type1);
  CHECK(dim1->cell_val_num() == cell_val_num1);
  CHECK(dim1->filters().size() == num_filters1);

  auto dim2{dom->dimension_ptr("d2")};
  CHECK(dim2->name() == dimension_name2);
  CHECK(dim2->type() == type2);
  CHECK(dim2->cell_val_num() == cell_val_num2);
  CHECK(dim2->filters().size() == num_filters2);
}

TEST_CASE("Domain: Test dimension_ptr is not oob", "[domain][oob]") {
  auto d = tiledb::sm::Domain(tiledb::test::get_test_memory_tracker());
  REQUIRE_THROWS(d.dimension_ptr(0));
}
