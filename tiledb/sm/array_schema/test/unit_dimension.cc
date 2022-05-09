/**
 * @file tiledb/sm/array_schema/test/unit_dimension.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file tests the Dimension class
 */

#include <catch.hpp>
#include "../dimension.h"
#include "tiledb/sm/enums/datatype.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

template <class T, int n>
inline T& dim_buffer_offset(void* p) {
  return *static_cast<T*>(static_cast<void*>(static_cast<char*>(p) + n));
}

TEST_CASE("Dimension::Dimension") {
  Dimension x{"", Datatype::UINT32};
}

TEST_CASE("Dimension: Test deserialize,int32", "[dimension][deserialize]") {
  char serialized_buffer[40];
  char* p = &serialized_buffer[0];
  uint32_t dimension_name_size = 2;
  std::string dimension_name = "d1";
  uint8_t datatype = static_cast<uint8_t>(Datatype::INT32);
  uint32_t cell_val_num =
      (datatype_is_string(Datatype::INT32)) ? constants::var_num : 1;
  uint32_t max_chunk_size = constants::max_tile_chunk_size;
  uint32_t num_filters = 0;
  // domain and tile extent
  uint64_t domain_size = 2 * datatype_size(Datatype::INT32);
  uint8_t null_tile_extent = 0;
  int32_t tile_extent = 16;

  dim_buffer_offset<uint32_t, 0>(p) = dimension_name_size;
  std::memcpy(
      &dim_buffer_offset<char, 4>(p),
      dimension_name.c_str(),
      dimension_name_size);
  dim_buffer_offset<uint8_t, 6>(p) = datatype;
  dim_buffer_offset<uint32_t, 7>(p) = cell_val_num;
  dim_buffer_offset<uint32_t, 11>(p) = max_chunk_size;
  dim_buffer_offset<uint32_t, 15>(p) = num_filters;
  dim_buffer_offset<uint64_t, 19>(p) = domain_size;
  dim_buffer_offset<int32_t, 27>(p) = 1;
  dim_buffer_offset<int32_t, 31>(p) = 100;
  dim_buffer_offset<uint8_t, 35>(p) = null_tile_extent;
  dim_buffer_offset<int32_t, 36>(p) = tile_extent;

  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_dim, dim]{
      Dimension::deserialize(&constbuffer, 10, Datatype::INT32)};

  REQUIRE(st_dim.ok());

  // Check name
  CHECK(dim.value()->name() == dimension_name);

  // Check type
  CHECK(dim.value()->type() == Datatype::INT32);

  CHECK(dim.value()->cell_val_num() == 1);
  CHECK(dim.value()->var_size() == false);
}

TEST_CASE("Dimension: Test deserialize,string", "[dimension][deserialize]") {
  char serialized_buffer[28];
  char* p = &serialized_buffer[0];
  uint32_t dimension_name_size = 2;
  std::string dimension_name = "d1";
  Datatype type = Datatype::STRING_ASCII;
  uint8_t datatype = static_cast<uint8_t>(type);
  uint32_t cell_val_num = (datatype_is_string(type)) ? constants::var_num : 1;
  uint32_t max_chunk_size = constants::max_tile_chunk_size;
  uint32_t num_filters = 0;
  // domain and tile extent
  uint64_t domain_size = 0;
  uint8_t null_tile_extent = 1;

  dim_buffer_offset<uint32_t, 0>(p) = dimension_name_size;
  std::memcpy(
      &dim_buffer_offset<char, 4>(p),
      dimension_name.c_str(),
      dimension_name_size);
  dim_buffer_offset<uint8_t, 6>(p) = datatype;
  dim_buffer_offset<uint32_t, 7>(p) = cell_val_num;
  dim_buffer_offset<uint32_t, 11>(p) = max_chunk_size;
  dim_buffer_offset<uint32_t, 15>(p) = num_filters;
  dim_buffer_offset<uint64_t, 19>(p) = domain_size;
  dim_buffer_offset<uint8_t, 27>(p) = null_tile_extent;

  ConstBuffer constbuffer(&serialized_buffer, sizeof(serialized_buffer));
  auto&& [st_dim, dim]{
      Dimension::deserialize(&constbuffer, 10, Datatype::INT32)};
  REQUIRE(st_dim.ok());
  // Check name
  CHECK(dim.value()->name() == dimension_name);
  // Check type
  CHECK(dim.value()->type() == type);
  CHECK(dim.value()->cell_val_num() == constants::var_num);
  CHECK(dim.value()->var_size() == true);
}

typedef tuple<
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t>
    IntegralTypes;
TEMPLATE_LIST_TEST_CASE(
    "test max tile extent for integer values",
    "[dimension][integral][max][tile_extent]",
    IntegralTypes) {
  typedef TestType T;
  T min = std::numeric_limits<T>::min() + std::is_signed<T>::value;
  T max = std::numeric_limits<T>::max() - !std::is_signed<T>::value;

  typedef typename std::make_unsigned<T>::type tile_extent_t;
  T max_extent = (tile_extent_t)max - (tile_extent_t)min + 1;

  auto tile_idx = Dimension::tile_idx(max, min, max_extent);

  CHECK(Dimension::tile_coord_low(tile_idx, min, max_extent) == min);
  CHECK(Dimension::tile_coord_high(tile_idx, min, max_extent) == max);

  CHECK(Dimension::round_to_tile(min, min, max_extent) == min);
  CHECK(Dimension::round_to_tile(max, min, max_extent) == min);
}

TEMPLATE_LIST_TEST_CASE(
    "test min tile extent for integer values",
    "[dimension][integral][min][tile_extent]",
    IntegralTypes) {
  typedef TestType T;
  T min = std::numeric_limits<T>::min();
  T max = std::numeric_limits<T>::max();

  T min_extent = 1;

  auto tile_idx = Dimension::tile_idx(max, min, min_extent);

  CHECK(Dimension::tile_coord_low(0, min, min_extent) == min);
  CHECK(Dimension::tile_coord_high(tile_idx, min, min_extent) == max);

  CHECK(Dimension::round_to_tile(min, min, min_extent) == min);
  CHECK(Dimension::round_to_tile(max, min, min_extent) == max);
}

typedef tuple<int8_t, int16_t, int32_t, int64_t> SignedIntegralTypes;
TEMPLATE_LIST_TEST_CASE(
    "test tile_idx signed",
    "[dimension][integral][signed][tile_idx]",
    SignedIntegralTypes) {
  typedef TestType T;
  for (T tile_extent = 5; tile_extent < 10; tile_extent++) {
    for (T domain_low = -50; domain_low < 50; domain_low++) {
      for (T val = domain_low; val < domain_low + 50; val++) {
        uint64_t idx = static_cast<int64_t>(val - domain_low) /
                       static_cast<int64_t>(tile_extent);
        CHECK(Dimension::tile_idx(val, domain_low, tile_extent) == idx);
      }
    }
  }
}

typedef tuple<uint8_t, uint16_t, uint32_t, uint64_t> UnsignedIntegralTypes;
TEMPLATE_LIST_TEST_CASE(
    "test tile_idx unsigned",
    "[dimension][integral][unsigned][tile_idx]",
    UnsignedIntegralTypes) {
  typedef TestType T;
  for (T tile_extent = 5; tile_extent < 10; tile_extent++) {
    for (T domain_low = 0; domain_low < 100; domain_low++) {
      for (T val = domain_low; val < domain_low + 100; val++) {
        uint64_t idx = static_cast<uint64_t>(val - domain_low) /
                       static_cast<uint64_t>(tile_extent);
        CHECK(Dimension::tile_idx(val, domain_low, tile_extent) == idx);
      }
    }
  }
}
