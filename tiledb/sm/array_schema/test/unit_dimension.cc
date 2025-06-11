/**
 * @file tiledb/sm/array_schema/test/unit_dimension.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2024 TileDB, Inc.
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

#include <test/support/tdb_catch.h>
#include "../dimension.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/sm/enums/datatype.h"

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

using Datatype = tiledb::sm::Datatype;

template <class T>
struct type_to_datatype {
  static Datatype datatype;
};

template <>
struct type_to_datatype<int8_t> {
  static constexpr Datatype datatype = Datatype::INT8;
};

template <>
struct type_to_datatype<int16_t> {
  static constexpr Datatype datatype = Datatype::INT16;
};

template <>
struct type_to_datatype<int32_t> {
  static constexpr Datatype datatype = Datatype::INT32;
};

template <>
struct type_to_datatype<int64_t> {
  static constexpr Datatype datatype = Datatype::INT64;
};

template <>
struct type_to_datatype<uint8_t> {
  static constexpr Datatype datatype = Datatype::UINT8;
};

template <>
struct type_to_datatype<uint16_t> {
  static constexpr Datatype datatype = Datatype::UINT16;
};

template <>
struct type_to_datatype<uint32_t> {
  static constexpr Datatype datatype = Datatype::UINT32;
};

template <>
struct type_to_datatype<uint64_t> {
  static constexpr Datatype datatype = Datatype::UINT64;
};

template <>
struct type_to_datatype<float> {
  static constexpr Datatype datatype = Datatype::FLOAT32;
};

template <>
struct type_to_datatype<double> {
  static constexpr Datatype datatype = Datatype::FLOAT64;
};

template <class T, int n>
inline T& dim_buffer_offset(void* p) {
  return *static_cast<T*>(static_cast<void*>(static_cast<char*>(p) + n));
}

TEST_CASE("Dimension::Dimension") {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  Dimension x{"", Datatype::UINT32, memory_tracker};
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

  Deserializer deserializer(&serialized_buffer, sizeof(serialized_buffer));
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  FilterPipeline fp;
  auto dim = Dimension::deserialize(
      deserializer, 10, Datatype::INT32, fp, memory_tracker);

  // Check name
  CHECK(dim->name() == dimension_name);

  // Check type
  CHECK(dim->type() == Datatype::INT32);

  CHECK(dim->cell_val_num() == 1);
  CHECK(dim->var_size() == false);
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

  Deserializer deserializer(&serialized_buffer, sizeof(serialized_buffer));
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  FilterPipeline fp;
  auto dim = Dimension::deserialize(
      deserializer, 10, Datatype::INT32, fp, memory_tracker);
  // Check name
  CHECK(dim->name() == dimension_name);
  // Check type
  CHECK(dim->type() == type);
  CHECK(dim->cell_val_num() == constants::var_num);
  CHECK(dim->var_size() == true);
}

TEST_CASE("Dimension: Test datatypes", "[dimension][datatypes]") {
  std::string dim_name = "dim";
  auto memory_tracker = tiledb::test::get_test_memory_tracker();

  SECTION("- valid and supported Datatypes") {
    std::vector<Datatype> valid_supported_datatypes = {
        Datatype::INT32,          Datatype::INT64,
        Datatype::FLOAT32,        Datatype::FLOAT64,
        Datatype::INT8,           Datatype::UINT8,
        Datatype::INT16,          Datatype::UINT16,
        Datatype::UINT32,         Datatype::UINT64,
        Datatype::STRING_ASCII,   Datatype::DATETIME_YEAR,
        Datatype::DATETIME_MONTH, Datatype::DATETIME_WEEK,
        Datatype::DATETIME_DAY,   Datatype::DATETIME_HR,
        Datatype::DATETIME_MIN,   Datatype::DATETIME_SEC,
        Datatype::DATETIME_MS,    Datatype::DATETIME_US,
        Datatype::DATETIME_NS,    Datatype::DATETIME_PS,
        Datatype::DATETIME_FS,    Datatype::DATETIME_AS,
        Datatype::TIME_HR,        Datatype::TIME_MIN,
        Datatype::TIME_SEC,       Datatype::TIME_MS,
        Datatype::TIME_US,        Datatype::TIME_NS,
        Datatype::TIME_PS,        Datatype::TIME_FS,
        Datatype::TIME_AS};

    for (Datatype type : valid_supported_datatypes) {
      try {
        Dimension dim{dim_name, type, memory_tracker};
      } catch (...) {
        throw std::logic_error("Uncaught exception in Dimension constructor");
      }
    }
  }

  SECTION("- valid and unsupported Datatypes") {
    std::vector<Datatype> valid_unsupported_datatypes = {
        Datatype::CHAR,
        Datatype::BLOB,
        Datatype::GEOM_WKB,
        Datatype::GEOM_WKT,
        Datatype::BOOL,
        Datatype::STRING_UTF8,
        Datatype::STRING_UTF16,
        Datatype::STRING_UTF32,
        Datatype::STRING_UCS2,
        Datatype::STRING_UCS4,
        Datatype::ANY};

    for (Datatype type : valid_unsupported_datatypes) {
      try {
        Dimension dim{dim_name, type, memory_tracker};
      } catch (std::exception& e) {
        CHECK(
            e.what() == "Datatype::" + datatype_str(type) +
                            " is not a valid Dimension Datatype");
      }
    }
  }

  SECTION("- invalid Datatypes") {
    // Note: Ensure this test is updated each time a new datatype is added.
    std::vector<std::underlying_type_t<Datatype>> invalid_datatypes = {44, 100};

    for (auto type : invalid_datatypes) {
      try {
        Dimension dim{dim_name, Datatype(type), memory_tracker};
      } catch (std::exception& e) {
        CHECK(
            std::string(e.what()) ==
            "[Dimension::ensure_datatype_is_supported] ");
      }
    }
  }
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

void check_relevant_ranges(
    tdb::pmr::vector<uint64_t>& relevant_ranges,
    std::vector<uint64_t>& expected) {
  CHECK(relevant_ranges.size() == expected.size());
  for (uint64_t r = 0; r < expected.size(); r++) {
    CHECK(relevant_ranges[r] == expected[r]);
  }
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
    FixedTypes;
TEMPLATE_LIST_TEST_CASE(
    "test relevant_ranges", "[dimension][relevant_ranges][fixed]", FixedTypes) {
  typedef TestType T;
  auto tiledb_type = type_to_datatype<T>().datatype;
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  Dimension dim{"", tiledb_type, memory_tracker};

  std::vector<T> range_data = {
      1, 1, 1, 1, 2, 2, 3, 4, 5, 6, 5, 7, 8, 9, 50, 56};
  NDRange ranges;
  for (uint64_t r = 0; r < range_data.size() / 2; r++) {
    ranges.emplace_back(&range_data[r * 2], 2 * sizeof(T));
  }

  // Test data.
  std::vector<std::vector<T>> mbr_data = {{1, 1}, {2, 6}, {7, 8}};
  std::vector<std::vector<uint64_t>> expected = {{0, 1}, {2, 3, 4, 5}, {5, 6}};

  // Compute and check relevant ranges.
  for (uint64_t i = 0; i < mbr_data.size(); i++) {
    Range mbr(mbr_data[i].data(), 2 * sizeof(T));

    tdb::pmr::vector<uint64_t> relevant_ranges(
        memory_tracker->get_resource(MemoryType::DIMENSIONS));
    dim.relevant_ranges(ranges, mbr, relevant_ranges);
    check_relevant_ranges(relevant_ranges, expected[i]);
  }
}

TEST_CASE("test relevant_ranges", "[dimension][relevant_ranges][string]") {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  Dimension dim{"", Datatype::STRING_ASCII, memory_tracker};

  std::vector<char> range_data = {
      'a',
      'a',
      'a',
      'a',
      'b',
      'b',
      'c',
      'd',
      'e',
      'f',
      'e',
      'g',
      'h',
      'i',
      'y',
      'z'};
  NDRange ranges;
  for (uint64_t r = 0; r < range_data.size() / 2; r++) {
    ranges.emplace_back(&range_data[r * 2], 2, 1);
  }

  // Test data.
  std::vector<std::vector<char>> mbr_data = {
      {'a', 'a'}, {'b', 'f'}, {'g', 'h'}};
  std::vector<std::vector<uint64_t>> expected = {{0, 1}, {2, 3, 4, 5}, {5, 6}};

  // Compute and check relevant ranges.
  for (uint64_t i = 0; i < mbr_data.size(); i++) {
    Range mbr(mbr_data[i].data(), 2, 1);

    tdb::pmr::vector<uint64_t> relevant_ranges(
        memory_tracker->get_resource(MemoryType::DIMENSIONS));
    dim.relevant_ranges(ranges, mbr, relevant_ranges);
    check_relevant_ranges(relevant_ranges, expected[i]);
  }
}

TEST_CASE("Dimension::oob format") {
  auto memory_tracker = tiledb::test::get_test_memory_tracker();
  Dimension d("X", Datatype::FLOAT64, memory_tracker);
  double d_dom[2]{-682.73999, 929.42999};
  d.set_domain(Range(&d_dom, sizeof(d_dom)));
  double x{-682.75};
  std::string error{};
  bool b{Dimension::oob<double>(&d, &x, &error)};
  REQUIRE(b);
  CHECK(
      error ==
      "Coordinate -682.75 is out of domain bounds [-682.73999, 929.42999] on "
      "dimension 'X'");
}
