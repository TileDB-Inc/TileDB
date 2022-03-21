/**
 * @file   unit-tile-metadata-generator.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * Tests the TileMetadataGenerator class.
 */

#include <catch.hpp>

#include "catch.hpp"
#include "helpers.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/tile/tile_metadata_generator.h"

using namespace tiledb::sm;

typedef tuple<
    std::byte,
    unsigned char,  // Used for TILEDB_CHAR.
    char,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    float,
    double>
    FixedTypesUnderTest;
TEMPLATE_LIST_TEST_CASE(
    "TileMetadataGenerator: fixed data type tile",
    "[tile-metadata-generator][fixed-data]",
    FixedTypesUnderTest) {
  std::default_random_engine random_engine;
  typedef TestType T;

  auto tiledb_type = Datatype::CHAR;
  if constexpr (!std::is_same<T, unsigned char>::value) {
    auto type = tiledb::impl::type_to_tiledb<T>();
    tiledb_type = static_cast<Datatype>(type.tiledb_type);
  }

  std::string test =
      GENERATE("non nullable", "nullable", "all null", "empty tile");

  bool nullable = test == "nullable" || test == "all null";
  bool all_null = test == "all null";
  bool empty_tile = test == "empty tile";

  uint64_t cell_val_num = std::is_same<T, char>::value ? 10 : 1;

  // Generate the array schema.
  ArraySchema schema;
  Attribute a("a", tiledb_type);
  a.set_cell_val_num(cell_val_num);
  schema.add_attribute(tdb::make_shared<Attribute>(HERE(), &a));

  // Generate random, sorted strings for the string ascii type.
  std::vector<std::string> string_ascii;
  string_ascii.reserve(256);
  if constexpr (std::is_same<T, char>::value) {
    for (uint64_t i = 0; i < 256; i++) {
      string_ascii.emplace_back(tiledb::test::random_string(10));
    }
    std::sort(string_ascii.begin(), string_ascii.end());
  }

  // Initialize a new tile.
  uint64_t num_cells = empty_tile ? 0 : 1000;
  Tile tile;
  tile.init_unfiltered(
      0,
      tiledb_type,
      num_cells * cell_val_num * sizeof(T),
      cell_val_num * sizeof(T),
      0,
      true);
  auto tile_buff = (T*)tile.data();

  // Initialize a new nullable tile.
  Tile tile_nullable;
  uint8_t* nullable_buff = nullptr;
  if (nullable) {
    tile_nullable.init_unfiltered(0, Datatype::UINT8, num_cells, 1, 0, true);
    nullable_buff = (uint8_t*)tile_nullable.data();
  }

  // Compute correct values as the tile is filled with data.
  T correct_min = std::numeric_limits<T>::max();
  T correct_max = std::numeric_limits<T>::lowest();
  int64_t correct_sum_int = 0;
  double correct_sum_double = 0;
  uint64_t correct_null_count = 0;

  // Fill the tiles with data.
  for (uint64_t i = 0; i < num_cells; i++) {
    // Generate a random value depending on the data type.
    T val;
    uint8_t validity_val = all_null ? 0 : (nullable ? rand() % 2 : 1);

    if constexpr (std::is_same<T, std::byte>::value) {
      val = std::byte(0);
    } else if constexpr (std::is_integral_v<T>) {
      if constexpr (std::is_signed_v<TestType>) {
        if constexpr (std::is_same<TestType, int64_t>::value) {
          std::uniform_int_distribution<int64_t> dist(
              std::numeric_limits<int32_t>::lowest(),
              std::numeric_limits<int32_t>::max());
          val = dist(random_engine);
        } else {
          std::uniform_int_distribution<int64_t> dist(
              std::numeric_limits<TestType>::lowest(),
              std::numeric_limits<TestType>::max());
          val = (TestType)dist(random_engine);
        }
      } else {
        if constexpr (std::is_same<TestType, uint64_t>::value) {
          std::uniform_int_distribution<uint64_t> dist(
              std::numeric_limits<uint32_t>::lowest(),
              std::numeric_limits<uint32_t>::max());
          val = dist(random_engine);
        } else {
          std::uniform_int_distribution<uint64_t> dist(
              std::numeric_limits<TestType>::lowest(),
              std::numeric_limits<TestType>::max());
          val = (TestType)dist(random_engine);
        }
      }

      if constexpr (!std::is_same<T, char>::value) {
        if (validity_val == 1) {
          correct_sum_int += (int64_t)val;
        }
      }

      correct_sum_double = 0;
    } else {
      std::uniform_real_distribution<T> dist(-10000, 10000);
      val = dist(random_engine);
      if (validity_val == 1) {
        correct_sum_double += (double)val;
      }

      correct_sum_int = 0;
    }

    if (nullable) {
      nullable_buff[i] = validity_val;
    }

    if (validity_val == 1) {
      correct_min = std::min(correct_min, val);
      correct_max = std::max(correct_max, val);
    }
    correct_null_count += uint64_t(validity_val == 0);

    if constexpr (std::is_same<T, char>::value) {
      int64_t idx = (int64_t)val - (int64_t)std::numeric_limits<char>::min();
      memcpy(
          &tile_buff[i * cell_val_num],
          string_ascii[idx].c_str(),
          cell_val_num);
    } else {
      tile_buff[i] = val;
    }
  }

  // Call the tile metadata generator.
  TileMetadataGenerator md_generator(
      tiledb_type, false, false, cell_val_num * sizeof(T), cell_val_num);
  md_generator.process_tile(
      &tile, nullptr, nullable ? &tile_nullable : nullptr);

  // Compare the metadata to what's expected.
  auto&& [min, min_size, max, max_size, sum, nc] = md_generator.metadata();

  if constexpr (std::is_same<T, char>::value) {
    if (all_null || empty_tile) {
      CHECK(min == nullptr);
      CHECK(max == nullptr);
    } else {
      int64_t idx_min =
          (int64_t)correct_min - (int64_t)std::numeric_limits<char>::min();
      int64_t idx_max =
          (int64_t)correct_max - (int64_t)std::numeric_limits<char>::min();
      CHECK(
          0 ==
          strncmp(
              (const char*)min, string_ascii[idx_min].c_str(), cell_val_num));
      CHECK(
          0 ==
          strncmp(
              (const char*)max, string_ascii[idx_max].c_str(), cell_val_num));
    }
  } else {
    if constexpr (std::is_same<T, std::byte>::value) {
      CHECK(min == nullptr);
      CHECK(max == nullptr);
    } else if constexpr (std::is_same<T, unsigned char>::value) {
      if (all_null || empty_tile) {
        CHECK(min == nullptr);
        CHECK(max == nullptr);
      } else {
        CHECK(*(T*)min == correct_min);
        CHECK(*(T*)max == correct_max);
      }
    } else {
      CHECK(*(T*)min == correct_min);
      CHECK(*(T*)max == correct_max);
    }
  }
  CHECK(min_size == sizeof(T) * cell_val_num);
  CHECK(max_size == sizeof(T) * cell_val_num);

  if constexpr (
      !std::is_same<T, unsigned char>::value &&
      !std::is_same<T, std::byte>::value) {
    if constexpr (std::is_integral_v<T>) {
      CHECK(*(int64_t*)sum->data() == correct_sum_int);
    } else {
      CHECK(*(double*)sum->data() == correct_sum_double);
    }
  }
  CHECK(nc == correct_null_count);
}

typedef tuple<uint64_t, int64_t, double> FixedTypesUnderTestOverflow;
TEMPLATE_LIST_TEST_CASE(
    "TileMetadataGenerator: fixed data type tile overflow",
    "[tile-metadata-generator][sum-overflow]",
    FixedTypesUnderTestOverflow) {
  typedef TestType T;
  auto type = tiledb::impl::type_to_tiledb<T>();

  // Generate the array schema.
  ArraySchema schema;
  Attribute a("a", (Datatype)type.tiledb_type);
  schema.add_attribute(tdb::make_shared<Attribute>(HERE(), &a));

  // Initialize a new tile.
  uint64_t num_cells = 4;
  Tile tile;
  tile.init_unfiltered(
      0, (Datatype)type.tiledb_type, num_cells * sizeof(T), sizeof(T), 0, true);
  auto tile_buff = (T*)tile.data();

  // Once an overflow happens, the computation should abort, try to add a few
  // min values after the overflow to confirm.
  tile_buff[0] = std::numeric_limits<T>::max();
  tile_buff[1] = std::numeric_limits<T>::max();
  tile_buff[2] = std::numeric_limits<T>::lowest();
  tile_buff[3] = std::numeric_limits<T>::lowest();

  // Call the tile metadata generator.
  TileMetadataGenerator md_generator(
      static_cast<Datatype>(type.tiledb_type), false, false, sizeof(T), 1);
  md_generator.process_tile(&tile, nullptr, nullptr);

  // Compare the metadata to what's expected.
  auto&& [min, min_size, max, max_size, sum, nc] = md_generator.metadata();
  if constexpr (std::is_integral_v<T>) {
    CHECK(*(T*)sum->data() == std::numeric_limits<T>::max());
  } else {
    CHECK(*(double*)sum->data() == std::numeric_limits<T>::max());
  }

  // Test negative overflow.
  if constexpr (std::is_signed_v<T>) {
    // Initialize a new tile.
    uint64_t num_cells = 4;
    Tile tile;
    tile.init_unfiltered(
        0,
        (Datatype)type.tiledb_type,
        num_cells * sizeof(T),
        sizeof(T),
        0,
        true);
    auto tile_buff = (T*)tile.data();

    // Once an overflow happens, the computation should abort, try to add a few
    // max values after the overflow to confirm.
    tile_buff[0] = std::numeric_limits<T>::lowest();
    tile_buff[1] = std::numeric_limits<T>::lowest();
    tile_buff[2] = std::numeric_limits<T>::max();
    tile_buff[3] = std::numeric_limits<T>::max();

    // Call the tile metadata generator.
    TileMetadataGenerator md_generator(
        static_cast<Datatype>(type.tiledb_type), false, false, sizeof(T), 1);
    md_generator.process_tile(&tile, nullptr, nullptr);

    // Compare the metadata to what's expected.
    auto&& [min, min_size, max, max_size, sum, nc] = md_generator.metadata();
    if constexpr (std::is_integral_v<T>) {
      CHECK(*(int64_t*)sum->data() == std::numeric_limits<T>::min());
    } else {
      CHECK(*(double*)sum->data() == std::numeric_limits<T>::lowest());
    }
  }
}

TEST_CASE(
    "TileMetadataGenerator: var data tiles",
    "[tile-metadata-generator][var-data]") {
  std::string test =
      GENERATE("nullable", "all null", "non nullable", "empty tile");

  bool nullable = test == "nullable" || test == "all null";
  bool all_null = test == "all null";
  bool empty_tile = test == "empty tile";

  uint64_t max_string_size = 100;
  uint64_t num_strings = 2000;

  // Generate the array schema.
  ArraySchema schema;
  Attribute a("a", Datatype::STRING_ASCII);
  a.set_cell_val_num(constants::var_num);
  schema.add_attribute(tdb::make_shared<Attribute>(HERE(), &a));

  // Generate random, sorted strings for the string ascii type.
  std::vector<std::string> strings;
  strings.reserve(num_strings);
  for (uint64_t i = 0; i < num_strings; i++) {
    strings.emplace_back(tiledb::test::random_string(rand() % max_string_size));
  }
  std::sort(strings.begin(), strings.end());

  // Choose strings randomly.
  uint64_t num_cells = empty_tile ? 0 : 20;
  std::vector<int> values;
  values.reserve(num_cells);
  uint64_t var_size = 0;
  for (uint64_t i = 0; i < num_cells; i++) {
    values.emplace_back(rand() % num_strings);
    var_size += strings[values.back()].size();
  }

  // Initialize offsets tile.
  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      0,
      Datatype::UINT64,
      num_cells * sizeof(uint64_t),
      sizeof(uint64_t),
      0,
      true);
  auto offsets_tile_buff = (uint64_t*)offsets_tile.data();

  // Initialize var tile.
  Tile var_tile;
  var_tile.init_unfiltered(
      0, Datatype::CHAR, var_size, constants::var_num, 0, true);
  auto var_tile_buff = (char*)var_tile.data();

  // Initialize a new nullable tile.
  Tile tile_nullable;
  uint8_t* nullable_buff = nullptr;
  if (nullable) {
    tile_nullable.init_unfiltered(0, Datatype::UINT8, num_cells, 1, 0, true);
    nullable_buff = (uint8_t*)tile_nullable.data();
  }

  // Compute correct values as the tile is filled with data.
  int correct_min = num_strings;
  int correct_max = 0;
  uint64_t correct_null_count = 0;

  // Fill the tiles with data.
  uint64_t offset = 0;
  for (uint64_t i = 0; i < num_cells; i++) {
    uint8_t validity_val = all_null ? 0 : (nullable ? rand() % 2 : 1);

    if (nullable) {
      nullable_buff[i] = validity_val;
    }

    if (validity_val == 1) {
      correct_min = std::min(correct_min, values[i]);
      correct_max = std::max(correct_max, values[i]);
    }
    correct_null_count += uint64_t(validity_val == 0);

    *offsets_tile_buff = offset;
    auto& val = strings[values[i]];
    memcpy(&var_tile_buff[offset], val.c_str(), val.size());

    offset += val.size();
    offsets_tile_buff++;
  }

  // Call the tile metadata generator.
  TileMetadataGenerator md_generator(
      Datatype::STRING_ASCII, false, true, TILEDB_VAR_NUM, 1);
  md_generator.process_tile(
      &offsets_tile, &var_tile, nullable ? &tile_nullable : nullptr);

  // Compare the metadata to what's expected.
  auto&& [min, min_size, max, max_size, sum, nc] = md_generator.metadata();

  if (all_null || empty_tile) {
    CHECK(min == nullptr);
    CHECK(max == nullptr);
    CHECK(min_size == 0);
    CHECK(max_size == 0);
  } else {
    CHECK(
        0 == strncmp(
                 (const char*)min,
                 strings[correct_min].c_str(),
                 strings[correct_min].size()));
    CHECK(
        0 == strncmp(
                 (const char*)max,
                 strings[correct_max].c_str(),
                 strings[correct_max].size()));
    CHECK(min_size == strings[correct_min].size());
    CHECK(max_size == strings[correct_max].size());
  }

  CHECK(*(int64_t*)sum->data() == 0);
  CHECK(nc == correct_null_count);
}

TEST_CASE(
    "TileMetadataGenerator: var data tiles same string, different lengths",
    "[tile-metadata-generator][var-data][same-length]") {
  // Generate the array schema.
  ArraySchema schema;
  Attribute a("a", Datatype::CHAR);
  a.set_cell_val_num(constants::var_num);
  schema.add_attribute(tdb::make_shared<Attribute>(HERE(), &a));

  // Store '123' and '12'
  // Initialize offsets tile.
  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      0, Datatype::UINT64, 2 * sizeof(uint64_t), sizeof(uint64_t), 0, true);
  auto offsets_tile_buff = (uint64_t*)offsets_tile.data();
  offsets_tile_buff[0] = 0;
  offsets_tile_buff[1] = 3;

  // Initialize var tile.
  Tile var_tile;
  var_tile.init_unfiltered(0, Datatype::CHAR, 5, constants::var_num, 0, true);
  auto var_tile_buff = (char*)var_tile.data();
  var_tile_buff[0] = '1';
  var_tile_buff[1] = '2';
  var_tile_buff[2] = '3';
  var_tile_buff[3] = '1';
  var_tile_buff[4] = '2';

  // Call the tile metadata generator.
  TileMetadataGenerator md_generator(
      Datatype::STRING_ASCII, false, true, TILEDB_VAR_NUM, 1);
  md_generator.process_tile(&offsets_tile, &var_tile, nullptr);

  // Compare the metadata to what's expected.
  auto&& [min, min_size, max, max_size, sum, nc] = md_generator.metadata();

  CHECK(0 == strncmp((const char*)min, "12", 2));
  CHECK(0 == strncmp((const char*)max, "123", 3));
  CHECK(min_size == 2);
  CHECK(max_size == 3);

  CHECK(*(int64_t*)sum->data() == 0);
  CHECK(nc == 0);
}