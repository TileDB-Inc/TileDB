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

#include <random>

#include <test/support/src/mem_helpers.h>
#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/mem_helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/tile/tile_metadata_generator.h"
#include "tiledb/sm/tile/writer_tile_tuple.h"

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
  uint64_t num_cells = empty_tile ? 0 : 1000;
  ArraySchema schema(
      ArrayType::DENSE, tiledb::test::create_test_memory_tracker());
  schema.set_capacity(num_cells);
  Attribute a("a", tiledb_type);
  a.set_cell_val_num(cell_val_num);
  CHECK(schema.add_attribute(make_shared<Attribute>(HERE(), a)).ok());

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
  WriterTileTuple writer_tile(
      schema,
      num_cells,
      false,
      nullable,
      cell_val_num * sizeof(T),
      tiledb_type,
      tiledb::test::create_test_memory_tracker());
  auto tile_buff = writer_tile.fixed_tile().data_as<T>();
  uint8_t* nullable_buff = nullptr;
  if (nullable) {
    nullable_buff = writer_tile.validity_tile().data_as<uint8_t>();
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
  md_generator.process_full_tile(writer_tile);
  md_generator.set_tile_metadata(writer_tile);

  // Compare the metadata to what's expected.
  if constexpr (std::is_same<T, char>::value) {
    if (all_null || empty_tile) {
      CHECK(writer_tile.min()[0] == 0);
      CHECK(writer_tile.max()[0] == 0);
    } else {
      int64_t idx_min =
          (int64_t)correct_min - (int64_t)std::numeric_limits<char>::min();
      int64_t idx_max =
          (int64_t)correct_max - (int64_t)std::numeric_limits<char>::min();
      CHECK(
          0 == strncmp(
                   (const char*)writer_tile.min().data(),
                   string_ascii[idx_min].c_str(),
                   cell_val_num));
      CHECK(
          0 == strncmp(
                   (const char*)writer_tile.max().data(),
                   string_ascii[idx_max].c_str(),
                   cell_val_num));
    }
  } else {
    if constexpr (std::is_same<T, std::byte>::value) {
      CHECK(writer_tile.min().data()[0] == 0);
      CHECK(writer_tile.max().data()[0] == 0);
    } else if constexpr (std::is_same<T, unsigned char>::value) {
      if (all_null || empty_tile) {
        CHECK(writer_tile.min().data()[0] == 0);
        CHECK(writer_tile.max().data()[0] == 0);
      } else {
        CHECK(*(T*)writer_tile.min().data() == correct_min);
        CHECK(*(T*)writer_tile.max().data() == correct_max);
      }
    } else {
      CHECK(*(T*)writer_tile.min().data() == correct_min);
      CHECK(*(T*)writer_tile.max().data() == correct_max);
    }
  }
  CHECK(writer_tile.min().size() == sizeof(T) * cell_val_num);
  CHECK(writer_tile.max().size() == sizeof(T) * cell_val_num);

  if constexpr (
      !std::is_same<T, unsigned char>::value &&
      !std::is_same<T, std::byte>::value) {
    if constexpr (std::is_integral_v<T>) {
      CHECK(*(int64_t*)writer_tile.sum().data() == correct_sum_int);
    } else {
      CHECK(*(double*)writer_tile.sum().data() == correct_sum_double);
    }
  }
  CHECK(writer_tile.null_count() == correct_null_count);
}

typedef tuple<uint64_t, int64_t, double> FixedTypesUnderTestOverflow;
TEMPLATE_LIST_TEST_CASE(
    "TileMetadataGenerator: fixed data type tile overflow",
    "[tile-metadata-generator][sum-overflow]",
    FixedTypesUnderTestOverflow) {
  typedef TestType T;
  auto type = tiledb::impl::type_to_tiledb<T>();

  // Generate the array schema.
  ArraySchema schema(
      ArrayType::DENSE, tiledb::test::create_test_memory_tracker());
  schema.set_capacity(4);
  Attribute a("a", (Datatype)type.tiledb_type);
  CHECK(schema.add_attribute(make_shared<Attribute>(HERE(), a)).ok());

  // Initialize a new tile.
  auto tiledb_type = static_cast<Datatype>(type.tiledb_type);
  WriterTileTuple writer_tile(
      schema,
      4,
      false,
      false,
      sizeof(T),
      tiledb_type,
      tiledb::test::create_test_memory_tracker());
  auto tile_buff = writer_tile.fixed_tile().data_as<T>();

  // Once an overflow happens, the computation should abort, try to add a few
  // min values after the overflow to confirm.
  tile_buff[0] = std::numeric_limits<T>::max();
  tile_buff[1] = std::numeric_limits<T>::max();
  tile_buff[2] = std::numeric_limits<T>::lowest();
  tile_buff[3] = std::numeric_limits<T>::lowest();

  // Call the tile metadata generator.
  TileMetadataGenerator md_generator(tiledb_type, false, false, sizeof(T), 1);
  md_generator.process_full_tile(writer_tile);
  md_generator.set_tile_metadata(writer_tile);

  // Compare the metadata to what's expected.
  if constexpr (std::is_integral_v<T>) {
    CHECK(*(T*)writer_tile.sum().data() == std::numeric_limits<T>::max());
  } else {
    CHECK(*(double*)writer_tile.sum().data() == std::numeric_limits<T>::max());
  }

  // Test negative overflow.
  if constexpr (std::is_signed_v<T>) {
    // Initialize a new tile.
    WriterTileTuple writer_tile(
        schema,
        4,
        false,
        false,
        sizeof(T),
        tiledb_type,
        tiledb::test::create_test_memory_tracker());
    auto tile_buff = writer_tile.fixed_tile().data_as<T>();

    // Once an overflow happens, the computation should abort, try to add a few
    // max values after the overflow to confirm.
    tile_buff[0] = std::numeric_limits<T>::lowest();
    tile_buff[1] = std::numeric_limits<T>::lowest();
    tile_buff[2] = std::numeric_limits<T>::max();
    tile_buff[3] = std::numeric_limits<T>::max();

    // Call the tile metadata generator.
    TileMetadataGenerator md_generator(tiledb_type, false, false, sizeof(T), 1);
    md_generator.process_full_tile(writer_tile);
    md_generator.set_tile_metadata(writer_tile);

    // Compare the metadata to what's expected.
    if constexpr (std::is_integral_v<T>) {
      CHECK(
          *(int64_t*)writer_tile.sum().data() == std::numeric_limits<T>::min());
    } else {
      CHECK(
          *(double*)writer_tile.sum().data() ==
          std::numeric_limits<T>::lowest());
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
  int num_strings = 2000;

  // Generate the array schema.
  uint64_t num_cells = empty_tile ? 0 : 20;
  ArraySchema schema(
      ArrayType::DENSE, tiledb::test::create_test_memory_tracker());
  schema.set_capacity(num_cells);
  Attribute a("a", Datatype::STRING_ASCII);
  a.set_cell_val_num(constants::var_num);
  CHECK(schema.add_attribute(make_shared<Attribute>(HERE(), a)).ok());

  // Generate random, sorted strings for the string ascii type.
  std::vector<std::string> strings;
  strings.reserve(num_strings);
  for (int i = 0; i < num_strings; i++) {
    strings.emplace_back(tiledb::test::random_string(rand() % max_string_size));
  }
  std::sort(strings.begin(), strings.end());

  // Choose strings randomly.
  std::vector<int> values;
  values.reserve(num_cells);
  uint64_t var_size = 0;
  for (uint64_t i = 0; i < num_cells; i++) {
    values.emplace_back(rand() % num_strings);
    var_size += strings[values.back()].size();
  }

  // Initialize tile.
  WriterTileTuple writer_tile(
      schema,
      num_cells,
      true,
      nullable,
      1,
      Datatype::CHAR,
      tiledb::test::create_test_memory_tracker());
  auto offsets_tile_buff = writer_tile.offset_tile().data_as<offsets_t>();

  // Initialize a new nullable tile.
  uint8_t* nullable_buff = nullptr;
  if (nullable) {
    nullable_buff = writer_tile.validity_tile().data_as<uint8_t>();
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
    CHECK_NOTHROW(
        writer_tile.var_tile().write_var(val.c_str(), offset, val.size()));

    offset += val.size();
    offsets_tile_buff++;
  }

  writer_tile.var_tile().set_size(var_size);

  // Call the tile metadata generator.
  TileMetadataGenerator md_generator(
      Datatype::STRING_ASCII, false, true, TILEDB_VAR_NUM, 1);
  md_generator.process_full_tile(writer_tile);
  md_generator.set_tile_metadata(writer_tile);

  // Compare the metadata to what's expected.
  if (all_null || empty_tile) {
    CHECK(writer_tile.min().data() == nullptr);
    CHECK(writer_tile.max().data() == nullptr);
    CHECK(writer_tile.min().size() == 0);
    CHECK(writer_tile.max().size() == 0);
  } else {
    CHECK(
        0 == strncmp(
                 (const char*)writer_tile.min().data(),
                 strings[correct_min].c_str(),
                 strings[correct_min].size()));
    CHECK(
        0 == strncmp(
                 (const char*)writer_tile.max().data(),
                 strings[correct_max].c_str(),
                 strings[correct_max].size()));
    CHECK(writer_tile.min().size() == strings[correct_min].size());
    CHECK(writer_tile.max().size() == strings[correct_max].size());
  }

  CHECK(*(int64_t*)writer_tile.sum().data() == 0);
  CHECK(writer_tile.null_count() == correct_null_count);
}

TEST_CASE(
    "TileMetadataGenerator: var data tiles same string, different lengths",
    "[tile-metadata-generator][var-data][same-length]") {
  // Generate the array schema.
  ArraySchema schema(
      ArrayType::DENSE, tiledb::test::create_test_memory_tracker());
  schema.set_capacity(2);
  Attribute a("a", Datatype::CHAR);
  a.set_cell_val_num(constants::var_num);
  CHECK(schema.add_attribute(make_shared<Attribute>(HERE(), a)).ok());

  // Store '123' and '12'
  // Initialize offsets tile.
  WriterTileTuple writer_tile(
      schema,
      2,
      true,
      false,
      1,
      Datatype::CHAR,
      tiledb::test::create_test_memory_tracker());
  auto offsets_tile_buff = writer_tile.offset_tile().data_as<offsets_t>();
  offsets_tile_buff[0] = 0;
  offsets_tile_buff[1] = 3;

  // Initialize var tile.
  std::string data = "12312";
  CHECK_NOTHROW(writer_tile.var_tile().write_var(data.c_str(), 0, 5));
  writer_tile.var_tile().set_size(5);

  // Call the tile metadata generator.
  TileMetadataGenerator md_generator(
      Datatype::STRING_ASCII, false, true, TILEDB_VAR_NUM, 1);
  md_generator.process_full_tile(writer_tile);
  md_generator.set_tile_metadata(writer_tile);

  // Compare the metadata to what's expected.
  CHECK(0 == strncmp((const char*)writer_tile.min().data(), "12", 2));
  CHECK(0 == strncmp((const char*)writer_tile.max().data(), "123", 3));
  CHECK(writer_tile.min().size() == 2);
  CHECK(writer_tile.max().size() == 3);

  CHECK(*(int64_t*)writer_tile.sum().data() == 0);
  CHECK(writer_tile.null_count() == 0);
}
