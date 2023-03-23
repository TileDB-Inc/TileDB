/**
 * @file tiledb/sm/filter/test/unit_categorical_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file tests the categorical filter implementation.
 *
 */

#include <stdio.h>

#include "test/support/tdb_catch.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/filter/categorical_filter.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/misc/endian.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::sm;

static tiledb::sm::stats::Stats test_stats("test");

std::tuple<std::shared_ptr<uint8_t[]>, uint64_t> vec_to_buf(
    std::vector<std::string>& categories) {
  uint64_t length = (categories.size() + 1) * sizeof(uint64_t);
  for (auto category : categories) {
    length += category.size();
  }

  auto ret = std::shared_ptr<uint8_t[]>(new uint8_t[length]);
  utils::endianness::encode_be<uint64_t>(categories.size(), ret.get());
  size_t offset = sizeof(uint64_t);

  for (auto category : categories) {
    utils::endianness::encode_be<uint64_t>(category.size(), ret.get() + offset);
    offset += sizeof(uint64_t);

    std::memcpy(ret.get() + offset, category.data(), category.size());
    offset += category.size();
  }

  return {ret, length};
}

WriterTile make_data_tile(std::vector<std::string>& words) {
  uint64_t input_size = 0;
  for (auto word : words) {
    input_size += word.size();
  }

  WriterTile data(
      constants::format_version,
      Datatype::STRING_UTF8,
      constants::var_size,
      input_size);

  uint64_t offset = 0;
  for (uint64_t i = 0; i < words.size(); i++) {
    CHECK(data.write(words[i].data(), offset, words[i].size()).ok());
    offset += words[i].size();
  }

  return data;
}

WriterTile make_offsets_tile(std::vector<std::string>& words) {
  WriterTile offsets(
      constants::format_version,
      Datatype::UINT64,
      constants::cell_var_offset_size,
      words.size() * constants::cell_var_offset_size);

  uint64_t offset = 0;
  for (uint64_t i = 0; i < words.size(); i++) {
    CHECK(offsets.write(&offset, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    offset += words[i].size();
  }

  return offsets;
}

struct TestCase {
  TestCase(
      std::vector<std::string> categories,
      std::vector<std::string> words,
      std::vector<uint64_t> encoding)
      : TestCase(categories, words, encoding, words) {
  }

  TestCase(
      std::vector<std::string> categories,
      std::vector<std::string> words,
      std::vector<uint64_t> encoding,
      std::vector<std::string> expect)
      : categories_(categories)
      , words_(words)
      , encoding_(encoding)
      , expect_(expect) {
  }

  std::vector<std::string> categories_;
  std::vector<std::string> words_;
  std::vector<uint64_t> encoding_;
  std::vector<std::string> expect_;
};

void check_filter(TestCase& tc) {
  auto [buffer, buffer_length] = vec_to_buf(tc.categories_);
  auto data_tile = make_data_tile(tc.words_);
  auto offsets_tile = make_offsets_tile(tc.words_);
  auto data_size = data_tile.size();

  ThreadPool tp(4);
  FilterPipeline fp;
  CategoricalFilter filt{buffer.get()};
  fp.add_filter(filt);

  CHECK(
      fp.run_forward(&test_stats, &data_tile, &offsets_tile, &tp, false).ok());

  CHECK(data_tile.size() == 0);
  auto fbuf = data_tile.filtered_buffer();
  // PJD: The filter pipeline appears to add 20 bytes of metadata
  // to the filtered buffer. This appears to be 3 uint32_t's and
  // 1 uint64_t. Should probably figuere this out better.
  CHECK(fbuf.size() == 20 + tc.encoding_.size() * sizeof(uint64_t));

  for (uint64_t i = 0; i < tc.encoding_.size(); i++) {
    uint64_t offset = 20 + i * sizeof(uint64_t);
    CHECK(
        utils::endianness::decode_be<uint64_t>(fbuf.data() + offset) ==
        tc.encoding_[i]);
  }

  tiledb::sm::Config config;
  Tile unfiltered_tile(
      data_tile.format_version(),
      data_tile.type(),
      data_tile.cell_size(),
      0,
      data_size,
      data_tile.filtered_buffer().data(),
      data_tile.filtered_buffer().size());

  Tile unfiltered_offsets_tile(
      offsets_tile.format_version(),
      offsets_tile.type(),
      offsets_tile.cell_size(),
      0,
      tc.words_.size() * sizeof(uint64_t),
      offsets_tile.filtered_buffer().data(),
      offsets_tile.filtered_buffer().size());

  ChunkData chunk_data;
  unfiltered_tile.load_chunk_data(chunk_data);

  CHECK(fp.run_reverse(
              &test_stats,
              &unfiltered_tile,
              &unfiltered_offsets_tile,
              chunk_data,
              0,
              chunk_data.filtered_chunks_.size(),
              tp.concurrency_level(),
              config)
            .ok());

  data_tile = make_data_tile(tc.expect_);
  offsets_tile = make_offsets_tile(tc.expect_);

  CHECK(unfiltered_tile.size() == data_tile.size());
  CHECK(
      std::memcmp(data_tile.data(), unfiltered_tile.data(), data_tile.size()) ==
      0);

  CHECK(unfiltered_offsets_tile.size() == offsets_tile.size());
  CHECK(
      std::memcmp(
          offsets_tile.data(),
          unfiltered_offsets_tile.data(),
          offsets_tile.size()) == 0);
}

TEST_CASE("Categorical filter basic constructors", "[filter][categorical]") {
  CategoricalFilter filt1{};
  CategoricalFilter filt2{nullptr};
}

TEST_CASE(
    "Categrical filter round trip categries",
    "[filter][categorical][option-roundtrip]") {
  std::vector<std::string> categories = {"red", "blue", "green"};
  auto [buffer, buffer_length] = vec_to_buf(categories);

  CategoricalFilter filt{};
  auto st = filt.set_option(FilterOption::CATEGORIES, buffer.get());
  REQUIRE(st.ok());

  uint64_t length;
  st = filt.get_option(FilterOption::CATEGORY_BUFFER_LENGTH, &length);
  REQUIRE(st.ok());

  uint8_t ret_buffer[length];
  st = filt.get_option(FilterOption::CATEGORIES, &ret_buffer);
  REQUIRE(st.ok());

  REQUIRE(length == buffer_length);
  REQUIRE(std::memcmp(buffer.get(), ret_buffer, length) == 0);
}

TEST_CASE(
    "Categorical filter round trip data",
    "[filter][categorical-filter][no-categories]") {
  TestCase tc(
      {},
      {"red", "red", "blue", "green", "blue", "red"},
      {6, 0, 0, 0, 0, 0, 0},
      {"", "", "", "", "", ""});

  check_filter(tc);
}

TEST_CASE(
    "Categorical filter round trip data",
    "[filter][categorical-filter][values-roundtrip]") {
  TestCase tc(
      {"red", "green", "blue"},
      {"red", "red", "blue", "green", "blue", "red"},
      {6, 1, 1, 3, 2, 3, 1});

  check_filter(tc);
}

TEST_CASE(
    "Categorical filter round trip data",
    "[filter][categorical-filter][not-a-category]") {
  TestCase tc(
      {"red", "green", "blue"},
      {"red", "red", "blue", "orange", "blue", "red"},
      {6, 1, 1, 3, 0, 3, 1},
      {"red", "red", "blue", "", "blue", "red"});

  check_filter(tc);
}
