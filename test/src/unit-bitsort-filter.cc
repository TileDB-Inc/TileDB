/**
 * @file   unit-bitsort-filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests for bitsort filter related functions.
 */

#include "test/src/helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/query/readers/reader_base.h"
#include "tiledb/sm/filter/bitsort_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/sm/misc/types.h"

#include <test/support/tdb_catch.h>
#include <random>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

tuple<Status, optional<uint64_t>> test_load_chunk_data(
    Tile* const tile, ChunkData* unfiltered_tile) {
  assert(tile);
  assert(unfiltered_tile);
  assert(tile->filtered());

  Status st = Status::Ok();
  auto filtered_buffer_data = tile->filtered_buffer().data();
  if (filtered_buffer_data == nullptr) {
    return {st, nullopt};
  }

  // Make a pass over the tile to get the chunk information.
  uint64_t num_chunks;
  memcpy(&num_chunks, filtered_buffer_data, sizeof(uint64_t));
  filtered_buffer_data += sizeof(uint64_t);

  auto& filtered_chunks = unfiltered_tile->filtered_chunks_;
  auto& chunk_offsets = unfiltered_tile->chunk_offsets_;
  filtered_chunks.resize(num_chunks);
  chunk_offsets.resize(num_chunks);
  uint64_t total_orig_size = 0;
  for (uint64_t i = 0; i < num_chunks; i++) {
    auto& chunk = filtered_chunks[i];
    memcpy(
        &(chunk.unfiltered_data_size_), filtered_buffer_data, sizeof(uint32_t));
    filtered_buffer_data += sizeof(uint32_t);

    memcpy(
        &(chunk.filtered_data_size_), filtered_buffer_data, sizeof(uint32_t));
    filtered_buffer_data += sizeof(uint32_t);

    memcpy(
        &(chunk.filtered_metadata_size_),
        filtered_buffer_data,
        sizeof(uint32_t));
    filtered_buffer_data += sizeof(uint32_t);

    chunk.filtered_metadata_ = filtered_buffer_data;
    chunk.filtered_data_ = static_cast<char*>(chunk.filtered_metadata_) +
                           chunk.filtered_metadata_size_;

    chunk_offsets[i] = total_orig_size;
    total_orig_size += chunk.unfiltered_data_size_;

    filtered_buffer_data +=
        chunk.filtered_metadata_size_ + chunk.filtered_data_size_;
  }

  if (total_orig_size != tile->size()) {
    return {Status_ReaderError(
                "Error incorrect unfiltered tile size allocated."),
            nullopt};
  }

  return {Status::Ok(), total_orig_size};
}

/*
 Defining distribution types to pass into the testing_bitsort_filter function.
 */
typedef typename std::uniform_int_distribution<int64_t> IntDistribution;
typedef typename std::uniform_real_distribution<double> FloatDistribution;

template <typename T, typename Distribution = IntDistribution>
void testing_bitsort_filter(Datatype t) {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(T);
  const uint64_t cell_size = sizeof(T);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version, t, tile_size, cell_size, dim_num);

  // Setting up the random number generator for the XOR filter testing.
  std::mt19937_64 gen(0x57A672DE);
  Distribution dis(
      std::numeric_limits<T>::min(), std::numeric_limits<T>::max());

  std::vector<T> results;

  for (uint64_t i = 0; i < nelts; i++) {
    T val = static_cast<T>(dis(gen));
    CHECK(tile.write(&val, i * sizeof(T), sizeof(T)).ok());
    results.push_back(val);
  }

  FilterPipeline pipeline;
  ThreadPool tp(4);
  REQUIRE(pipeline.add_filter(BitSortFilter()).ok());

  std::vector<Tile*> dim_tiles_dummy;
  REQUIRE(pipeline.run_forward(&test::g_helper_stats, &tile, dim_tiles_dummy, &tp).ok());

  // Check new size and number of chunks
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);
  CHECK(tile.alloc_data(nelts * sizeof(T)).ok());

  ChunkData chunk_data;
  auto && [chunk_data_status, total_orig_size] {test_load_chunk_data(&tile, &chunk_data)};
  REQUIRE(chunk_data_status.ok());
  
  CHECK(pipeline.run_reverse(&test::g_helper_stats, &tile, dim_tiles_dummy, &tp, config)
            .ok());
  for (uint64_t i = 0; i < nelts; i++) {
    T elt = 0;
    CHECK(tile.read(&elt, i * sizeof(T), sizeof(T)).ok());
    CHECK(elt == results[i]);
  }
}

TEST_CASE("Filter: Test BitSort", "[filter][bitsort]") {
  testing_bitsort_filter<int8_t>(Datatype::INT8);
  testing_bitsort_filter<uint8_t>(Datatype::UINT8);
  testing_bitsort_filter<int16_t>(Datatype::INT16);
  testing_bitsort_filter<uint16_t>(Datatype::UINT16);
  testing_bitsort_filter<int32_t>(Datatype::INT32);
  testing_bitsort_filter<uint32_t>(Datatype::UINT32);
  testing_bitsort_filter<int64_t>(Datatype::INT64);
  testing_bitsort_filter<uint64_t>(Datatype::UINT64);
  testing_bitsort_filter<float, FloatDistribution>(Datatype::FLOAT32);
  testing_bitsort_filter<double, FloatDistribution>(Datatype::FLOAT64);
  testing_bitsort_filter<char>(Datatype::CHAR);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_YEAR);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_MONTH);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_WEEK);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_DAY);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_HR);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_MIN);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_SEC);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_MS);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_US);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_NS);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_PS);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_FS);
  testing_bitsort_filter<int64_t>(Datatype::DATETIME_AS);
}