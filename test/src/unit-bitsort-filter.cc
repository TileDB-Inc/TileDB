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
#include "tiledb/sm/filter/bitsort_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/tile/tile.h"

#include <test/support/tdb_catch.h>
#include <random>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

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
  CHECK(pipeline.add_filter(BitSortFilter()).ok());

  std::vector<Tile*> dim_tiles_dummy;
  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, dim_tiles_dummy, &tp).ok());

/*
  // Check new size and number of chunks
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);
  CHECK(tile.alloc_data(nelts * sizeof(T)).ok());
  CHECK(pipeline.run_reverse_chunk_range(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
  for (uint64_t i = 0; i < nelts; i++) {
    T elt = 0;
    CHECK(tile.read(&elt, i * sizeof(T), sizeof(T)).ok());
    CHECK(elt == results[i]);
  }
  */
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