/**
 * @file   bench_dense_tile_cache.cc
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
 * Benchmark compressed dense 2D read performance from the tile cache.
 */

#include <tiledb/tiledb>

#include "benchmark.h"

using namespace tiledb;

class Benchmark : public BenchmarkBase {
 public:
  Benchmark()
      : BenchmarkBase() {
    // Set the max tile cache size to 10GB -- this is more
    // than enough to guarantee that the tile cach can
    // hold all of our tiles.
    Config config;
    config["sm.tile_cache_size"] = "10000000000";
    ctx_ = std::make_unique<Context>(config);
  }

 protected:
  virtual void setup() {
    ArraySchema schema(*ctx_, TILEDB_DENSE);
    Domain domain(*ctx_);
    domain.add_dimension(
        Dimension::create<uint32_t>(*ctx_, "d1", {{1, array_rows}}, tile_rows));
    domain.add_dimension(
        Dimension::create<uint32_t>(*ctx_, "d2", {{1, array_cols}}, tile_cols));
    schema.set_domain(domain);
    FilterList filters(*ctx_);
    filters.add_filter({*ctx_, TILEDB_FILTER_BYTESHUFFLE})
        .add_filter({*ctx_, TILEDB_FILTER_LZ4});
    schema.add_attribute(Attribute::create<int32_t>(*ctx_, "a", filters));
    Array::create(array_uri_, schema);

    data_.resize(array_rows * array_cols);
    for (uint64_t i = 0; i < data_.size(); i++) {
      data_[i] = i;
    }

    // Write the array one time.
    Array write_array(*ctx_, array_uri_, TILEDB_WRITE);
    Query write_query(*ctx_, write_array, TILEDB_WRITE);
    write_query
        .set_subarray(Subarray(*ctx_, write_array)
                          .set_subarray({1u, array_rows, 1u, array_cols}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_);
    write_query.submit();
    write_array.close();
  }

  virtual void teardown() {
    VFS vfs(*ctx_);
    if (vfs.is_dir(array_uri_))
      vfs.remove_dir(array_uri_);
  }

  virtual void pre_run() {
    data_.resize(array_rows * array_cols);

    // Read the array one time, populating the entire tile cache.
    Array read_array(*ctx_, array_uri_, TILEDB_READ);
    Query read_query(*ctx_, read_array);
    read_query
        .set_subarray(Subarray(*ctx_, read_array)
                          .set_subarray({1u, array_rows, 1u, array_cols}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_);
    read_query.submit();
    read_array.close();
  }

  virtual void run() {
    // Read the entire array multiple times. We have already populated
    // the tile cache in pre_run().
    for (int i = 0; i < 10; ++i) {
      Array array(*ctx_, array_uri_, TILEDB_READ);
      Query query(*ctx_, array);
      query
          .set_subarray(Subarray(*ctx_, array)
                            .set_subarray({1u, array_rows, 1u, array_cols}))
          .set_layout(TILEDB_ROW_MAJOR)
          .set_data_buffer("a", data_);
      query.submit();
      array.close();
    }
  }

 private:
  const std::string array_uri_ = "bench_array";
  const unsigned array_rows = 10000, array_cols = 20000;
  const unsigned tile_rows = 100, tile_cols = 100;

  std::unique_ptr<Context> ctx_;
  std::vector<int> data_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
