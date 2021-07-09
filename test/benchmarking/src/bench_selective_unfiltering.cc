/**
 * @file   bench_selective_unfiltering.cc
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
 * Benchmark selective unfiltering on a dense array.
 */

#include <tiledb/tiledb>

#include "benchmark.h"

using namespace tiledb;

class Benchmark : public BenchmarkBase {
 public:
  Benchmark()
      : BenchmarkBase() {
    // Set the max tile cache size to 0B -- this reduces memory
    // so that we can properly benchmark memory saved by
    // selective unfiltering.
    Config config;
    config["sm.tile_cache_size"] = "0";
    ctx_ = std::unique_ptr<Context>(new Context(config));
  }

 protected:
  virtual void setup() {
    FilterList filters(*ctx_);
    filters.add_filter({*ctx_, TILEDB_FILTER_BYTESHUFFLE})
        .add_filter({*ctx_, TILEDB_FILTER_BZIP2});

    // Setup the dense array.
    ArraySchema d_schema(*ctx_, TILEDB_DENSE);
    Domain d_domain(*ctx_);
    d_domain.add_dimension(Dimension::create<uint32_t>(
        *ctx_, "d1", {{1, dense_array_rows}}, tile_rows));
    d_domain.add_dimension(Dimension::create<uint32_t>(
        *ctx_, "d2", {{1, dense_array_cols}}, tile_cols));
    d_schema.set_domain(d_domain);
    d_schema.add_attribute(Attribute::create<int32_t>(*ctx_, "a", filters));
    d_schema.add_attribute(Attribute::create<int32_t>(*ctx_, "b"));
    d_schema.add_attribute(
        Attribute::create<std::vector<int>>(*ctx_, "c", filters));
    Array::create(dense_array_uri_, d_schema);
  }

  virtual void teardown() {
    VFS vfs(*ctx_);
    if (vfs.is_dir(dense_array_uri_))
      vfs.remove_dir(dense_array_uri_);
  }

  virtual void pre_run() {
    std::vector<int> data_a;
    std::vector<int> data_b;
    std::vector<uint64_t> off_c;
    std::vector<int> data_c;

    data_a.resize(dense_array_rows * dense_array_cols);
    data_b.resize(data_a.size());
    off_c.resize(data_a.size());
    data_c.resize(data_a.size() * 2);
    for (uint64_t i = 0; i < data_a.size(); i++) {
      data_a[i] = 1;
      data_b[i] = 1;
      off_c[i] = (2 * i) * sizeof(int);
      data_c[2 * i] = 1;
      data_c[(2 * i) + 1] = 1;
    }

    // Write the dense array 1 time.
    Array d_write_array(*ctx_, dense_array_uri_, TILEDB_WRITE);
    Query d_write_query(*ctx_, d_write_array, TILEDB_WRITE);
    d_write_query.set_subarray({1u, dense_array_rows, 1u, dense_array_cols})
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_a)
        .set_data_buffer("b", data_b)
        .set_data_buffer("c", data_c)
        .set_offsets_buffer("c", off_c);
    d_write_query.submit();
    d_write_array.close();
  }

  virtual void run() {
    std::vector<int> data_a;
    std::vector<int> data_b;
    std::vector<uint64_t> off_c;
    std::vector<int> data_c;

    const unsigned query_rows = dense_array_rows / tile_rows;
    const unsigned query_cols = dense_array_cols / tile_cols;
    const unsigned subarray_size = 5;

    data_a.resize(query_rows * query_cols * subarray_size * subarray_size);
    data_b.resize(query_rows * query_cols * subarray_size * subarray_size);
    off_c.resize(query_rows * query_cols * subarray_size * subarray_size);
    data_c.resize(query_rows * query_cols * subarray_size * subarray_size * 2);

    // Read the array 20 times.
    for (int i = 0; i < 20; ++i) {
      Array array(*ctx_, dense_array_uri_, TILEDB_READ);
      Query query(*ctx_, array);

      for (int j = 0; j < query_rows; ++j) {
        query.add_range(
            0, (j * tile_rows) + 1, (j * tile_rows) + subarray_size);
      }

      for (int j = 0; j < query_cols; ++j) {
        query.add_range(
            1, (j * tile_cols) + 1, (j * tile_cols) + subarray_size);
      }

      query.set_layout(TILEDB_ROW_MAJOR)
          .set_data_buffer("a", data_a)
          .set_data_buffer("b", data_b)
          .set_data_buffer("c", data_c)
          .set_offsets_buffer("c", off_c);
      query.submit();
      array.close();
    }
  }

 private:
  const std::string dense_array_uri_ = "dense_bench_array";
  const unsigned dense_array_rows = 6000, dense_array_cols = 6000;
  const unsigned tile_rows = 2000, tile_cols = 2000;

  std::unique_ptr<Context> ctx_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
