/**
 * @file   bench_dense_3d_read.cc
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
 * Benchmark dense 3D array read performance with a sub-region query.
 * Reads the inner half of each dimension.
 *
 * For large-scale runs, increase dim_size to 500.
 */

#include <tiledb/tiledb>

#include "benchmark.h"
#include "benchmark_config.h"

using namespace tiledb;

class Benchmark : public BenchmarkBase {
 protected:
  virtual void setup() {
    ArraySchema schema(ctx_, TILEDB_DENSE);
    Domain domain(ctx_);
    domain.add_dimension(
        Dimension::create<uint32_t>(ctx_, "x", {{1, dim_size}}, tile_extent));
    domain.add_dimension(
        Dimension::create<uint32_t>(ctx_, "y", {{1, dim_size}}, tile_extent));
    domain.add_dimension(
        Dimension::create<uint32_t>(ctx_, "z", {{1, dim_size}}, tile_extent));
    schema.set_domain(domain);
    FilterList filters(ctx_);
    filters.add_filter({ctx_, TILEDB_FILTER_LZ4});
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a", filters));
    Array::create(array_uri_, schema);

    uint64_t ncells = static_cast<uint64_t>(dim_size) * dim_size * dim_size;
    std::vector<int> write_data(ncells);
    for (uint64_t i = 0; i < ncells; i++)
      write_data[i] = i;

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);
    query
        .set_subarray(
            Subarray(ctx_, array)
                .set_subarray({1u, dim_size, 1u, dim_size, 1u, dim_size}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", write_data);
    query.submit();
    array.close();
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    // Read inner half of each dimension
    uint32_t range = dim_size / 2;
    uint64_t ncells = static_cast<uint64_t>(range) * range * range;
    data_.resize(ncells);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);
    uint32_t lo = dim_size / 4 + 1;
    uint32_t hi = lo + dim_size / 2 - 1;
    query
        .set_subarray(
            Subarray(ctx_, array).set_subarray({lo, hi, lo, hi, lo, hi}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_);
    query.submit();
    array.close();
  }

 private:
  const std::string array_uri_ = bench_uri("bench_array");
  const unsigned dim_size = 200;
  const unsigned tile_extent = 50;

  Context ctx_{bench_config()};
  std::vector<int> data_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
