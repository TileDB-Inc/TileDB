/**
 * @file   bench_dense_multi_fragment_read.cc
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
 * Benchmark dense 2D read performance across multiple unconsolidated
 * fragments. Setup writes 10 overlapping fragments, run reads the full array.
 *
 * For large-scale runs, increase array_rows/array_cols to 20000 and
 * num_fragments to 50.
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
    domain.add_dimension(Dimension::create<uint32_t>(
        ctx_, "d1", {{1, array_rows}}, tile_extent));
    domain.add_dimension(Dimension::create<uint32_t>(
        ctx_, "d2", {{1, array_cols}}, tile_extent));
    schema.set_domain(domain);
    FilterList filters(ctx_);
    filters.add_filter({ctx_, TILEDB_FILTER_LZ4});
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a", filters));
    Array::create(array_uri_, schema);

    // Write num_fragments overlapping fragments, each covering the full domain
    uint64_t ncells = array_rows * array_cols;
    std::vector<int> data(ncells);
    for (unsigned f = 0; f < num_fragments; f++) {
      for (uint64_t i = 0; i < ncells; i++)
        data[i] = f * ncells + i;

      Array array(ctx_, array_uri_, TILEDB_WRITE);
      Query query(ctx_, array, TILEDB_WRITE);
      query
          .set_subarray(Subarray(ctx_, array)
                            .set_subarray({1u, array_rows, 1u, array_cols}))
          .set_layout(TILEDB_ROW_MAJOR)
          .set_data_buffer("a", data);
      query.submit();
      array.close();
    }
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    data_.resize(array_rows * array_cols);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);
    query
        .set_subarray(Subarray(ctx_, array)
                          .set_subarray({1u, array_rows, 1u, array_cols}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_);
    query.submit();
    array.close();
  }

 private:
  const std::string array_uri_ = bench_uri("bench_array");
  const unsigned array_rows = 5000, array_cols = 5000;
  const unsigned tile_extent = 500;
  const unsigned num_fragments = 10;

  Context ctx_{bench_config()};
  std::vector<int> data_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
