/**
 * @file   bench_sparse_multi_fragment_read.cc
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
 * Benchmark sparse 2D read performance across multiple unconsolidated
 * fragments. Setup writes 10 non-overlapping fragments, run reads all data.
 *
 * For large-scale runs, increase max_row and max_col to 20000 and
 * num_fragments to 50.
 */

#include <tiledb/tiledb>

#include "benchmark.h"
#include "benchmark_config.h"

using namespace tiledb;

class Benchmark : public BenchmarkBase {
 protected:
  virtual void setup() {
    ArraySchema schema(ctx_, TILEDB_SPARSE);
    Domain domain(ctx_);
    domain.add_dimension(Dimension::create<uint32_t>(
        ctx_,
        "d1",
        {{1, std::numeric_limits<uint32_t>::max() - tile_extent}},
        tile_extent));
    domain.add_dimension(Dimension::create<uint32_t>(
        ctx_,
        "d2",
        {{1, std::numeric_limits<uint32_t>::max() - tile_extent}},
        tile_extent));
    schema.set_domain(domain);
    schema.set_capacity(capacity);
    FilterList filters(ctx_);
    filters.add_filter({ctx_, TILEDB_FILTER_LZ4});
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a", filters));
    Array::create(array_uri_, schema);

    // Write non-overlapping fragments across row ranges
    uint32_t rows_per_fragment = max_row / num_fragments;
    const unsigned skip = 2;
    for (unsigned f = 0; f < num_fragments; f++) {
      std::vector<uint32_t> fd1, fd2;
      std::vector<int32_t> fdata;
      uint32_t row_start = f * rows_per_fragment + 1;
      uint32_t row_end = row_start + rows_per_fragment;
      for (uint32_t i = row_start; i < row_end; i += skip) {
        for (uint32_t j = 1; j < max_col; j += skip) {
          fd1.push_back(i);
          fd2.push_back(j);
          fdata.push_back(i * max_col + j);
        }
      }

      Array array(ctx_, array_uri_, TILEDB_WRITE);
      Query query(ctx_, array);
      query.set_layout(TILEDB_UNORDERED)
          .set_data_buffer("a", fdata)
          .set_data_buffer("d1", fd1)
          .set_data_buffer("d2", fd2);
      query.submit();
      array.close();
    }
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    auto non_empty = array.non_empty_domain<uint32_t>();
    subarray_ = {
        non_empty[0].second.first,
        non_empty[0].second.second,
        non_empty[1].second.first,
        non_empty[1].second.second};

    // Estimate total cells across all fragments
    uint64_t est_cells = (max_row / 2) * static_cast<uint64_t>(max_col / 2);
    data_.resize(est_cells);
    d1_.resize(est_cells);
    d2_.resize(est_cells);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);
    query.set_subarray(Subarray(ctx_, array).set_subarray(subarray_))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_)
        .set_data_buffer("d1", d1_)
        .set_data_buffer("d2", d2_);
    query.submit();
    array.close();
  }

 private:
  const std::string array_uri_ = bench_uri("bench_array");
  const unsigned tile_extent = 300;
  const unsigned capacity = 100000;
  const unsigned max_row = 5000, max_col = 5000;
  const unsigned num_fragments = 10;

  Context ctx_{bench_config()};
  std::vector<int32_t> data_;
  std::vector<uint32_t> subarray_, d1_, d2_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
