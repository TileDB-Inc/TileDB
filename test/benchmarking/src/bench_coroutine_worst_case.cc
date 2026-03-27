/**
 * @file   bench_coroutine_worst_case.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * Coroutine pipeline benchmark — WORST CASE scenario.
 *
 * Worst case: single attribute with light compression. The coroutine pipeline
 * has no benefit here because:
 *   - Only 1 attribute means no inter-attribute pipelining opportunity
 *   - LZ4 is fast to decompress, so unfilter time is small
 *   - The async path adds minor overhead (coroutine creation, event signaling)
 *
 * This benchmark verifies that the coroutine overhead is negligible — the
 * async path should be roughly equal to or within noise of the sync path.
 *
 * Run comparison:
 *   ./bench_coroutine_worst_case setup
 *   ./bench_coroutine_worst_case run                 # sync baseline
 *   TILEDB_SM_QUERY_READER_USE_COROUTINE_PIPELINE=true \
 *     ./bench_coroutine_worst_case run               # coroutine pipeline
 *   ./bench_coroutine_worst_case teardown
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
        Dimension::create<uint32_t>(ctx_, "d1", {{1, nrows_}}, tile_extent_));
    domain.add_dimension(
        Dimension::create<uint32_t>(ctx_, "d2", {{1, ncols_}}, tile_extent_));
    schema.set_domain(domain);

    // Light compression — fast unfiltering, I/O dominates.
    FilterList filters(ctx_);
    filters.add_filter({ctx_, TILEDB_FILTER_LZ4});
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a", filters));

    Array::create(array_uri_, schema);

    uint64_t ncells = static_cast<uint64_t>(nrows_) * ncols_;
    data_.resize(ncells);
    for (uint64_t i = 0; i < ncells; i++) {
      data_[i] = static_cast<int32_t>(i);
    }

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);
    query
        .set_subarray(
            Subarray(ctx_, array).set_subarray({1u, nrows_, 1u, ncols_}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_);
    query.submit();
    array.close();
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    uint64_t ncells = static_cast<uint64_t>(nrows_) * ncols_;
    data_.resize(ncells);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);
    query
        .set_subarray(
            Subarray(ctx_, array).set_subarray({1u, nrows_, 1u, ncols_}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_);
    query.submit();
    array.close();
  }

 private:
  const std::string array_uri_ = bench_uri("bench_array");
  static constexpr unsigned nrows_ = 10000;
  static constexpr unsigned ncols_ = 10000;
  static constexpr unsigned tile_extent_ = 1000;

  Context ctx_{bench_config()};
  std::vector<int32_t> data_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
