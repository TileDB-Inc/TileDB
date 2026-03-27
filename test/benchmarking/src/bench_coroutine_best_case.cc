/**
 * @file   bench_coroutine_best_case.cc
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
 * Coroutine pipeline benchmark — BEST CASE scenario.
 *
 * Best case: many attributes with heavy compression. The coroutine pipeline
 * benefits most here because:
 *   - 10 attributes means 10 independent I/O + unfilter streams
 *   - ZSTD compression makes unfiltering CPU-intensive
 *   - While attribute N is unfiltering, attributes N+1..N+K can be doing I/O
 *   - The sync path forces ALL I/O to complete before ANY unfiltering starts
 *
 * Run comparison:
 *   ./bench_coroutine_best_case setup
 *   ./bench_coroutine_best_case run                 # sync baseline
 *   TILEDB_SM_QUERY_READER_USE_COROUTINE_PIPELINE=true \
 *     ./bench_coroutine_best_case run               # coroutine pipeline
 *   ./bench_coroutine_best_case teardown
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

    // Heavy compression to maximize unfilter CPU time.
    FilterList filters(ctx_);
    filters.add_filter({ctx_, TILEDB_FILTER_BYTESHUFFLE})
        .add_filter({ctx_, TILEDB_FILTER_ZSTD});

    // 10 attributes of varying types.
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a1", filters));
    schema.add_attribute(Attribute::create<double>(ctx_, "a2", filters));
    schema.add_attribute(Attribute::create<int64_t>(ctx_, "a3", filters));
    schema.add_attribute(Attribute::create<uint16_t>(ctx_, "a4", filters));
    schema.add_attribute(Attribute::create<int8_t>(ctx_, "a5", filters));
    schema.add_attribute(Attribute::create<float>(ctx_, "a6", filters));
    schema.add_attribute(Attribute::create<uint32_t>(ctx_, "a7", filters));
    schema.add_attribute(Attribute::create<int16_t>(ctx_, "a8", filters));
    schema.add_attribute(Attribute::create<uint64_t>(ctx_, "a9", filters));
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a10", filters));

    Array::create(array_uri_, schema);

    uint64_t ncells = static_cast<uint64_t>(nrows_) * ncols_;
    a1_.resize(ncells);
    a2_.resize(ncells);
    a3_.resize(ncells);
    a4_.resize(ncells);
    a5_.resize(ncells);
    a6_.resize(ncells);
    a7_.resize(ncells);
    a8_.resize(ncells);
    a9_.resize(ncells);
    a10_.resize(ncells);
    for (uint64_t i = 0; i < ncells; i++) {
      a1_[i] = static_cast<int32_t>(i);
      a2_[i] = static_cast<double>(i) * 1.1;
      a3_[i] = static_cast<int64_t>(i) * 100;
      a4_[i] = static_cast<uint16_t>(i % 65536);
      a5_[i] = static_cast<int8_t>(i % 128);
      a6_[i] = static_cast<float>(i) * 0.5f;
      a7_[i] = static_cast<uint32_t>(i);
      a8_[i] = static_cast<int16_t>(i % 32768);
      a9_[i] = static_cast<uint64_t>(i) * 10;
      a10_[i] = static_cast<int32_t>(ncells - i);
    }

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);
    query
        .set_subarray(
            Subarray(ctx_, array).set_subarray({1u, nrows_, 1u, ncols_}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", a1_)
        .set_data_buffer("a2", a2_)
        .set_data_buffer("a3", a3_)
        .set_data_buffer("a4", a4_)
        .set_data_buffer("a5", a5_)
        .set_data_buffer("a6", a6_)
        .set_data_buffer("a7", a7_)
        .set_data_buffer("a8", a8_)
        .set_data_buffer("a9", a9_)
        .set_data_buffer("a10", a10_);
    query.submit();
    array.close();
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    uint64_t ncells = static_cast<uint64_t>(nrows_) * ncols_;
    a1_.resize(ncells);
    a2_.resize(ncells);
    a3_.resize(ncells);
    a4_.resize(ncells);
    a5_.resize(ncells);
    a6_.resize(ncells);
    a7_.resize(ncells);
    a8_.resize(ncells);
    a9_.resize(ncells);
    a10_.resize(ncells);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);
    query
        .set_subarray(
            Subarray(ctx_, array).set_subarray({1u, nrows_, 1u, ncols_}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", a1_)
        .set_data_buffer("a2", a2_)
        .set_data_buffer("a3", a3_)
        .set_data_buffer("a4", a4_)
        .set_data_buffer("a5", a5_)
        .set_data_buffer("a6", a6_)
        .set_data_buffer("a7", a7_)
        .set_data_buffer("a8", a8_)
        .set_data_buffer("a9", a9_)
        .set_data_buffer("a10", a10_);
    query.submit();
    array.close();
  }

 private:
  const std::string array_uri_ = bench_uri("bench_array");
  static constexpr unsigned nrows_ = 8000;
  static constexpr unsigned ncols_ = 8000;
  static constexpr unsigned tile_extent_ = 500;

  Context ctx_{bench_config()};
  std::vector<int32_t> a1_;
  std::vector<double> a2_;
  std::vector<int64_t> a3_;
  std::vector<uint16_t> a4_;
  std::vector<int8_t> a5_;
  std::vector<float> a6_;
  std::vector<uint32_t> a7_;
  std::vector<int16_t> a8_;
  std::vector<uint64_t> a9_;
  std::vector<int32_t> a10_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
