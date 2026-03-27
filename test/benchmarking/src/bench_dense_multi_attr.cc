/**
 * @file   bench_dense_multi_attr.cc
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
 * Benchmark dense 2D read performance with 5 attributes of different types:
 * int32, float64, int64, uint16, int8. Setup writes data, run reads all
 * attributes.
 *
 * For large-scale runs, increase array_rows and array_cols to 20000.
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
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a1", filters));
    schema.add_attribute(Attribute::create<double>(ctx_, "a2", filters));
    schema.add_attribute(Attribute::create<int64_t>(ctx_, "a3", filters));
    schema.add_attribute(Attribute::create<uint16_t>(ctx_, "a4", filters));
    schema.add_attribute(Attribute::create<int8_t>(ctx_, "a5", filters));
    Array::create(array_uri_, schema);

    uint64_t ncells = array_rows * array_cols;
    a1_.resize(ncells);
    a2_.resize(ncells);
    a3_.resize(ncells);
    a4_.resize(ncells);
    a5_.resize(ncells);
    for (uint64_t i = 0; i < ncells; i++) {
      a1_[i] = static_cast<int32_t>(i);
      a2_[i] = static_cast<double>(i) * 1.1;
      a3_[i] = static_cast<int64_t>(i) * 100;
      a4_[i] = static_cast<uint16_t>(i % 65536);
      a5_[i] = static_cast<int8_t>(i % 128);
    }

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);
    query
        .set_subarray(Subarray(ctx_, array)
                          .set_subarray({1u, array_rows, 1u, array_cols}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", a1_)
        .set_data_buffer("a2", a2_)
        .set_data_buffer("a3", a3_)
        .set_data_buffer("a4", a4_)
        .set_data_buffer("a5", a5_);
    query.submit();
    array.close();
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    uint64_t ncells = array_rows * array_cols;
    a1_.resize(ncells);
    a2_.resize(ncells);
    a3_.resize(ncells);
    a4_.resize(ncells);
    a5_.resize(ncells);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);
    query
        .set_subarray(Subarray(ctx_, array)
                          .set_subarray({1u, array_rows, 1u, array_cols}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", a1_)
        .set_data_buffer("a2", a2_)
        .set_data_buffer("a3", a3_)
        .set_data_buffer("a4", a4_)
        .set_data_buffer("a5", a5_);
    query.submit();
    array.close();
  }

 private:
  const std::string array_uri_ = bench_uri("bench_array");
  const unsigned array_rows = 5000, array_cols = 5000;
  const unsigned tile_extent = 500;

  Context ctx_{bench_config()};
  std::vector<int32_t> a1_;
  std::vector<double> a2_;
  std::vector<int64_t> a3_;
  std::vector<uint16_t> a4_;
  std::vector<int8_t> a5_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
