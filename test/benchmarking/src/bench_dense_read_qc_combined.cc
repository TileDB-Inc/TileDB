/**
 * @file   bench_dense_read_qc_combined.cc
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
 * Benchmark dense 1D read with a combined query condition:
 * (a >= num_cells/4) AND (b < num_cells*3/4), filtering across two attributes.
 *
 * For large-scale runs, increase num_cells to 800000000.
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
        Dimension::create<uint64_t>(ctx_, "d1", {{1, num_cells}}, tile_extent));
    schema.set_domain(domain);
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a"));
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "b"));
    Array::create(array_uri_, schema);

    std::vector<int32_t> a_data(num_cells), b_data(num_cells);
    for (uint64_t i = 0; i < num_cells; i++) {
      a_data[i] = i;
      b_data[i] = num_cells - i;
    }

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);
    query
        .set_subarray(
            Subarray(ctx_, array).set_subarray<uint64_t>({1ul, num_cells}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", a_data)
        .set_data_buffer("b", b_data);
    query.submit();
    array.close();
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    a_data_.resize(num_cells);
    b_data_.resize(num_cells);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);

    const int32_t a_val = static_cast<int32_t>(num_cells / 4);
    const int32_t b_val = static_cast<int32_t>(num_cells * 3 / 4);
    QueryCondition qc_a = QueryCondition::create(ctx_, "a", a_val, TILEDB_GE);
    QueryCondition qc_b = QueryCondition::create(ctx_, "b", b_val, TILEDB_LT);
    QueryCondition combined = qc_a.combine(qc_b, TILEDB_AND);

    query
        .set_subarray(
            Subarray(ctx_, array).set_subarray<uint64_t>({1ul, num_cells}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_condition(combined)
        .set_data_buffer("a", a_data_)
        .set_data_buffer("b", b_data_);
    query.submit();
    array.close();
  }

 private:
  const std::string array_uri_ = bench_uri("bench_array");
  const uint64_t num_cells = 100000000;
  const uint64_t tile_extent = 1000000;

  Context ctx_{bench_config()};
  std::vector<int32_t> a_data_, b_data_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
