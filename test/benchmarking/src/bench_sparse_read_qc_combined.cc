/**
 * @file   bench_sparse_read_qc_combined.cc
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
 * Benchmark sparse 2D read with a combined query condition:
 * (a >= N/4) AND (b < N*3/4), filtering across two attributes.
 *
 * For large-scale runs, increase max_row and max_col to 20000.
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
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a"));
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "b"));
    Array::create(array_uri_, schema);

    const unsigned skip = 2;
    for (uint32_t i = 1; i < max_row; i += skip) {
      for (uint32_t j = 1; j < max_col; j += skip) {
        d1_.push_back(i);
        d2_.push_back(j);
      }
    }

    a_data_.resize(d1_.size());
    b_data_.resize(d1_.size());
    for (uint64_t i = 0; i < a_data_.size(); i++) {
      a_data_[i] = i;
      b_data_[i] = a_data_.size() - i;
    }

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", a_data_)
        .set_data_buffer("b", b_data_)
        .set_data_buffer("d1", d1_)
        .set_data_buffer("d2", d2_);
    query.submit();
    array.close();
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

    uint64_t num_cells =
        ((max_row - 1) / 2) * static_cast<uint64_t>((max_col - 1) / 2);
    a_data_.resize(num_cells);
    b_data_.resize(num_cells);
    d1_.resize(num_cells);
    d2_.resize(num_cells);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);

    const int32_t a_val = static_cast<int32_t>(a_data_.size() / 4);
    const int32_t b_val = static_cast<int32_t>(a_data_.size() * 3 / 4);
    QueryCondition qc_a = QueryCondition::create(ctx_, "a", a_val, TILEDB_GE);
    QueryCondition qc_b = QueryCondition::create(ctx_, "b", b_val, TILEDB_LT);
    QueryCondition combined = qc_a.combine(qc_b, TILEDB_AND);

    query.set_subarray(Subarray(ctx_, array).set_subarray(subarray_))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_condition(combined)
        .set_data_buffer("a", a_data_)
        .set_data_buffer("b", b_data_)
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

  Context ctx_{bench_config()};
  std::vector<int32_t> a_data_, b_data_;
  std::vector<uint32_t> subarray_, d1_, d2_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
