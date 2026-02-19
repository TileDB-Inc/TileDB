/**
 * @file   bench_sparse_multi_attr.cc
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
 * Benchmark sparse 2D read performance with 5 attributes of different types:
 * int32, float64, int64, uint16, int8. Setup writes data, run reads all
 * attributes.
 *
 * For large-scale runs, increase max_row and max_col to 15000.
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
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a1", filters));
    schema.add_attribute(Attribute::create<double>(ctx_, "a2", filters));
    schema.add_attribute(Attribute::create<int64_t>(ctx_, "a3", filters));
    schema.add_attribute(Attribute::create<uint16_t>(ctx_, "a4", filters));
    schema.add_attribute(Attribute::create<int8_t>(ctx_, "a5", filters));
    Array::create(array_uri_, schema);

    const unsigned skip = 2;
    for (uint32_t i = 1; i < max_row; i += skip) {
      for (uint32_t j = 1; j < max_col; j += skip) {
        d1_.push_back(i);
        d2_.push_back(j);
      }
    }

    uint64_t ncells = d1_.size();
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
    Query query(ctx_, array);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a1", a1_)
        .set_data_buffer("a2", a2_)
        .set_data_buffer("a3", a3_)
        .set_data_buffer("a4", a4_)
        .set_data_buffer("a5", a5_)
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

    uint64_t ncells =
        ((max_row - 1) / 2) * static_cast<uint64_t>((max_col - 1) / 2);
    a1_.resize(ncells);
    a2_.resize(ncells);
    a3_.resize(ncells);
    a4_.resize(ncells);
    a5_.resize(ncells);
    d1_.resize(ncells);
    d2_.resize(ncells);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);
    query.set_subarray(Subarray(ctx_, array).set_subarray(subarray_))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a1", a1_)
        .set_data_buffer("a2", a2_)
        .set_data_buffer("a3", a3_)
        .set_data_buffer("a4", a4_)
        .set_data_buffer("a5", a5_)
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
  std::vector<int32_t> a1_;
  std::vector<double> a2_;
  std::vector<int64_t> a3_;
  std::vector<uint16_t> a4_;
  std::vector<int8_t> a5_;
  std::vector<uint32_t> subarray_, d1_, d2_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
