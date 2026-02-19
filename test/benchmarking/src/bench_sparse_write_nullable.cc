/**
 * @file   bench_sparse_write_nullable.cc
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
 * Benchmark sparse 2D write performance with a nullable INT32 attribute.
 * Approximately 50% of cells are null.
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

    auto attr = Attribute::create<int32_t>(ctx_, "a");
    attr.set_nullable(true);
    schema.add_attribute(attr);
    Array::create(array_uri_, schema);
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    d1_.clear();
    d2_.clear();
    data_.clear();
    validity_.clear();

    const unsigned skip = 2;
    for (uint32_t i = 1; i < max_row; i += skip) {
      for (uint32_t j = 1; j < max_col; j += skip) {
        d1_.push_back(i);
        d2_.push_back(j);
      }
    }

    data_.resize(d1_.size());
    validity_.resize(d1_.size());
    for (uint64_t i = 0; i < data_.size(); i++) {
      data_[i] = i;
      validity_[i] = (i % 2 == 0) ? 1 : 0;
    }
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", data_)
        .set_validity_buffer("a", validity_)
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
  std::vector<int32_t> data_;
  std::vector<uint8_t> validity_;
  std::vector<uint32_t> d1_, d2_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
