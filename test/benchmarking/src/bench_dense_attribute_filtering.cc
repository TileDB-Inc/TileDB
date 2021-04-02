/**
 * @file   bench_dense_attribute_filtering.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * Benchmarks a query condition that filters out the first half of all cells
 * values using a single less-than clause.
 */

#include <tiledb/tiledb>

#include "benchmark.h"

using namespace tiledb;

class Benchmark : public BenchmarkBase {
 protected:
  virtual void setup() {
    ArraySchema schema(ctx_, TILEDB_DENSE);
    Domain domain(ctx_);
    domain.add_dimension(
        Dimension::create<uint64_t>(ctx_, "d1", {{1, array_rows}}, array_rows));
    schema.set_domain(domain);
    FilterList filters(ctx_);
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a", filters));
    Array::create(array_uri_, schema);

    data_.resize(array_rows);
    for (uint64_t i = 0; i < data_.size(); i++) {
      data_[i] = i;
    }

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);

    std::array<uint64_t, 2> subarray = {1ul, array_rows};
    query.set_subarray({subarray})
        .set_layout(TILEDB_ROW_MAJOR)
        .set_buffer("a", data_);
    query.submit();
    array.close();
  }

  virtual void teardown() {
    VFS vfs(ctx_);
    if (vfs.is_dir(array_uri_))
      vfs.remove_dir(array_uri_);
  }

  virtual void pre_run() {
    data_.resize(array_rows);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);
    std::array<uint64_t, 2> subarray = {1ul, array_rows};
    const int cmp_value = array_rows / 2;
    QueryCondition condition =
        QueryCondition::create(ctx_, "a", cmp_value, TILEDB_LT);
    query.set_subarray({subarray})
        .set_layout(TILEDB_ROW_MAJOR)
        .set_condition(condition)
        .set_buffer("a", data_);
    auto st = query.submit();
    array.close();
  }

 private:
  const std::string array_uri_ = "bench_array";

  // 3.2GB for a single cell, 4-byte attribute.
  const uint64_t array_rows = 800000000;

  Context ctx_;
  std::vector<int> data_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
