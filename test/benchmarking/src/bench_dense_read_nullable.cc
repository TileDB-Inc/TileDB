/**
 * @file   bench_dense_read_nullable.cc
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
 * Benchmark dense 1D read performance with a nullable INT32 attribute.
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

    auto attr = Attribute::create<int32_t>(ctx_, "a");
    attr.set_nullable(true);
    schema.add_attribute(attr);
    Array::create(array_uri_, schema);

    // Write nullable data during setup
    std::vector<int32_t> write_data(num_cells);
    std::vector<uint8_t> write_validity(num_cells);
    for (uint64_t i = 0; i < num_cells; i++) {
      write_data[i] = i;
      write_validity[i] = (i % 2 == 0) ? 1 : 0;
    }

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array, TILEDB_WRITE);
    query
        .set_subarray(
            Subarray(ctx_, array).set_subarray<uint64_t>({1ul, num_cells}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", write_data)
        .set_validity_buffer("a", write_validity);
    query.submit();
    array.close();
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    data_.resize(num_cells);
    validity_.resize(num_cells);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    Query query(ctx_, array);
    query
        .set_subarray(
            Subarray(ctx_, array).set_subarray<uint64_t>({1ul, num_cells}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_)
        .set_validity_buffer("a", validity_);
    query.submit();
    array.close();
  }

 private:
  const std::string array_uri_ = bench_uri("bench_array");
  const uint64_t num_cells = 100000000;
  const uint64_t tile_extent = 1000000;

  Context ctx_{bench_config()};
  std::vector<int32_t> data_;
  std::vector<uint8_t> validity_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
