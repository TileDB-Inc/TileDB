/**
 * @file   bench_sparse_string_dim_read.cc
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
 * Benchmark sparse 1D array read with a STRING_ASCII dimension.
 * Reads a range covering approximately half of all keys.
 *
 * For large-scale runs, increase num_cells to 10000000.
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
    domain.add_dimension(
        Dimension::create(ctx_, "d1", TILEDB_STRING_ASCII, nullptr, nullptr));
    schema.set_domain(domain);
    schema.set_capacity(capacity);
    schema.add_attribute(Attribute::create<int32_t>(ctx_, "a"));
    Array::create(array_uri_, schema);

    // Write data during setup
    std::string wdim_data;
    std::vector<uint64_t> wdim_offsets;
    std::vector<int32_t> wattr_data(num_cells);
    for (uint64_t i = 0; i < num_cells; i++) {
      wdim_offsets.push_back(wdim_data.size());
      std::string key = "key_";
      std::string num = std::to_string(i);
      while (num.size() < 7)
        num = "0" + num;
      key += num;
      wdim_data += key;
      wattr_data[i] = static_cast<int32_t>(i);
    }

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array);
    query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("d1", wdim_data)
        .set_offsets_buffer("d1", wdim_offsets)
        .set_data_buffer("a", wattr_data);
    query.submit();
    array.close();
  }

  virtual void teardown() {
    bench_teardown(ctx_, array_uri_);
  }

  virtual void pre_run() {
    // Allocate read buffers generously
    dim_data_.resize(num_cells * 12);
    dim_offsets_.resize(num_cells);
    attr_data_.resize(num_cells);
  }

  virtual void run() {
    Array array(ctx_, array_uri_, TILEDB_READ);
    // Read range covering the first half of keys
    std::string lo = "key_0000000";
    std::string hi = "key_" + std::to_string(num_cells / 2 - 1);
    while (hi.size() < 11)
      hi.insert(4, 1, '0');

    Subarray subarray(ctx_, array);
    subarray.add_range(0, lo, hi);

    Query query(ctx_, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("d1", dim_data_)
        .set_offsets_buffer("d1", dim_offsets_)
        .set_data_buffer("a", attr_data_);
    query.submit();
    array.close();
  }

 private:
  const std::string array_uri_ = bench_uri("bench_array");
  const uint64_t num_cells = 2000000;
  const uint64_t capacity = 100000;

  Context ctx_{bench_config()};
  std::string dim_data_;
  std::vector<uint64_t> dim_offsets_;
  std::vector<int32_t> attr_data_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
