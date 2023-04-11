/**
 * @file   bench_reader_base_unfilter_tile.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * Benchmark ReaderBase::unfilter_tile
 */

#include <numeric>

#include <tiledb/tiledb>

#include "benchmark.h"

using namespace tiledb;

class Benchmark : public BenchmarkBase {
 protected:
  virtual void setup() {
    auto dim = Dimension::create<uint32_t>(ctx_, "d", {{1, max_rows_}}, 1000);
    auto attr = Attribute::create<uint32_t>(ctx_, "a");

    Domain dom(ctx_);
    dom.add_dimension(dim);

    ArraySchema schema(ctx_, TILEDB_SPARSE);
    schema.set_domain(dom)
        .add_attribute(attr)
        .set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}})
        .set_capacity(1000);

    Array::create(uri_, schema);

    dim_data_.resize(max_rows_);
    std::iota(dim_data_.begin(), dim_data_.end(), 1);

    attr_data_.resize(max_rows_);
    std::iota(attr_data_.begin(), attr_data_.end(), 1);

    Array array_w(ctx_, uri_, TILEDB_WRITE);
    Query query_w(ctx_, array_w, TILEDB_WRITE);
    query_w.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("d", dim_data_)
        .set_data_buffer("a", attr_data_);
    query_w.submit();
    array_w.close();
  }

  virtual void teardown() {
    VFS vfs(ctx_);
    if (vfs.is_dir(uri_)) {
      vfs.remove_dir(uri_);
    }
  }

  virtual void pre_run() {
    dim_data_.resize(max_rows_);
    attr_data_.resize(max_rows_);
  }

  virtual void run() {
    for (int i = 0; i < 5; i++) {
      Array array(ctx_, uri_, TILEDB_READ);
      auto ned = array.non_empty_domain<uint32_t>();
      std::vector<uint32_t> subarray = {
          ned[0].second.first,
          ned[0].second.second,
          ned[1].second.first,
          ned[1].second.second};

      Query query(ctx_, array);
      query.set_layout(TILEDB_ROW_MAJOR)
          .set_subarray(Subarray(ctx_, array).set_subarray(subarray))
          .set_data_buffer("d", dim_data_)
          .set_data_buffer("a", attr_data_);
      query.submit();
      array.close();
    }
  }

 private:
  const std::string uri_ = "bench_reader_base_unfilter_tiles";
  const uint32_t max_rows_ = 5000;

  Context ctx_;

  std::vector<uint32_t> dim_data_;
  std::vector<uint32_t> attr_data_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
