/**
 * @file   bench_large_io.cc
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
 * Benchmark IO on dense/sparse arrays with fixed/var-sized attributes.
 */

#include <tiledb/tiledb>

#include "benchmark.h"

using namespace tiledb;

class Benchmark : public BenchmarkBase {
 protected:
  virtual void setup() {
    FilterList filters(ctx_);
    filters.add_filter({ctx_, TILEDB_FILTER_BYTESHUFFLE})
        .add_filter({ctx_, TILEDB_FILTER_BZIP2});

    // Setup the dense array.
    ArraySchema d_schema(ctx_, TILEDB_DENSE);
    Domain d_domain(ctx_);
    d_domain.add_dimension(Dimension::create<uint32_t>(
        ctx_, "d1", {{1, dense_array_rows}}, tile_rows));
    d_domain.add_dimension(Dimension::create<uint32_t>(
        ctx_, "d2", {{1, dense_array_cols}}, tile_cols));
    d_schema.set_domain(d_domain);
    d_schema.add_attribute(Attribute::create<int32_t>(ctx_, "a", filters));
    d_schema.add_attribute(Attribute::create<int32_t>(ctx_, "b"));
    d_schema.add_attribute(
        Attribute::create<std::vector<int>>(ctx_, "c", filters));
    Array::create(dense_array_uri_, d_schema);

    data_a_.resize(dense_array_rows * dense_array_cols);
    data_b_.resize(data_a_.size());
    off_c_.resize(data_a_.size());
    data_c_.resize(data_a_.size() * 2);
    for (uint64_t i = 0; i < data_a_.size(); i++) {
      data_a_[i] = 1 * i;
      data_b_[i] = 2 * i;
      off_c_[i] = (2 * i) * sizeof(int);
      data_c_[2 * i] = i;
      data_c_[(2 * i) + 1] = i;
    }

    // Setup the sparse array.
    ArraySchema s_schema(ctx_, TILEDB_SPARSE);
    Domain s_domain(ctx_);
    s_domain.add_dimension(Dimension::create<uint32_t>(
        ctx_,
        "d1",
        {{1, std::numeric_limits<uint32_t>::max() - tile_rows}},
        tile_rows));
    s_domain.add_dimension(Dimension::create<uint32_t>(
        ctx_,
        "d2",
        {{1, std::numeric_limits<uint32_t>::max() - tile_cols}},
        tile_cols));
    s_schema.set_domain(s_domain);
    s_schema.add_attribute(Attribute::create<int32_t>(ctx_, "a", filters));
    s_schema.add_attribute(Attribute::create<int32_t>(ctx_, "b"));
    s_schema.add_attribute(
        Attribute::create<std::vector<int>>(ctx_, "c", filters));
    Array::create(sparse_array_uri_, s_schema);

    // RNG coords are expensive to generate. Just make the data "sparse"
    // by skipping a few cells between each nonempty cell.
    const unsigned skip = 2;
    for (uint32_t i = 1; i < sparse_max_row; i += skip) {
      for (uint32_t j = 1; j < sparse_max_col; j += skip) {
        sparse_coords_.push_back(i);
        sparse_coords_.push_back(j);
      }
    }
  }

  virtual void teardown() {
    VFS vfs(ctx_);
    if (vfs.is_dir(dense_array_uri_))
      vfs.remove_dir(dense_array_uri_);
    if (vfs.is_dir(sparse_array_uri_))
      vfs.remove_dir(sparse_array_uri_);
  }

  virtual void pre_run() {
  }

  virtual void run() {
    // Write the dense array 1 time.
    Array d_write_array(ctx_, dense_array_uri_, TILEDB_WRITE);
    Query d_write_query(ctx_, d_write_array, TILEDB_WRITE);
    d_write_query
        .set_subarray(
            Subarray(ctx_, d_write_array)
                .set_subarray({1u, dense_array_rows, 1u, dense_array_cols}))
        .set_layout(TILEDB_ROW_MAJOR)
        .set_data_buffer("a", data_a_)
        .set_data_buffer("b", data_b_)
        .set_data_buffer("c", data_c_)
        .set_offsets_buffer("c", off_c_);
    d_write_query.submit();
    d_write_array.close();

    // Write the sparse array 1 time.
    Array s_write_array(ctx_, sparse_array_uri_, TILEDB_WRITE);
    Query s_write_query(ctx_, s_write_array);
    s_write_query.set_layout(TILEDB_UNORDERED)
        .set_data_buffer("a", data_a_)
        .set_data_buffer("b", data_b_)
        .set_data_buffer("c", data_c_)
        .set_offsets_buffer("c", off_c_)
        .set_coordinates(sparse_coords_);
    s_write_query.submit();
    s_write_array.close();

    // Read the entire dense array 2 times.
    for (int i = 0; i < 2; ++i) {
      Array array(ctx_, dense_array_uri_, TILEDB_READ);
      Query query(ctx_, array);
      query
          .set_subarray(
              Subarray(ctx_, array)
                  .set_subarray({1u, dense_array_rows, 1u, dense_array_cols}))
          .set_layout(TILEDB_ROW_MAJOR)
          .set_data_buffer("a", data_a_)
          .set_data_buffer("b", data_b_)
          .set_data_buffer("c", data_c_)
          .set_offsets_buffer("c", off_c_);
      query.submit();
      array.close();
    }

    // Read the entire sparse array 2 times.
    for (int i = 0; i < 2; ++i) {
      Array array(ctx_, sparse_array_uri_, TILEDB_READ);
      auto non_empty = array.non_empty_domain<uint32_t>();
      std::vector<uint32_t> subarray = {
          non_empty[0].second.first,
          non_empty[0].second.second,
          non_empty[1].second.first,
          non_empty[1].second.second};

      Query query(ctx_, array);
      data_a_.resize(query.est_result_size("a"));
      sparse_coords_.resize(query.est_result_size("TILEDB_COORDS"));
      query.set_subarray(Subarray(ctx_, array).set_subarray(subarray))
          .set_layout(TILEDB_ROW_MAJOR)
          .set_data_buffer("a", data_a_)
          .set_data_buffer("b", data_b_)
          .set_data_buffer("c", data_c_)
          .set_offsets_buffer("c", off_c_)
          .set_coordinates(sparse_coords_);
      query.submit();
      array.close();
    }
  }

 private:
  const std::string dense_array_uri_ = "dense_bench_array";
  const std::string sparse_array_uri_ = "sparse_bench_array";
  const unsigned dense_array_rows = 6000, dense_array_cols = 6000;
  const unsigned sparse_max_row = 12000, sparse_max_col = 12000;
  const unsigned tile_rows = 2000, tile_cols = 2000;

  Context ctx_;

  std::vector<int> data_a_;
  std::vector<int> data_b_;
  std::vector<uint64_t> off_c_;
  std::vector<int> data_c_;

  std::vector<uint32_t> sparse_coords_;
};

int main(int argc, char** argv) {
  Benchmark bench;
  return bench.main(argc, argv);
}
