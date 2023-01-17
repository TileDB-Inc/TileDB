/**
 * @file unit-cppapi-global-order-writes-remote.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests for global order remote writes.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/tiledb"

#include <cstring>
#include <iostream>

using namespace tiledb;

enum { KB = 1024, MB = KB * 1024, GB = MB * 1024 };

const std::string aws_region = "us-east-2";
const std::string aws_key_id = "<AWS_KEY_ID>";
const std::string aws_secret_key = "<AWS_SECRET_KEY>";

// Toggle remote queries for sanity check.
constexpr bool remote = true;

template <typename T>
struct RemoteGlobalOrderWriteFx {
  RemoteGlobalOrderWriteFx(
      uint64_t total_cells,
      uint64_t extent,
      uint64_t submit_cell_count,
      bool is_var = true,
      bool is_nullable = true)
      : is_var_(is_var)
      , is_nullable_(is_nullable)
      , submit_cell_count_(submit_cell_count)
      , total_cell_count_(total_cells)
      , extent_(extent) {
    if constexpr (remote) {
      // Config for using local REST server.
      Config localConfig;
      localConfig.set("rest.username", "shaunreed");
      localConfig.set("rest.password", "demodemo");
      localConfig.set("rest.server_address", "http://localhost:8181");
      localConfig.set("vfs.s3.region", aws_region);
      localConfig.set("vfs.s3.aws_access_key_id", aws_key_id);
      localConfig.set("vfs.s3.aws_secret_access_key", aws_secret_key);
      ctx_ = Context(localConfig);
      auto type = Object::object(ctx_, array_uri_).type();
      if (type == Object::Type::Array) {
        delete_array();
      }
      VFS vfs(ctx_);
      if (vfs.is_dir(s3_uri_)) {
        vfs.remove_dir(s3_uri_);
      }
    } else {
      VFS vfs(ctx_);
      if (vfs.is_dir(array_uri_)) {
        vfs.remove_dir(array_uri_);
      }
    }
  }

  ~RemoteGlobalOrderWriteFx() {
    delete_array();
  }

  void delete_array() {
    // Delete the array.
    Array array(ctx_, array_uri_, TILEDB_MODIFY_EXCLUSIVE);
    array.delete_array(array_uri_);
  }

  // Create a simple dense array
  void create_array() {
    Domain domain(ctx_);
    domain.add_dimension(Dimension::create<uint64_t>(
        ctx_, "cols", {{1, total_cell_count_}}, extent_));

    auto array_type = GENERATE(TILEDB_DENSE, TILEDB_SPARSE);

    ArraySchema schema(ctx_, array_type);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    if (array_type == TILEDB_SPARSE) {
      schema.set_capacity(extent_);
    }

    auto a1 = Attribute::create<T>(ctx_, "a");
    auto a2 = Attribute::create<std::string>(ctx_, "var");
    if (is_nullable_) {
      a1.set_nullable(true);
      a2.set_nullable(true);
    }
    schema.add_attribute(a1);
    if (is_var_) {
      schema.add_attribute(a2);
    }

    Array::create(array_uri_, schema);

    Array array(ctx_, array_uri_, TILEDB_READ);
    CHECK(array.schema().array_type() == array_type);
    CHECK(
        array.schema()
            .domain()
            .dimension(0)
            .template domain<uint64_t>()
            .second == total_cell_count_);
    if (array_type == TILEDB_SPARSE) {
      CHECK(array.schema().capacity() == extent_);
    }
    array.close();
  }

  void write_array() {
    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array);
    query.set_layout(TILEDB_GLOBAL_ORDER);

    auto is_sparse = array.schema().array_type() == TILEDB_SPARSE;

    // Generate some data to write to the array.
    std::vector<T> data(submit_cell_count_);
    std::iota(data.begin(), data.end(), 0);

    std::vector<uint8_t> validity_buffer(submit_cell_count_, 0);
    for (size_t i = 0; i < validity_buffer.size(); i += 2) {
      validity_buffer[i] = 1;
    }

    char char_data = 'a';
    // Start at column coordinate 1
    uint64_t cols_start = 1;
    // Resubmit until we reach total mbs requested
    for (uint64_t i = 0; i < total_cell_count_; i += submit_cell_count_) {
      // Handle coords for sparse case.
      std::vector<uint64_t> cols(submit_cell_count_);
      if (is_sparse) {
        std::iota(cols.begin(), cols.end(), cols_start);
        query.set_data_buffer("cols", cols);
      }

      // Fixed sized attribute.
      query.set_data_buffer("a", data);
      if (is_nullable_) {
        query.set_validity_buffer("a", validity_buffer);
      }

      // Variable sized attribute.
      std::string var_data(submit_cell_count_ * sizeof(T), char_data++);
      char_data = char_data > 'z' ? 'a' : char_data;
      std::vector<uint64_t> var_offsets;
      if (is_var_) {
        // Generate offsets for variable sized attribute.
        uint64_t max_step = var_data.size() / submit_cell_count_;
        uint64_t j = 0;  // + (i * sizeof(T));
        var_offsets.resize(submit_cell_count_, j);
        std::generate_n(var_offsets.begin() + 1, var_offsets.size() - 1, [&]() {
          j += max_step--;
          max_step =
              max_step < 1 ? var_data.size() / var_offsets.size() : max_step;
          return j;
        });

        query.set_data_buffer("var", var_data)
            .set_offsets_buffer("var", var_offsets);
        if (is_nullable_) {
          query.set_validity_buffer("var", validity_buffer);
        }
      }

      // Submit intermediate queries up to the final submission.
      if (i + submit_cell_count_ < total_cell_count_) {
        query.submit();
      } else {
        // IMPORTANT: Submit final write query and close the array.
        // We must do this within loop; Else our buffers will be out of scope.
        query.submit_and_finalize();
      }

      // Use result buffer elements to ensure we submit enough data after trim.
      auto result = query.result_buffer_elements();

      data_wrote_.insert(
          data_wrote_.end(), data.begin(), data.end());
      if (is_sparse) {
        cols_wrote_.insert(
            cols_wrote_.end(), cols.begin(), cols.end());
        // Pick up where we left off for next iteration of coords.
        cols_start = cols_wrote_.back() + 1;
      }
      if (is_nullable_) {
        data_validity_wrote_.insert(
            data_validity_wrote_.end(),
            validity_buffer.begin(),
            validity_buffer.end());
      }
      if (is_var_) {
        // Update data and offsets wrote for variable size attributes.
        var_data_wrote_ += var_data.substr(0, result["var"].second);
        var_offsets_wrote_.insert(
            var_offsets_wrote_.end(),
            var_offsets.begin(),
            var_offsets.end());
        // Update validity buffer wrote for variable size attributes.
        if (is_nullable_) {
          var_validity_wrote_.insert(
              var_validity_wrote_.end(),
              validity_buffer.begin(),
              validity_buffer.end());
        }
      }
    }
    CHECK(query.query_status() == Query::Status::COMPLETE);
    array.close();

    // Finalize data wrote buffers to reflect total trimmed cells.
    data_wrote_.resize(total_cell_count_);
    if (is_sparse) {
      cols_wrote_.resize(total_cell_count_);
    }
  }

  void read_array() {
    Array array(ctx_, array_uri_, TILEDB_READ);

    // Read the entire array.
    auto c = array.schema().domain().dimension("cols").domain<uint64_t>();
    Subarray subarray(ctx_, array);
    subarray.add_range("cols", c.first, c.second);

    Query query(ctx_, array);
    query.set_subarray(subarray).set_layout(TILEDB_GLOBAL_ORDER);

    // Fixed sized attribute buffers.
    std::vector<T> data(total_cell_count_);

    // Coordinate buffers.
    std::vector<uint64_t> cols;
    if (array.schema().array_type() == TILEDB_SPARSE) {
      cols.resize(total_cell_count_);
      query.set_data_buffer("cols", cols);
    }

    // Variable sized attribute buffers.
    std::string var_data;
    std::vector<uint64_t> var_offsets;

    std::vector<uint8_t> data_validity;
    std::vector<uint8_t> var_validity;

    query.set_data_buffer("a", data);
    if (is_nullable_) {
      data_validity.resize(total_cell_count_);
      query.set_validity_buffer("a", data_validity);
    }

    if (is_var_) {
      var_data.resize(var_data_wrote_.size());
      var_offsets.resize(total_cell_count_);
      query.set_data_buffer("var", var_data)
          .set_offsets_buffer("var", var_offsets);
      if (is_nullable_) {
        var_validity.resize(total_cell_count_);
        query.set_validity_buffer("var", var_validity);
      }
    }

    query.submit();

    if (is_nullable_) {
      CHECK_THAT(data_validity, Catch::Matchers::Equals(data_validity_wrote_));
    }

    if (is_var_) {
      CHECK(var_data == var_data_wrote_);
      CHECK_THAT(var_offsets, Catch::Matchers::Equals(var_offsets_wrote_));

      if (is_nullable_) {
        CHECK_THAT(var_validity, Catch::Matchers::Equals(var_validity_wrote_));
      }
    }

    CHECK_THAT(data, Catch::Matchers::Equals(data_wrote_));

    if (array.schema().array_type() == TILEDB_SPARSE) {
      CHECK_THAT(cols, Catch::Matchers::Equals(cols_wrote_));
    }

    CHECK(query.query_status() == Query::Status::COMPLETE);
    array.close();
  }

  void run_test() {
    create_array();
    write_array();
    read_array();
  }

  Context ctx_;
  bool is_var_;
  bool is_nullable_;
  const unsigned submit_cell_count_;
  // Find min number of values of type T to fill `bytes_`.
  const uint64_t total_cell_count_;
  const uint64_t extent_;

  const std::string array_name_ =
      "global-array-" + std::to_string(total_cell_count_);
  const std::string s3_uri_ = "s3://shaun.reed/arrays/" + array_name_;
  const std::string array_uri_ =
      remote ? "tiledb://shaunreed/" + s3_uri_ : array_name_;

  // Vectors to store all the data wrote to the array.
  // + We will use these vectors to validate subsequent read.
  std::vector<uint64_t> cols_wrote_;
  std::vector<T> data_wrote_;
  std::vector<uint8_t> data_validity_wrote_;
  std::string var_data_wrote_;
  std::vector<uint64_t> var_offsets_wrote_;
  std::vector<uint8_t> var_validity_wrote_;
};

// typedef std::tuple<
//     uint64_t, int64_t, uint32_t, int32_t, uint16_t, int16_t, uint8_t, int8_t,
//     float, double> TestTypes;
typedef std::tuple<uint64_t, float> TestTypes;
// Marked as mayfail pending CI for remote writes. (SC-23785)
TEST_CASE(
    "Global order remote writes debug",
    "[global-order][remote][write][!mayfail]") {
  typedef uint64_t T;
  uint64_t cells;
  uint64_t extent;
  uint64_t chunk_size;
//  bool nullable = GENERATE(true, false);

  SECTION("Full array write") {
    cells = 20;
    extent = 10;
    chunk_size = 20;
    RemoteGlobalOrderWriteFx<T> fx(cells, extent, chunk_size, false, false);
    fx.run_test();
  }

  SECTION("Tile aligned writes") {
    cells = 20;
    extent = 10;
    chunk_size = 10;
    RemoteGlobalOrderWriteFx<T> fx(cells, extent, chunk_size, false, false);
    fx.run_test();
  }

  SECTION("Tile aligned underflow N writes") {
    cells = 20;
    extent = 10;
    chunk_size = 5;
    RemoteGlobalOrderWriteFx<T> fx(cells, extent, chunk_size, false, false);
    fx.run_test();
  }

  SECTION("Tile unaligned overflow N writes") {
    cells = 20;
    extent = 5;
    chunk_size = 6;
    RemoteGlobalOrderWriteFx<T> fx(cells, extent, chunk_size, false, false);
    fx.run_test();
  }

  SECTION("Tile unaligned underflow N writes") {
    cells = 20;
    extent = 5;
    chunk_size = 3;  // Should not divide evenly into `cells` for this test.
    RemoteGlobalOrderWriteFx<T> fx(cells, extent, chunk_size, false, false);
    fx.run_test();
  }

  SECTION("Multi-tile unaligned overflow N writes") {
    cells = 50;
    extent = 5;
    chunk_size = 12;
    RemoteGlobalOrderWriteFx<T> fx(cells, extent, chunk_size, false, false);
    fx.run_test();
  }
}
