/**
 * @file unit-cppapi-global-order-writes-remote.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB Inc.
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

#include <test/support/src/vfs_helpers.h>
#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/tiledb"

#include <cstring>
#include <iostream>
#include <numeric>

using namespace tiledb;
using namespace tiledb::test;

template <typename T>
struct RemoteGlobalOrderWriteFx {
  RemoteGlobalOrderWriteFx(
      uint64_t total_cells,
      uint64_t extent,
      uint64_t submit_cell_count,
      tiledb_array_type_t array_type,
      bool is_var = true,
      bool is_nullable = true)
      : is_var_(is_var)
      , is_nullable_(is_nullable)
      , submit_cell_count_(submit_cell_count)
      , total_cell_count_(total_cells)
      , extent_(extent)
      , array_name_{"global-array-" + std::to_string(total_cell_count_)}
      , array_uri_(vfs_test_setup_.array_uri(array_name_))
      , ctx_(vfs_test_setup_.ctx())
      , array_type_(array_type){};

  // Create a simple dense array
  void create_array() {
    Domain domain(ctx_);
    domain.add_dimension(Dimension::create<uint64_t>(
        ctx_, "cols", {{1, total_cell_count_}}, extent_));

    ArraySchema schema(ctx_, array_type_);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    if (array_type_ == TILEDB_SPARSE) {
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
    CHECK(array.schema().array_type() == array_type_);
    CHECK(
        array.schema()
            .domain()
            .dimension(0)
            .template domain<uint64_t>()
            .second == total_cell_count_);
    if (array_type_ == TILEDB_SPARSE) {
      CHECK(array.schema().capacity() == extent_);
    }
    array.close();
  }

  void write_array(bool check_finalize_fails = false) {
    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array);
    query.set_layout(TILEDB_GLOBAL_ORDER);

    auto is_sparse = array.schema().array_type() == TILEDB_SPARSE;

    char char_data = 'a';
    // Start at column coordinate 1
    uint64_t cols_start = 1;
    // Resubmit until we reach total mbs requested
    uint64_t submit_cell_count = submit_cell_count_;
    for (uint64_t i = 0; i < total_cell_count_; i += submit_cell_count_) {
      if (i + submit_cell_count_ > total_cell_count_) {
        submit_cell_count -= (i + submit_cell_count_) - total_cell_count_;
      }

      // Generate some data to write to the array.
      std::vector<T> data(submit_cell_count);
      std::iota(data.begin(), data.end(), 0);

      std::vector<uint8_t> validity_buffer(submit_cell_count, 0);
      for (size_t i = 0; i < validity_buffer.size(); i += 2) {
        validity_buffer[i] = 1;
      }

      // Handle coords for sparse case.
      std::vector<uint64_t> cols(submit_cell_count);
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
      std::string var_data(submit_cell_count * sizeof(T), char_data++);
      char_data = char_data > 'z' ? 'a' : char_data;
      std::vector<uint64_t> var_offsets;
      if (is_var_) {
        // Generate offsets for variable sized attribute.
        uint64_t max_step = var_data.size() / submit_cell_count_;
        uint64_t j = 0;  // + (i * sizeof(T));
        var_offsets.resize(submit_cell_count, j);
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
        if (vfs_test_setup_.is_rest()) {
          if (check_finalize_fails) {
            CHECK_THROWS_WITH(
                query.finalize(),
                Catch::Matchers::ContainsSubstring("submit_and_finalize"));
          }
        }
        // IMPORTANT: Submit final write query and close the array.
        // We must do this within loop; Else our buffers will be out of scope.
        query.submit_and_finalize();
      }

      // Use result buffer elements to ensure we submit enough data after trim.
      auto result = query.result_buffer_elements();

      data_wrote_.insert(data_wrote_.end(), data.begin(), data.end());
      if (is_sparse) {
        cols_wrote_.insert(cols_wrote_.end(), cols.begin(), cols.end());
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
        var_data_wrote_ += var_data;
        var_offsets_wrote_.insert(
            var_offsets_wrote_.end(), var_offsets.begin(), var_offsets.end());
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
  }

  void read_array(uint64_t batch_size) {
    Array array(ctx_, array_uri_, TILEDB_READ);

    // Make all offsets absolute from 0.
    // Allows slicing offsets at any position for validating.
    make_absolute();

    // Read the entire array.
    auto c = array.schema().domain().dimension("cols").domain<uint64_t>();
    Subarray subarray(ctx_, array);
    subarray.add_range("cols", c.first, c.second);

    Query query(ctx_, array);
    query.set_subarray(subarray).set_layout(TILEDB_GLOBAL_ORDER);

    uint64_t last_check = 0;
    uint64_t last_var_check = 0;
    uint64_t last_offset_check = 1;
    do {
      // Fixed sized attribute buffers.
      std::vector<T> data(batch_size);

      // Coordinate buffers.
      std::vector<uint64_t> cols;
      if (array.schema().array_type() == TILEDB_SPARSE) {
        cols.resize(batch_size);
        query.set_data_buffer("cols", cols);
      }

      // Variable sized attribute buffers.
      std::string var_data;
      std::vector<uint64_t> var_offsets;

      std::vector<uint8_t> data_validity;
      std::vector<uint8_t> var_validity;

      query.set_data_buffer("a", data);
      if (is_nullable_) {
        data_validity.resize(batch_size);
        query.set_validity_buffer("a", data_validity);
      }

      if (is_var_) {
        var_data.clear();
        var_data.resize(var_data_wrote_.size());
        var_offsets.resize(batch_size);
        query.set_data_buffer("var", var_data)
            .set_offsets_buffer("var", var_offsets);
        if (is_nullable_) {
          var_validity.resize(batch_size);
          query.set_validity_buffer("var", var_validity);
        }
      }

      query.submit();
      auto result = query.result_buffer_elements();

      if (is_nullable_) {
        auto data_validity_slice = std::vector(
            data_validity_wrote_.begin() + last_check,
            data_validity_wrote_.begin() + last_check + batch_size);
        CHECK_THAT(data_validity, Catch::Matchers::Equals(data_validity_slice));
      }

      if (is_var_) {
        // Validate variable size data and offsets.
        auto var_data_slice =
            var_data_wrote_.substr(last_var_check, result["var"].second);
        last_var_check += result["var"].second;

        var_data.resize(result["var"].second);
        CHECK(var_data == var_data_slice);

        // Slice absolute offsets and make them relative to this range.
        auto var_offsets_slice = std::vector(
            var_offsets_wrote_.begin() + last_check,
            var_offsets_wrote_.begin() + last_check + batch_size);
        std::adjacent_difference(
            var_offsets_slice.begin(),
            var_offsets_slice.end(),
            var_offsets_slice.begin());
        var_offsets_slice[0] = 0;
        for (size_t i = 1; i < var_offsets_slice.size();
             i++, last_offset_check++) {
          var_offsets_slice[i] += var_offsets_slice[i - 1];
        }
        CHECK_THAT(var_offsets, Catch::Matchers::Equals(var_offsets_slice));

        if (is_nullable_) {
          auto var_validity_slice = std::vector(
              var_validity_wrote_.begin() + last_check,
              var_validity_wrote_.begin() + last_check + batch_size);
          CHECK_THAT(var_validity, Catch::Matchers::Equals(var_validity_slice));
        }
      }

      auto data_slice = std::vector(
          data_wrote_.begin() + last_check,
          data_wrote_.begin() + last_check + batch_size);
      CHECK_THAT(data, Catch::Matchers::Equals(data_slice));

      if (array.schema().array_type() == TILEDB_SPARSE) {
        auto cols_slice = std::vector(
            cols_wrote_.begin() + last_check,
            cols_wrote_.begin() + last_check + batch_size);
        CHECK_THAT(cols, Catch::Matchers::Equals(cols_slice));
      }

      last_check += batch_size;
    } while (query.query_status() == tiledb::Query::Status::INCOMPLETE);

    CHECK(query.query_status() == Query::Status::COMPLETE);
    array.close();
  }

  // For validating batched reads.
  // Converts a set of offsets to absolute positions.
  // The resulting vector can be sliced anywhere and validated using adj_diff.
  void make_absolute() {
    // If we can't find a 0 after the first element offsets are already abs.
    auto it =
        std::find(var_offsets_wrote_.begin() + 1, var_offsets_wrote_.end(), 0);
    if (it != var_offsets_wrote_.end()) {
      std::adjacent_difference(it, var_offsets_wrote_.end(), it);
      uint64_t submit = submit_cell_count_;
      uint64_t count = 1;
      for (size_t i = 1; i < var_offsets_wrote_.size(); i++) {
        if (i == submit) {
          var_offsets_wrote_[i] = count * (submit_cell_count_ * sizeof(T));
          count += 1;
          submit += submit_cell_count_;
          if (i + 1 < var_offsets_wrote_.size()) {
            var_offsets_wrote_[i + 1] += var_offsets_wrote_[i];
            i++;
          }
          continue;
        }
        if (count > 1) {
          var_offsets_wrote_[i] += var_offsets_wrote_[i - 1];
        }
      }
    }
  }

  void run_test() {
    create_array();
    write_array();
    read_array(total_cell_count_);
    read_array(extent_);
  }

  bool is_var_;
  bool is_nullable_;
  const unsigned submit_cell_count_;
  const uint64_t total_cell_count_;
  const uint64_t extent_;

  const std::string array_name_;
  test::VFSTestSetup vfs_test_setup_;
  std::string array_uri_;
  Context ctx_;
  tiledb_array_type_t array_type_;

  // Vectors to store all the data wrote to the array.
  // + We will use these vectors to validate subsequent read.
  std::vector<uint64_t> cols_wrote_;
  std::vector<T> data_wrote_;
  std::vector<uint8_t> data_validity_wrote_;
  std::string var_data_wrote_;
  std::vector<uint64_t> var_offsets_wrote_;
  std::vector<uint8_t> var_validity_wrote_;
};

typedef std::tuple<uint64_t, float> TestTypes;
TEMPLATE_LIST_TEST_CASE(
    "Global order remote writes",
    "[rest][global][global-order][write]",
    TestTypes) {
  auto array_type = GENERATE(TILEDB_DENSE, TILEDB_SPARSE);
  typedef TestType T;
  uint64_t cells;
  uint64_t extent;
  uint64_t chunk_size;
  bool var = true;
  bool nullable = true;

  SECTION("Small unaligned chunks") {
    cells = 20;
    extent = 10;
    chunk_size = 3;
    RemoteGlobalOrderWriteFx<T> fx(
        cells, extent, chunk_size, array_type, var, nullable);
    fx.run_test();
  }

  SECTION("Large unaligned chunks") {
    cells = 20;
    extent = 10;
    chunk_size = 19;
    RemoteGlobalOrderWriteFx<T> fx(
        cells, extent, chunk_size, array_type, var, nullable);
    fx.run_test();
  }

  SECTION("Full array write") {
    cells = 20;
    extent = 10;
    chunk_size = 20;
    RemoteGlobalOrderWriteFx<T> fx(
        cells, extent, chunk_size, array_type, var, nullable);
    fx.run_test();
  }

  SECTION("Tile aligned writes") {
    cells = 20;
    extent = 10;
    chunk_size = 10;
    RemoteGlobalOrderWriteFx<T> fx(
        cells, extent, chunk_size, array_type, var, nullable);
    fx.run_test();
  }

  // These writes will align when combined.
  SECTION("Tile aligned underflow N writes") {
    cells = 20;
    extent = 10;
    chunk_size = 5;
    RemoteGlobalOrderWriteFx<T> fx(
        cells, extent, chunk_size, array_type, var, nullable);
    fx.run_test();
  }

  SECTION("Tile unaligned overflow N writes") {
    cells = 20;
    extent = 5;
    chunk_size = 6;
    RemoteGlobalOrderWriteFx<T> fx(
        cells, extent, chunk_size, array_type, var, nullable);
    fx.run_test();
  }

  SECTION("Tile unaligned underflow N writes") {
    cells = 20;
    extent = 5;
    chunk_size = 3;  // Should not divide evenly into `cells` for this test.
    RemoteGlobalOrderWriteFx<T> fx(
        cells, extent, chunk_size, array_type, var, nullable);
    fx.run_test();
  }

  SECTION("Multi-tile unaligned overflow N writes") {
    cells = 50;
    extent = 5;
    chunk_size = 12;
    RemoteGlobalOrderWriteFx<T> fx(
        cells, extent, chunk_size, array_type, var, nullable);
    fx.run_test();
  }

  SECTION("> half-tile unaligned overflow N writes") {
    cells = 50;
    extent = 10;
    chunk_size = 18;
    RemoteGlobalOrderWriteFx<T> fx(
        cells, extent, chunk_size, array_type, var, nullable);
    fx.run_test();
  }
}

TEST_CASE(
    "Remote global order writes finalize errors",
    "[rest][global][global-order][write][finalize]") {
  auto array_type = GENERATE(TILEDB_DENSE, TILEDB_SPARSE);
  RemoteGlobalOrderWriteFx<uint64_t> fx(20, 10, 3, array_type, true, true);
  fx.create_array();
  fx.write_array(true);
}
