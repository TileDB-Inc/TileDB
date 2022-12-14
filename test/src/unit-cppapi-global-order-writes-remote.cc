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

// TODO: Tests for variable-sized, nullable
enum { KB = 1024, MB = KB * 1024, GB = MB * 1024 };

const std::string aws_region = "us-east-2";
const std::string aws_key_id = "<AWS_KEY_ID>";
const std::string aws_secret_key = "<AWS_SECRET_KEY>";

template <typename T>
struct RemoteGlobalOrderWriteFx {
  RemoteGlobalOrderWriteFx(uint64_t bytes, uint64_t extent, uint64_t chunk_size)
      : submit_cell_count_(chunk_size), bytes_(bytes), extent_(extent) {
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
  }

  ~RemoteGlobalOrderWriteFx() { }

  void delete_array() {
    // Delete the array.
    Array array(ctx_, array_uri_, TILEDB_MODIFY_EXCLUSIVE);
    array.delete_array(array_uri_);
  }

  // Create a simple dense array
  void create_array() {
    std::cout << "Creating array...\n";

    Domain domain(ctx_);
    domain.add_dimension(Dimension::create<uint64_t>(
        ctx_, "cols", {{1, total_cell_count_}}, extent_));

    auto array_type = GENERATE(TILEDB_DENSE, TILEDB_SPARSE);

    ArraySchema schema(ctx_, array_type);
    schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});

    if (array_type == TILEDB_SPARSE) {
      schema.set_capacity(extent_);
    }

    schema.add_attribute(Attribute::create<T>(ctx_, "a"));

    Array::create(array_uri_, schema);

    Array array(ctx_, array_uri_, TILEDB_READ);
    CHECK(array.schema().array_type() == array_type);
    CHECK(
        array.schema().domain().dimension(0).template domain<uint64_t>().second
        == total_cell_count_);
    if (array_type == TILEDB_SPARSE) {
      CHECK(array.schema().capacity() == extent_);
    }
    array.close();
  }

  void write_array() {
    std::cout << "Writing array...\n";

    Array array(ctx_, array_uri_, TILEDB_WRITE);
    Query query(ctx_, array);
    query.set_layout(TILEDB_GLOBAL_ORDER);
    // Vector to store all the data we will write to the array.
    // + We will use this vector to validate subsequent read.
    std::vector<T> data_wrote;
    std::vector<uint64_t> cols_wrote;

    auto is_sparse = array.schema().array_type() == TILEDB_SPARSE;
    uint64_t cols_start = 1;

    // Generate some data to write to the array.
    std::vector<T> data(submit_cell_count_);
    std::iota(data.begin(), data.end(), 0);

    // Resubmit until we reach total mbs requested
    for (uint64_t i = 0; i < bytes_ / sizeof(T); i += submit_cell_count_) {
      // Handle coords for sparse case.
      std::vector<uint64_t> cols(submit_cell_count_);
      if (is_sparse) {
        std::iota(cols.begin(), cols.end(), cols_start);
        cols_start = cols.back() + 1;
        query.set_data_buffer("cols", cols);
        cols_wrote.insert(cols_wrote.end(), cols.begin(), cols.end());
      }

      data_wrote.insert(data_wrote.end(), data.begin(), data.end());
      // Submit intermediate queries up to the final submission.
      query.set_data_buffer("a", data);
      if (i + submit_cell_count_ < bytes_ / sizeof(T)) {
        query.submit();
      } else {
        // IMPORTANT: Submit final write query and close the array.
        // We must do this within loop; Else our buffers will be out of scope.
        query.submit_and_finalize();
      }
    }
    CHECK(query.query_status() == Query::Status::COMPLETE);
    array.close();

    // Read back array and check data is equal to what we wrote.
    read_array(data_wrote, cols_wrote);
  }

  void read_array(
      const std::vector<T>& data_wrote,
      const std::vector<uint64_t> & cols_wrote) {
    std::cout << "Reading array...\n";
    Array array(ctx_, array_uri_, TILEDB_READ);

    // Read the entire array
    auto c = array.schema().domain().dimension("cols").domain<uint64_t>();
    Subarray subarray(ctx_, array);
    subarray.add_range("cols", c.first, c.second);

    std::vector<T> data(total_cell_count_);
    std::vector<uint64_t> cols(total_cell_count_);

    Query query(ctx_, array);
    query.set_subarray(subarray)
        .set_layout(TILEDB_GLOBAL_ORDER)
        .set_data_buffer("cols", cols)
        .set_data_buffer("a", data);
    query.submit();

    CHECK(query.query_status() == Query::Status::COMPLETE);
    CHECK_THAT(data, Catch::Matchers::Equals(data_wrote));
    if (array.schema().array_type() == TILEDB_SPARSE) {
      CHECK_THAT(cols, Catch::Matchers::Equals(cols_wrote));
    }
    array.close();
  }


  Context ctx_;
  const unsigned submit_cell_count_;
  const unsigned bytes_;
  // Find min number of values of type T to fill `bytes_`.
  const uint64_t total_cell_count_ = bytes_ / sizeof(T);
  const uint64_t extent_;

  const std::string array_name_ = "global-array-" + std::to_string(bytes_);
  const std::string s3_uri_ = "s3://shaun.reed/arrays/" + array_name_;
  const std::string array_uri_ = "tiledb://shaunreed/" + s3_uri_;
};

// TODO (1) Fix coordinates bug here for sparse.
TEST_CASE("Global order remote write failures", "[!mayfail]") {
  typedef uint64_t T;
  uint64_t bytes;
  uint64_t extent;
  uint64_t chunk_size;

  // Fails sparse case. Passes dense.
  SECTION("15 MB - 5MB chunk_size") {
    bytes = MB * 15;
    extent = (MB * 5) / sizeof(T);
    chunk_size = (MB * 5) / sizeof(T);
    RemoteGlobalOrderWriteFx<T> fx(bytes, extent, chunk_size);
    fx.create_array();
    fx.write_array();
  }
}

TEST_CASE("Debug", "[debug][!mayfail]") {
  typedef uint64_t T;
  uint64_t bytes;
  uint64_t extent;
  uint64_t chunk_size;

  // TODO Fails sparse case, likely same bug as [1]
  SECTION("15 MB array - 5 MB chunk_size") {
    bytes = MB * 15;
    extent = (MB * 1) / sizeof(T);
    chunk_size = (MB * 5) / sizeof(T);
    RemoteGlobalOrderWriteFx<T> fx(bytes, extent, chunk_size);
    fx.create_array();
    fx.write_array();
  }

  // Write test to submit entirely unaligned writes up to the final submit.
//  SECTION("1 KB - 15 chunk_size") {
//    bytes = KB;
//    extent = 8;
//    chunk_size = 10;
//    RemoteGlobalOrderWriteFx<T> fx(bytes, extent, chunk_size);
//    fx.create_array();
//    fx.write_array();
//  }
}

// All tests below are passing for all types using dense and sparse arrays.

//typedef std::tuple<
//    uint64_t, int64_t, uint32_t, int32_t, uint16_t, int16_t, uint8_t, int8_t,
//    float, double> TestTypes;
typedef std::tuple<uint64_t, int64_t, float, double> TestTypes;
TEMPLATE_LIST_TEST_CASE(
    "Global order remote writes",
    "[global-order][remote][write]", TestTypes) {
  typedef TestType T;
  uint64_t bytes;
  uint64_t extent;
  uint64_t chunk_size;

  SECTION("1 KB array - 16 chunk_size") {
    bytes = KB;
    extent = KB / sizeof(T);
    chunk_size = 16;
    RemoteGlobalOrderWriteFx<T> fx(bytes, extent, chunk_size);
    fx.create_array();
    fx.write_array();
  }

  SECTION("1 KB array - 32 chunk_size") {
    bytes = KB;
    extent = KB / sizeof(T);
    chunk_size = 32;
    RemoteGlobalOrderWriteFx<T> fx(bytes, extent, chunk_size);
    fx.create_array();
    fx.write_array();
  }

  // Submit writes with 1MB tile chunks
  SECTION("1 MB array - KB chunk_size") {
    bytes = MB;
    extent = MB / sizeof(T);
    chunk_size = KB / sizeof(T);
    RemoteGlobalOrderWriteFx<T> fx(bytes, extent, chunk_size);
    fx.create_array();
    fx.write_array();
  }

  // Submit writes with 1MB tile chunks
  SECTION("5 MB array - MB chunk_size") {
    bytes = MB * 5;
    extent = MB / sizeof(T);
    chunk_size = MB / sizeof(T);
    RemoteGlobalOrderWriteFx<T> fx(bytes, extent, chunk_size);
    fx.create_array();
    fx.write_array();
  }

  // Submit writes with chunks > extent
  SECTION("15 MB array - 5 MB chunk_size") {
    bytes = MB * 15;
    extent = (MB * 1) / sizeof(T);
    chunk_size = (MB * 5) / sizeof(T);
    RemoteGlobalOrderWriteFx<T> fx(bytes, extent, chunk_size);
    fx.create_array();
    fx.write_array();
  }

  // TODO: Fix for sparse case. [1]
  //   Coordinate (655361) comes before last written in GOWs
  // Submit writes with 5 MB tile chunks
  SECTION("15 MB array - MB chunk_size") {
    bytes = MB * 15;
    extent = (MB * 5) / sizeof(T);
    chunk_size = (MB * 1) / sizeof(T);
    RemoteGlobalOrderWriteFx<T> fx(bytes, extent, chunk_size);
    fx.create_array();
    fx.write_array();
  }
}
