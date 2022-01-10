/**
 * @file   unit-capi-attributes.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB Inc.
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
 * Tests of C API for attributes.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"

#include <iostream>

#include "catch.hpp"
#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#ifdef _WIN32
#include <Windows.h>
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct Attributesfx {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filesystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  // Functions
  Attributesfx();
  ~Attributesfx();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void create_dense_vector(
      const std::string& path,
      const std::string& attr_name,
      tiledb_datatype_t attr_type);
  static std::string random_name(const std::string& prefix);
};

Attributesfx::Attributesfx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
}

Attributesfx::~Attributesfx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void Attributesfx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void Attributesfx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

std::string Attributesfx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

void Attributesfx::create_dense_vector(
    const std::string& path,
    const std::string& attr_name,
    tiledb_datatype_t attr_type) {
  int rc;
  int64_t dim_domain[] = {1, 10};
  int64_t tile_extent = 2;

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_INT64, dim_domain, &tile_extent, &dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx_, attr_name.c_str(), attr_type, &attr);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);

  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    Attributesfx,
    "C API: Test attributes with illegal filesystem characters in the name",
    "[capi][attributes][illegal_name]") {
  const std::vector<std::string> attr_names = {
      "miles!hour",  "miles#hour", "miles$hour", "miles%hour",  "miles&hour",
      "miles'hour",  "miles(hour", "miles)hour", "miles*hour",  "miles+hour",
      "miles,hour",  "miles/hour", "miles:hour", "miles;hour",  "miles=hour",
      "miles?hour",  "miles@hour", "miles[hour", "miles]hour",  "miles[hour",
      "miles\"hour", "miles<hour", "miles>hour", "miles\\hour", "miles|hour"};

  for (const auto& attr_name : attr_names) {
    for (const auto& fs : fs_vec_) {
      std::string temp_dir = fs->temp_dir();
      std::string array_name = temp_dir + "array-illegal-char";

      // Create new TileDB context with file lock config disabled, rest the
      // same.
      tiledb_ctx_free(&ctx_);
      tiledb_vfs_free(&vfs_);

      tiledb_config_t* config = nullptr;
      tiledb_error_t* error = nullptr;
      REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
      REQUIRE(error == nullptr);

      REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok());

      tiledb_config_free(&config);

      create_temp_dir(temp_dir);

      create_dense_vector(array_name, attr_name, TILEDB_INT32);

      // Prepare cell buffers
      int buffer_a1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
      uint64_t buffer_a1_size = sizeof(buffer_a1);

      // Open array
      int64_t subarray[] = {1, 10};
      tiledb_array_t* array;
      int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
      CHECK(rc == TILEDB_OK);

      // Submit query
      tiledb_query_t* query;
      rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_data_buffer(
          ctx_, query, attr_name.c_str(), buffer_a1, &buffer_a1_size);
      CHECK(rc == TILEDB_OK);

      rc = tiledb_query_submit(ctx_, query);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_finalize(ctx_, query);
      CHECK(rc == TILEDB_OK);

      // Close array and clean up
      rc = tiledb_array_close(ctx_, array);
      CHECK(rc == TILEDB_OK);
      tiledb_array_free(&array);
      tiledb_query_free(&query);

      int buffer_read[10];
      uint64_t buffer_read_size = sizeof(buffer_read);

      // Open array
      rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_array_open(ctx_, array, TILEDB_READ);
      CHECK(rc == TILEDB_OK);

      // Submit query
      rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_subarray(ctx_, query, subarray);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_data_buffer(
          ctx_, query, attr_name.c_str(), buffer_read, &buffer_read_size);
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_submit(ctx_, query);
      CHECK(rc == TILEDB_OK);

      // Close array and clean up
      rc = tiledb_array_close(ctx_, array);
      CHECK(rc == TILEDB_OK);
      tiledb_array_free(&array);
      tiledb_query_free(&query);

      // Check correctness
      int buffer_read_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
      CHECK(!std::memcmp(buffer_read, buffer_read_c, sizeof(buffer_read_c)));
      CHECK(buffer_read_size == sizeof(buffer_read_c));

      remove_temp_dir(temp_dir);
    }
  }
}

TEST_CASE_METHOD(
    Attributesfx,
    "C API: Test attributes with tiledb_blob datatype",
    "[capi][attributes][tiledb_blob]") {
  for (const auto& fs : fs_vec_) {
    std::string temp_dir = fs->temp_dir();
    std::string array_name = temp_dir;
    std::string attr_name = "attr";

    // Create new TileDB context with file lock config disabled, rest the
    // same.
    tiledb_ctx_free(&ctx_);
    tiledb_vfs_free(&vfs_);

    tiledb_config_t* config = nullptr;
    tiledb_error_t* error = nullptr;
    REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
    REQUIRE(error == nullptr);

    REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok());

    tiledb_config_free(&config);

    create_temp_dir(temp_dir);

    create_dense_vector(array_name, attr_name, TILEDB_BLOB);

    // Prepare cell buffers
    uint8_t buffer_a1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    uint64_t buffer_a1_size = sizeof(buffer_a1);

    // Open array
    int64_t subarray[] = {1, 10};
    tiledb_array_t* array;
    int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
    CHECK(rc == TILEDB_OK);

    // Submit query
    tiledb_query_t* query;
    rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_subarray(ctx_, query, subarray);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, attr_name.c_str(), buffer_a1, &buffer_a1_size);
    CHECK(rc == TILEDB_OK);

    rc = tiledb_query_submit(ctx_, query);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_finalize(ctx_, query);
    CHECK(rc == TILEDB_OK);

    // Close array and clean up
    rc = tiledb_array_close(ctx_, array);
    CHECK(rc == TILEDB_OK);
    tiledb_array_free(&array);
    tiledb_query_free(&query);

    int buffer_read[10];
    uint64_t buffer_read_size = sizeof(buffer_read);

    // Open array
    rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Submit query
    rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_subarray(ctx_, query, subarray);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, attr_name.c_str(), buffer_read, &buffer_read_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_submit(ctx_, query);
    CHECK(rc == TILEDB_OK);

    // Close array and clean up
    rc = tiledb_array_close(ctx_, array);
    CHECK(rc == TILEDB_OK);
    tiledb_array_free(&array);
    tiledb_query_free(&query);

    // Check correctness
    uint8_t buffer_read_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    CHECK(!std::memcmp(buffer_read, buffer_read_c, sizeof(buffer_read_c)));
    CHECK(buffer_read_size == sizeof(buffer_read_c));

    remove_temp_dir(temp_dir);
  }
}