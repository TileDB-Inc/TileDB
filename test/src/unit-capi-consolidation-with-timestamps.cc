/**
 * @file   unit-capi-consolidation-with-timestamps.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests the C API consolidation with timestamps.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/storage_format/uri/parse_uri.h"

#include <climits>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace tiledb::test;

/** Tests for C API consolidation with timestamps. */
struct ConsolidationWithTimestampsFx {
  // Constants
  const char* SPARSE_ARRAY_NAME = "test_consolidate_sparse_array";
  const char* SPARSE_ARRAY_FRAG_DIR =
      "test_consolidate_sparse_array/__fragments";

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Encryption parameters
  tiledb_encryption_type_t encryption_type_ = TILEDB_NO_ENCRYPTION;
  const char* encryption_key_ = nullptr;

  // Constructors/destructors
  ConsolidationWithTimestampsFx();
  ~ConsolidationWithTimestampsFx();

  // Functions
  void create_sparse_array();
  void write_sparse(
      std::vector<int> a1,
      std::vector<uint64_t> dim1,
      std::vector<uint64_t> dim2,
      uint64_t timestamp);
  void consolidate_sparse();
  void check_timestamps_file(std::vector<uint64_t> expected);

  void remove_sparse_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);

  static int find_consolidated_frag_uri(const char* path, void* data);
};

ConsolidationWithTimestampsFx::ConsolidationWithTimestampsFx() {
  ctx_ = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx_) == TILEDB_OK);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx_, nullptr, &vfs_) == TILEDB_OK);
}

ConsolidationWithTimestampsFx::~ConsolidationWithTimestampsFx() {
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void ConsolidationWithTimestampsFx::create_sparse_array() {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a1, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);

  // Create array schmea
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 20);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);

  // Set up filters
  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx_, TILEDB_FILTER_NONE, &filter);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx_, &filter_list);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_free(&filter);

  rc = tiledb_array_schema_set_coords_filter_list(
      ctx_, array_schema, filter_list);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    tiledb_ctx_free(&ctx_);
    tiledb_vfs_free(&vfs_);
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    REQUIRE(tiledb_ctx_alloc(cfg, &ctx_) == TILEDB_OK);
    REQUIRE(tiledb_vfs_alloc(ctx_, cfg, &vfs_) == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_create(ctx_, SPARSE_ARRAY_NAME, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_filter_list_free(&filter_list);
  tiledb_array_schema_free(&array_schema);
}

void ConsolidationWithTimestampsFx::write_sparse(
    std::vector<int> a1,
    std::vector<uint64_t> dim1,
    std::vector<uint64_t> dim2,
    uint64_t timestamp) {
  // Prepare cell buffers
  void* buffers[] = {a1.data(), dim1.data(), dim2.data()};
  uint64_t buffer_sizes[] = {a1.size() * sizeof(int),
                             dim1.size() * sizeof(uint64_t)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, SPARSE_ARRAY_NAME, &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, timestamp);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[2], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void ConsolidationWithTimestampsFx::consolidate_sparse() {
  int rc;
  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);

  rc = tiledb_config_set(cfg, "sm.consolidation.with_timestamps", "true", &err);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(err == nullptr);

  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
  }
  rc = tiledb_array_consolidate(ctx_, SPARSE_ARRAY_NAME, cfg);
  REQUIRE(rc == TILEDB_OK);
  tiledb_config_free(&cfg);
}

void ConsolidationWithTimestampsFx::check_timestamps_file(
    std::vector<uint64_t> expected) {
  std::string consolidated_fragment_uri;
  auto rc = tiledb_vfs_ls(
      ctx_,
      vfs_,
      SPARSE_ARRAY_FRAG_DIR,
      &find_consolidated_frag_uri,
      &consolidated_fragment_uri);
  REQUIRE(rc == TILEDB_OK);

  auto timestamps_file = consolidated_fragment_uri + "/t.tdb";

  tiledb_vfs_fh_t* fh;
  rc = tiledb_vfs_open(
      ctx_, vfs_, timestamps_file.c_str(), TILEDB_VFS_READ, &fh);
  REQUIRE(rc == TILEDB_OK);

  uint64_t off = 0;
  uint64_t num_tiles;
  rc = tiledb_vfs_read(ctx_, fh, off, &num_tiles, sizeof(uint64_t));
  REQUIRE(rc == TILEDB_OK);
  off += sizeof(uint64_t);
  REQUIRE(num_tiles == 1);

  uint32_t filtered_size;
  rc = tiledb_vfs_read(ctx_, fh, off, &filtered_size, sizeof(uint32_t));
  REQUIRE(rc == TILEDB_OK);
  off += sizeof(uint32_t);
  REQUIRE(filtered_size == expected.size() * sizeof(uint64_t));

  uint32_t unfiltered_size;
  rc = tiledb_vfs_read(ctx_, fh, off, &unfiltered_size, sizeof(uint32_t));
  REQUIRE(rc == TILEDB_OK);
  off += sizeof(uint32_t);
  REQUIRE(unfiltered_size == expected.size() * sizeof(uint64_t));

  uint32_t md_size;
  rc = tiledb_vfs_read(ctx_, fh, off, &md_size, sizeof(uint32_t));
  REQUIRE(rc == TILEDB_OK);
  off += sizeof(uint32_t);
  REQUIRE(md_size == 0);

  std::vector<uint64_t> written(unfiltered_size / sizeof(uint64_t));
  rc = tiledb_vfs_read(ctx_, fh, off, written.data(), unfiltered_size);
  REQUIRE(rc == TILEDB_OK);

  for (uint64_t i = 0; i < expected.size(); i++) {
    REQUIRE(expected[i] == written[i]);
  }
  tiledb_vfs_fh_free(&fh);
}

void ConsolidationWithTimestampsFx::remove_array(
    const std::string& array_name) {
  if (!is_array(array_name))
    return;

  CHECK(tiledb_object_remove(ctx_, array_name.c_str()) == TILEDB_OK);
}

void ConsolidationWithTimestampsFx::remove_sparse_array() {
  remove_array(SPARSE_ARRAY_NAME);
}

bool ConsolidationWithTimestampsFx::is_array(const std::string& array_name) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, array_name.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY;
}

int ConsolidationWithTimestampsFx::find_consolidated_frag_uri(
    const char* path, void* data) {
  std::string fragment = path;
  if (fragment.find("__1_2_") != std::string::npos) {
    auto uri = (std::string*)data;
    *uri = path;
  }

  return 1;
}

TEST_CASE_METHOD(
    ConsolidationWithTimestampsFx,
    "C API: Test consolidation with timestamps",
    "[capi][consolidation-with-timestamps]") {
  remove_sparse_array();
  create_sparse_array();

  // Write first fragment.
  write_sparse(
      {0, 1, 2, 3, 4, 5, 6, 7},
      {1, 1, 1, 2, 3, 4, 3, 3},
      {1, 2, 4, 3, 1, 2, 3, 4},
      1);

  // Write second fragment.
  write_sparse({8, 9, 10, 11}, {2, 2, 3, 3}, {2, 3, 2, 3}, 2);

  // Consolidate.
  consolidate_sparse();

  // Check t.tdb file.
  check_timestamps_file({1, 1, 2, 1, 1, 2, 1, 2, 1, 1, 2, 1});

  remove_sparse_array();
}