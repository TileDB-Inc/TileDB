/**
 * @file   unit-capi-axes_lables.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB Inc.
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
 * Tests of C API for querying by axes labels.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/utils.h"

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct AxesLabelsFx {
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
  const std::string S3_PREFIX = "s3://";
  const std::string S3_BUCKET = S3_PREFIX + random_name("tiledb") + "/";
  const std::string S3_TEMP_DIR = S3_BUCKET + "tiledb_test/";
  const std::string AZURE_PREFIX = "azure://";
  const std::string bucket = AZURE_PREFIX + random_name("tiledb") + "/";
  const std::string AZURE_TEMP_DIR = bucket + "tiledb_test/";
#ifdef _WIN32
  const std::string FILE_URI_PREFIX = "";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;
  bool supports_azure_;

  bool serialize_ = false;

  // Functions
  AxesLabelsFx();
  ~AxesLabelsFx();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void set_supported_fs();
  std::string random_name(const std::string& prefix);

  void write_array_1d(
      tiledb_ctx_t* ctx,
      const std::string& array_name,
      tiledb_layout_t layout,
      const std::vector<int32_t>& d,
      const std::vector<int32_t>& a);
  void write_axis_labels(
      tiledb_ctx_t* ctx,
      const std::string& axis_name,
      tiledb_layout_t layout,
      const std::vector<uint64_t>& labels_off,
      const std::string& labels_val,
      const std::vector<int32_t>& d);
  void read_array_1d(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      tiledb_array_t* axis,
      tiledb_layout_t layout,
      const std::vector<std::string>& labels,
      std::vector<int32_t>* a,
      tiledb_query_status_t* status);
};

AxesLabelsFx::AxesLabelsFx() {
  // Supported filesystems
  set_supported_fs();

  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  if (supports_s3_) {
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.endpoint_override", "localhost:9999", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.s3.scheme", "https", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.use_virtual_addressing", "false", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.s3.verify_ssl", "false", &error) ==
        TILEDB_OK);
    REQUIRE(error == nullptr);
#endif
  }
  if (supports_azure_) {
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.storage_account_name",
            "devstoreaccount1",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.storage_account_key",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
            "K1SZFPTOtr/KBHBeksoGMGw==",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.blob_endpoint",
            "127.0.0.1:10000/devstoreaccount1",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.azure.use_https", "false", &error) ==
        TILEDB_OK);
  }
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);

  // Connect to S3
  if (supports_s3_) {
    // Create bucket if it does not exist
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    REQUIRE(rc == TILEDB_OK);
    if (!is_bucket) {
      rc = tiledb_vfs_create_bucket(ctx_, vfs_, S3_BUCKET.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }

  // Connect to Azure
  if (supports_azure_) {
    int is_container = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, bucket.c_str(), &is_container);
    REQUIRE(rc == TILEDB_OK);
    if (!is_container) {
      rc = tiledb_vfs_create_bucket(ctx_, vfs_, bucket.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }
}

AxesLabelsFx::~AxesLabelsFx() {
  if (supports_s3_) {
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    CHECK(rc == TILEDB_OK);
    if (is_bucket) {
      CHECK(
          tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str()) == TILEDB_OK);
    }
  }

  if (supports_azure_) {
    int is_container = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, bucket.c_str(), &is_container);
    REQUIRE(rc == TILEDB_OK);
    if (is_container) {
      rc = tiledb_vfs_remove_bucket(ctx_, vfs_, bucket.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }

  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void AxesLabelsFx::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);
  get_supported_fs(&supports_s3_, &supports_hdfs_, &supports_azure_);
  tiledb_ctx_free(&ctx);
}

void AxesLabelsFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void AxesLabelsFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

std::string AxesLabelsFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

void AxesLabelsFx::write_array_1d(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_layout_t layout,
    const std::vector<int32_t>& d,
    const std::vector<int32_t>& a) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create and submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);

  uint64_t d_size = d.size() * sizeof(uint32_t);
  uint64_t a_size = a.size() * sizeof(int32_t);
  rc = tiledb_query_set_buffer(ctx, query, "d", (void*)d.data(), &d_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx, query, "a", (void*)a.data(), &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, layout);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void AxesLabelsFx::write_axis_labels(
    tiledb_ctx_t* ctx,
    const std::string& axis_name,
    tiledb_layout_t layout,
    const std::vector<uint64_t>& labels_off,
    const std::string& labels_val,
    const std::vector<int32_t>& d) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, axis_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create and submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);

  uint64_t labels_off_size = labels_off.size() * sizeof(uint64_t);
  uint64_t labels_val_size = labels_val.size();
  uint64_t d_size = d.size() * sizeof(int32_t);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      "labels",
      (uint64_t*)&labels_off[0],
      &labels_off_size,
      (void*)&labels_val[0],
      &labels_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx, query, "d", (void*)d.data(), &d_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, layout);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void AxesLabelsFx::read_array_1d(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_array_t* axis,
    tiledb_layout_t layout,
    const std::vector<std::string>& labels,
    std::vector<int32_t>* a,
    tiledb_query_status_t* status) {
  // Create query
  tiledb_query_t* query;
  int rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Set axis array
  rc = tiledb_query_set_axis_array(ctx_, query, 0, axis, "d");
  CHECK(rc == TILEDB_OK);

  // Add ranges
  for (const auto& l : labels) {
    rc = tiledb_query_add_axis_range_var(
        ctx, query, 0, 0, l.data(), l.size(), l.data(), l.size());
    CHECK(rc == TILEDB_OK);
  }

  // Set query buffers
  uint64_t a_size = a->size() * sizeof(int32_t);
  rc = tiledb_query_set_buffer(ctx, query, "a", a->data(), &a_size);
  REQUIRE(rc == TILEDB_OK);

  // Set layout
  rc = tiledb_query_set_layout(ctx, query, layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Get status
  rc = tiledb_query_get_status(ctx, query, status);
  CHECK(rc == TILEDB_OK);

  // Resize the result buffer
  a->resize(a_size / sizeof(int32_t));

  // Clean up
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    AxesLabelsFx,
    "C API: Test axes labels, 1D sparse array",
    "[capi][sparse][axes-labels][1d]") {
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_array";
  std::string axis_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "axis_labels";

  // Create a 1D array with int32 domain
  int32_t domain[] = {1, 100};
  int32_t extent = 10;
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_INT32},
      {domain},
      {&extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Create a string axis labels array mapping to the int32 indices of
  // the array above
  create_array(
      ctx_,
      axis_name,
      TILEDB_SPARSE,
      {"labels"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"d"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Write to array
  std::vector<int32_t> d = {3, 8, 10, 16};
  std::vector<int32_t> a = {1, 2, 3, 4};
  write_array_1d(ctx_, array_name, TILEDB_UNORDERED, d, a);

  // Write to axis labels
  std::string labels_val = "onetwothreefour";
  std::vector<uint64_t> labels_off = {0, 3, 6, 11};
  std::vector<int32_t> d_pos = {3, 8, 10, 16};
  write_axis_labels(
      ctx_, axis_name, TILEDB_UNORDERED, labels_off, labels_val, d_pos);

  // #### Read ####

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Open axis labels
  tiledb_array_t* axis;
  rc = tiledb_array_alloc(ctx_, axis_name.c_str(), &axis);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, axis, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Read with labels
  tiledb_query_status_t status;
  std::vector<std::string> labels = {"three", "one"};
  std::vector<int32_t> r_a(20);
  std::vector<int32_t> c_a = {3, 1};
  read_array_1d(ctx_, array, axis, TILEDB_ROW_MAJOR, labels, &r_a, &status);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_a == c_a);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Close axis
  rc = tiledb_array_close(ctx_, axis);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_array_free(&axis);

  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}