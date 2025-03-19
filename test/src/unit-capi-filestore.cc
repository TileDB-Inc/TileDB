/**
 * @file   unit-capi-filestore.cc
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
 * Tests the TileDB filestore C API.
 */

#include <test/support/tdb_catch.h>
#include <iostream>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::test;

static const std::string files_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/files";

struct FileFx {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  tiledb_config_t* config_;

  // Vector of supported filesystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  std::string compression_file_path_;
  uint32_t expected_nfilters_;

  // Functions
  FileFx();
  ~FileFx();
  void create_temp_dir(const std::string& path) const;
  void remove_temp_dir(const std::string& path) const;
  void check_metadata_correctness(
      tiledb_array_t* array, uint64_t expected_file_size) const;
};

FileFx::FileFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config_, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, config_).ok());
}

FileFx::~FileFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_config_free(&config_);
}

void FileFx::create_temp_dir(const std::string& path) const {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void FileFx::remove_temp_dir(const std::string& path) const {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
  else {
    int is_file = 0;
    REQUIRE(
        tiledb_vfs_is_file(ctx_, vfs_, path.c_str(), &is_file) == TILEDB_OK);
    if (is_file)
      REQUIRE(tiledb_vfs_remove_file(ctx_, vfs_, path.c_str()) == TILEDB_OK);
  }
}

void FileFx::check_metadata_correctness(
    tiledb_array_t* array, uint64_t expected_file_size) const {
  const void* value;
  tiledb_datatype_t dtype;
  uint32_t num;
  CHECK(
      tiledb_array_get_metadata(
          ctx_,
          array,
          tiledb::sm::constants::filestore_metadata_size_key.c_str(),
          &dtype,
          &num,
          &value) == TILEDB_OK);
  REQUIRE(*static_cast<const uint64_t*>(value) == expected_file_size);
  CHECK(
      tiledb_array_get_metadata(
          ctx_,
          array,
          tiledb::sm::constants::filestore_metadata_mime_type_key.c_str(),
          &dtype,
          &num,
          &value) == TILEDB_OK);
  CHECK(memcmp(value, "text/plain", num) == 0);
  CHECK(
      tiledb_array_get_metadata(
          ctx_,
          array,
          tiledb::sm::constants::filestore_metadata_mime_encoding_key.c_str(),
          &dtype,
          &num,
          &value) == TILEDB_OK);
  CHECK(memcmp(value, "us-ascii", num) == 0);
  CHECK(
      tiledb_array_get_metadata(
          ctx_,
          array,
          tiledb::sm::constants::filestore_metadata_original_filename_key
              .c_str(),
          &dtype,
          &num,
          &value) == TILEDB_OK);
  // value == nullptr acceptable for buffer import
  if (value) {
    CHECK(memcmp(value, "text", num) == 0);
  }
  CHECK(
      tiledb_array_get_metadata(
          ctx_,
          array,
          tiledb::sm::constants::filestore_metadata_file_extension_key.c_str(),
          &dtype,
          &num,
          &value) == TILEDB_OK);
  // value == nullptr acceptable for buffer import
  if (value) {
    CHECK(memcmp(value, "txt", num) == 0);
  }
}

TEST_CASE_METHOD(
    FileFx, "C API: Test schema create from uri", "[capi][filestore][schema]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string txt_path = files_dir + "/" + "text.txt";
  std::string array_path = temp_dir + "/" + "test_filestore_non_imported_array";

  create_temp_dir(temp_dir);

  tiledb_array_schema_t* schema;

  CHECK(
      tiledb_filestore_schema_create(ctx_, txt_path.c_str(), &schema) ==
      TILEDB_OK);

  CHECK(tiledb_array_schema_check(ctx_, schema) == TILEDB_OK);

  // Check array_schema type
  tiledb_array_type_t type;
  CHECK(tiledb_array_schema_get_array_type(ctx_, schema, &type) == TILEDB_OK);
  CHECK(type == TILEDB_DENSE);

  // Check dimension
  tiledb_domain_t* domain;
  CHECK(tiledb_array_schema_get_domain(ctx_, schema, &domain) == TILEDB_OK);
  tiledb_dimension_t* dim;
  CHECK(
      tiledb_domain_get_dimension_from_name(
          ctx_,
          domain,
          tiledb::sm::constants::filestore_dimension_name.c_str(),
          &dim) == TILEDB_OK);

  // Check attribute
  tiledb_attribute_t* attr;
  int rc = tiledb_array_schema_get_attribute_from_name(
      ctx_,
      schema,
      tiledb::sm::constants::filestore_attribute_name.c_str(),
      &attr);
  REQUIRE(rc == TILEDB_OK);

  size_t size = 0;
  REQUIRE(tiledb_array_create(ctx_, array_path.c_str(), schema) == TILEDB_OK);
  REQUIRE(tiledb_filestore_size(ctx_, array_path.c_str(), &size) == TILEDB_ERR);

  // Cleanup
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&schema);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    FileFx,
    "C API: Test schema create detects compression",
    "[capi][filestore][schema][compression]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  SECTION("- Uncompressed file") {
    compression_file_path_ = files_dir + "/" + "text.txt";
    expected_nfilters_ = 1;
  }
  SECTION("- Compressed file") {
    compression_file_path_ = files_dir + "/" + "quickstart_dense.csv.gz";
    expected_nfilters_ = 0;
  }
  SECTION("- Fake gz extension file") {
    compression_file_path_ = files_dir + "/" + "fake_gz.gz";
    expected_nfilters_ = 1;
  }

  create_temp_dir(temp_dir);

  tiledb_array_schema_t* schema;
  REQUIRE(
      tiledb_filestore_schema_create(
          ctx_, compression_file_path_.c_str(), &schema) == TILEDB_OK);

  tiledb_attribute_t* attr;
  REQUIRE(
      tiledb_array_schema_get_attribute_from_name(
          ctx_,
          schema,
          tiledb::sm::constants::filestore_attribute_name.c_str(),
          &attr) == TILEDB_OK);

  // Text files should get a default filter
  tiledb_filter_list_t* attr_filters;
  REQUIRE(
      tiledb_attribute_get_filter_list(ctx_, attr, &attr_filters) == TILEDB_OK);
  uint32_t nfilters;
  REQUIRE(
      tiledb_filter_list_get_nfilters(ctx_, attr_filters, &nfilters) ==
      TILEDB_OK);
  CHECK(nfilters == expected_nfilters_);

  // Cleanup
  tiledb_filter_list_free(&attr_filters);
  tiledb_attribute_free(&attr);
  tiledb_array_schema_free(&schema);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    FileFx,
    "C API: Test importing from uri",
    "[capi][filestore][uri][import]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string array_name = temp_dir + "uri_import";
  std::string file_path = files_dir + "/" + "text.txt";
  create_temp_dir(temp_dir);

  tiledb_array_schema_t* schema;
  CHECK(
      tiledb_filestore_schema_create(ctx_, file_path.c_str(), &schema) ==
      TILEDB_OK);
  CHECK(tiledb_array_create(ctx_, array_name.c_str(), schema) == TILEDB_OK);

  // Check uri import
  CHECK(
      tiledb_filestore_uri_import(
          ctx_,
          array_name.c_str(),
          file_path.c_str(),
          TILEDB_MIME_AUTODETECT) == TILEDB_OK);

  tiledb_array_t* array;
  CHECK(tiledb_array_alloc(ctx_, array_name.c_str(), &array) == TILEDB_OK);
  CHECK(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);

  std::string file_content("Simple text file.\nWith two lines.");

  // Check metadata
  check_metadata_correctness(array, file_content.size() + 1);

  // Read array
  std::vector<std::byte> buffer(file_content.size());
  uint64_t subarray_read[] = {0, file_content.size() - 1};
  uint64_t size = buffer.size();
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  tiledb_subarray_alloc(ctx_, array, &sub);
  tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  tiledb_query_set_subarray_t(ctx_, query, sub);
  tiledb_subarray_free(&sub);

  tiledb_query_set_data_buffer(
      ctx_,
      query,
      tiledb::sm::constants::filestore_attribute_name.c_str(),
      buffer.data(),
      &size);
  CHECK(tiledb_query_submit(ctx_, query) == TILEDB_OK);

  // Check correctness
  CHECK(!std::memcmp(buffer.data(), file_content.data(), buffer.size()));

  // Clean up
  tiledb_array_close(ctx_, array);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_array_schema_free(&schema);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    FileFx, "C API: Test exporting to uri", "[capi][filestore][uri][export]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string array_name = temp_dir + "uri_export";
  std::string file_path = files_dir + "/" + "text.txt";
  std::string copy_file_path = temp_dir + "copy.txt";
  create_temp_dir(temp_dir);

  tiledb_array_schema_t* schema;
  CHECK(
      tiledb_filestore_schema_create(ctx_, file_path.c_str(), &schema) ==
      TILEDB_OK);
  CHECK(tiledb_array_create(ctx_, array_name.c_str(), schema) == TILEDB_OK);
  CHECK(
      tiledb_filestore_uri_import(
          ctx_,
          array_name.c_str(),
          file_path.c_str(),
          TILEDB_MIME_AUTODETECT) == TILEDB_OK);

  tiledb_array_t* array;
  CHECK(tiledb_array_alloc(ctx_, array_name.c_str(), &array) == TILEDB_OK);
  CHECK(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);

  CHECK(
      tiledb_filestore_uri_export(
          ctx_, copy_file_path.c_str(), array_name.c_str()) == TILEDB_OK);

  std::string file_content("Simple text file.\nWith two lines.");

  uint64_t size;
  REQUIRE(
      tiledb_vfs_file_size(ctx_, vfs_, copy_file_path.c_str(), &size) ==
      TILEDB_OK);
  REQUIRE(size == file_content.size() + 1);

  char buffer[100];
  tiledb_vfs_fh_t* fh;
  REQUIRE(
      tiledb_vfs_open(
          ctx_, vfs_, copy_file_path.c_str(), TILEDB_VFS_READ, &fh) ==
      TILEDB_OK);
  CHECK(tiledb_vfs_read(ctx_, fh, 0, buffer, file_content.size()) == TILEDB_OK);

  // Check correctness
  CHECK(!std::memcmp(buffer, file_content.data(), file_content.size()));

  // Clean up
  tiledb_vfs_close(ctx_, fh);
  tiledb_array_close(ctx_, array);
  tiledb_array_free(&array);
  tiledb_array_schema_free(&schema);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    FileFx,
    "C API: Test importing from a buffer",
    "[capi][filestore][buffer][import]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string array_name = temp_dir + "buffer_import";
  create_temp_dir(temp_dir);

  tiledb_array_schema_t* schema;
  CHECK(tiledb_filestore_schema_create(ctx_, nullptr, &schema) == TILEDB_OK);
  CHECK(tiledb_array_create(ctx_, array_name.c_str(), schema) == TILEDB_OK);

  std::string file_content("Simple text file.\nWith two lines.");

  // Check buffer import
  CHECK(
      tiledb_filestore_buffer_import(
          ctx_,
          array_name.c_str(),
          file_content.data(),
          file_content.size(),
          TILEDB_MIME_AUTODETECT) == TILEDB_OK);

  tiledb_array_t* array;
  CHECK(tiledb_array_alloc(ctx_, array_name.c_str(), &array) == TILEDB_OK);
  CHECK(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);

  // Check metadata
  check_metadata_correctness(array, file_content.size());

  // Read array
  std::vector<std::byte> buffer(file_content.size());
  uint64_t subarray_read[] = {0, file_content.size() - 1};
  uint64_t size = buffer.size();
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  tiledb_subarray_alloc(ctx_, array, &sub);
  tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  tiledb_query_set_subarray_t(ctx_, query, sub);
  tiledb_subarray_free(&sub);
  tiledb_query_set_data_buffer(
      ctx_,
      query,
      tiledb::sm::constants::filestore_attribute_name.c_str(),
      buffer.data(),
      &size);
  CHECK(tiledb_query_submit(ctx_, query) == TILEDB_OK);

  // Check correctness
  CHECK(!std::memcmp(buffer.data(), file_content.data(), buffer.size()));

  // Clean up
  tiledb_array_close(ctx_, array);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_array_schema_free(&schema);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    FileFx,
    "C API: Test exporting to a buffer",
    "[capi][filestore][buffer][export]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string array_name = temp_dir + "uri_export";
  create_temp_dir(temp_dir);

  tiledb_array_schema_t* schema;
  CHECK(tiledb_filestore_schema_create(ctx_, nullptr, &schema) == TILEDB_OK);
  CHECK(tiledb_array_create(ctx_, array_name.c_str(), schema) == TILEDB_OK);

  std::string file_content("Simple text file.\nWith two lines.");
  CHECK(
      tiledb_filestore_buffer_import(
          ctx_,
          array_name.c_str(),
          file_content.data(),
          file_content.size(),
          TILEDB_MIME_AUTODETECT) == TILEDB_OK);

  tiledb_array_t* array;
  CHECK(tiledb_array_alloc(ctx_, array_name.c_str(), &array) == TILEDB_OK);
  CHECK(tiledb_array_open(ctx_, array, TILEDB_READ) == TILEDB_OK);

  char buffer[100];
  CHECK(
      tiledb_filestore_buffer_export(
          ctx_, array_name.c_str(), 0, buffer, file_content.size()) ==
      TILEDB_OK);
  CHECK(!std::memcmp(buffer, file_content.data(), file_content.size()));

  // Check exporting from offset
  CHECK(
      tiledb_filestore_buffer_export(
          ctx_, array_name.c_str(), 18, buffer, 15) == TILEDB_OK);
  CHECK(!std::memcmp(buffer, "With two lines.", 15));

  // Check filestore size
  size_t size;
  CHECK(tiledb_filestore_size(ctx_, array_name.c_str(), &size) == TILEDB_OK);
  CHECK(size == file_content.size());

  // Clean up
  tiledb_array_close(ctx_, array);
  tiledb_array_free(&array);
  tiledb_array_schema_free(&schema);
  remove_temp_dir(temp_dir);
}
