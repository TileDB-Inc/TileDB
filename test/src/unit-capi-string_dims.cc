/**
 * @file   unit-capi-string_dims.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB Inc.
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
 * Tests of C API for sparse arrays with string dimensions.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/enums/serialization_type.h"

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

typedef std::pair<std::string, uint64_t> EstSize;

struct StringDimsFx {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Serialization parameters
  bool serialize_ = false;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  // Path to prepend to array name according to filesystem/mode
  std::string prefix_;

  // Used to get the number of directories or files of another directory
  struct get_num_struct {
    tiledb_ctx_t* ctx;
    tiledb_vfs_t* vfs;
    int num;
  };
  static int get_dir_num(const char* path, void* data);

  // Functions
  StringDimsFx();
  ~StringDimsFx();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  int array_create_wrapper(
      const std::string& path, tiledb_array_schema_t* array_schema);
  int array_schema_load_wrapper(
      const std::string& path, tiledb_array_schema_t** array_schema);
  void write_array_1d(
      tiledb_ctx_t* ctx,
      const std::string& array_name,
      tiledb_layout_t layout,
      const std::vector<uint64_t>& d_off,
      const std::string& d_val,
      const std::vector<int32_t>& a);
  void write_array_2d(
      tiledb_ctx_t* ctx,
      const std::string& array_name,
      tiledb_layout_t layout,
      const std::vector<uint64_t>& d1_off,
      const std::string& d1_val,
      const std::vector<int32_t>& d2,
      const std::vector<int32_t>& a);
  int tiledb_array_get_non_empty_domain_var_size_from_name_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      const char* name,
      uint64_t* start_size,
      uint64_t* end_size,
      int32_t* is_empty);
  int tiledb_array_get_non_empty_domain_var_from_name_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      const char* name,
      void* start,
      void* end,
      int32_t* is_empty);
  int tiledb_array_get_non_empty_domain_from_name_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      const char* name,
      void* domain,
      int32_t* is_empty);
  void get_non_empty_domain(
      const std::string& array_name,
      const std::string& dim_name,
      std::vector<int32_t>* dom,
      int32_t* is_empty);
  void get_non_empty_domain_var(
      const std::string& array_name,
      const std::string& dim_name,
      std::string* start,
      std::string* end,
      int32_t* is_empty);
  void get_est_result_size_var(
      tiledb_array_t* array,
      unsigned dim_idx,
      const std::string& dim_name,
      const std::string& start,
      const std::string& end,
      uint64_t* size_off,
      uint64_t* size_val);
  void read_array_1d(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      tiledb_layout_t layout,
      const std::string& start,
      const std::string& end,
      std::vector<uint64_t>* d_off,
      std::string* d_val,
      std::vector<int32_t>* a,
      tiledb_query_status_t* status,
      tiledb_query_status_details_t* details);
  void read_array_2d(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      tiledb_layout_t layout,
      const std::string& d1_start,
      const std::string& d1_end,
      int32_t d2_start,
      int32_t d2_end,
      std::vector<uint64_t>* d1_off,
      std::string* d1_val,
      std::vector<int32_t>* d2,
      std::vector<int32_t>* a,
      tiledb_query_status_t* status);
  void read_array_2d_default_string_range(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      tiledb_layout_t layout,
      int32_t d2_start,
      int32_t d2_end,
      std::vector<uint64_t>* d1_off,
      std::string* d1_val,
      std::vector<int32_t>* d2,
      std::vector<int32_t>* a,
      tiledb_query_status_t* status);
};

StringDimsFx::StringDimsFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
  auto temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  prefix_ = vfs_array_uri(fs_vec_[0], "strings-dim-fx", ctx_);
}

StringDimsFx::~StringDimsFx() {
  remove_temp_dir(fs_vec_[0]->temp_dir());
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void StringDimsFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void StringDimsFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

int StringDimsFx::get_dir_num(const char* path, void* data) {
  auto data_struct = (StringDimsFx::get_num_struct*)data;
  auto ctx = data_struct->ctx;
  auto vfs = data_struct->vfs;
  int is_dir;
  int rc = tiledb_vfs_is_dir(ctx, vfs, path, &is_dir);
  CHECK(rc == TILEDB_OK);
  data_struct->num += is_dir;

  return 1;
}

int StringDimsFx::array_schema_load_wrapper(
    const std::string& path, tiledb_array_schema_t** array_schema) {
#ifndef TILEDB_SERIALIZATION
  return tiledb_array_schema_load(ctx_, path.c_str(), array_schema);
#endif

  if (!serialize_) {
    return tiledb_array_schema_load(ctx_, path.c_str(), array_schema);
  }

  // Load array.
  int rc = tiledb_array_schema_load(ctx_, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Serialize the array
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          *array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &buff) == TILEDB_OK);

  // Load array schema from the rest server
  tiledb_array_schema_t* new_array_schema = nullptr;
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &new_array_schema) == TILEDB_OK);

  // Serialize the new array schema and deserialize into the original array
  // schema.
  tiledb_array_schema_free(array_schema);
  tiledb_buffer_t* buff2;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          new_array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff2) == TILEDB_OK);
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff2,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          array_schema) == TILEDB_OK);

  // Clean up.
  tiledb_array_schema_free(&new_array_schema);
  tiledb_buffer_free(&buff);
  tiledb_buffer_free(&buff2);

  return rc;
}

int StringDimsFx::array_create_wrapper(
    const std::string& path, tiledb_array_schema_t* array_schema) {
#ifndef TILEDB_SERIALIZATION
  return tiledb_array_create(ctx_, path.c_str(), array_schema);
#endif

  if (!serialize_) {
    return tiledb_array_create(ctx_, path.c_str(), array_schema);
  }

  // Serialize the array
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &buff) == TILEDB_OK);

  // Load array schema from the rest server
  tiledb_array_schema_t* new_array_schema = nullptr;
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &new_array_schema) == TILEDB_OK);

  // Create array from new schema
  int rc = tiledb_array_create(ctx_, path.c_str(), new_array_schema);

  // Serialize the new array schema and deserialize into the original array
  // schema.
  tiledb_buffer_t* buff2;
  tiledb_array_schema_t* new_array_schema2 = nullptr;
  REQUIRE(
      tiledb_serialize_array_schema(
          ctx_,
          new_array_schema,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff2) == TILEDB_OK);
  REQUIRE(
      tiledb_deserialize_array_schema(
          ctx_,
          buff2,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &new_array_schema2) == TILEDB_OK);

  // Clean up.
  tiledb_array_schema_free(&new_array_schema);
  tiledb_array_schema_free(&new_array_schema2);
  tiledb_buffer_free(&buff);
  tiledb_buffer_free(&buff2);

  return rc;
}

void StringDimsFx::write_array_1d(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_layout_t layout,
    const std::vector<uint64_t>& d_off,
    const std::string& d_val,
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

  uint64_t d_off_size = d_off.size() * sizeof(uint64_t);
  uint64_t d_val_size = d_val.size();
  uint64_t a_size = a.size() * sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "d", (void*)&d_val[0], &d_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "d", (uint64_t*)&d_off[0], &d_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "a", (void*)a.data(), &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  if (layout == TILEDB_GLOBAL_ORDER) {
    rc = tiledb_query_submit_and_finalize(ctx, query);
  } else {
    rc = tiledb_query_submit(ctx, query);
  }
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void StringDimsFx::write_array_2d(
    tiledb_ctx_t* ctx,
    const std::string& array_name,
    tiledb_layout_t layout,
    const std::vector<uint64_t>& d1_off,
    const std::string& d1_val,
    const std::vector<int32_t>& d2,
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

  uint64_t d1_off_size = d1_off.size() * sizeof(uint64_t);
  uint64_t d1_val_size = d1_val.size();
  uint64_t d2_size = d2.size() * sizeof(int32_t);
  uint64_t a_size = a.size() * sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "d1", (void*)&d1_val[0], &d1_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "d1", (uint64_t*)&d1_off[0], &d1_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "d2", (void*)d2.data(), &d2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "a", (void*)a.data(), &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  if (layout == TILEDB_GLOBAL_ORDER) {
    rc = tiledb_query_submit_and_finalize(ctx, query);
  } else {
    rc = tiledb_query_submit(ctx, query);
  }
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

int StringDimsFx::tiledb_array_get_non_empty_domain_from_name_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* domain,
    int32_t* is_empty) {
  int ret = tiledb_array_get_non_empty_domain_from_name(
      ctx, array, name, domain, is_empty);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_)
    return ret;

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_non_empty_domain_all_dimensions(
          ctx,
          array,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_array_non_empty_domain_all_dimensions(
          ctx,
          array,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1) == TILEDB_OK);

  tiledb_buffer_free(&buff);

  return tiledb_array_get_non_empty_domain_from_name(
      ctx, array, name, domain, is_empty);
}

void StringDimsFx::get_non_empty_domain(
    const std::string& array_name,
    const std::string& dim_name,
    std::vector<int32_t>* dom,
    int32_t* is_empty) {
  dom->resize(2);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Get non-empty domain
  rc = tiledb_array_get_non_empty_domain_from_name_wrapper(
      ctx_, array, dim_name.c_str(), &(*dom)[0], is_empty);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
}

int StringDimsFx::tiledb_array_get_non_empty_domain_var_size_from_name_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    uint64_t* start_size,
    uint64_t* end_size,
    int32_t* is_empty) {
  int ret = tiledb_array_get_non_empty_domain_var_size_from_name(
      ctx, array, name, start_size, end_size, is_empty);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_)
    return ret;

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_non_empty_domain_all_dimensions(
          ctx,
          array,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_array_non_empty_domain_all_dimensions(
          ctx,
          array,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1) == TILEDB_OK);

  tiledb_buffer_free(&buff);

  return tiledb_array_get_non_empty_domain_var_size_from_name(
      ctx, array, name, start_size, end_size, is_empty);
}

int StringDimsFx::tiledb_array_get_non_empty_domain_var_from_name_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* start,
    void* end,
    int32_t* is_empty) {
  int ret = tiledb_array_get_non_empty_domain_var_from_name(
      ctx, array, name, start, end, is_empty);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_)
    return ret;

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array_non_empty_domain_all_dimensions(
          ctx,
          array,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_array_non_empty_domain_all_dimensions(
          ctx,
          array,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1) == TILEDB_OK);

  tiledb_buffer_free(&buff);

  return tiledb_array_get_non_empty_domain_var_from_name(
      ctx, array, name, start, end, is_empty);
}

void StringDimsFx::get_non_empty_domain_var(
    const std::string& array_name,
    const std::string& dim_name,
    std::string* start,
    std::string* end,
    int32_t* is_empty) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Get non-empty domain size
  uint64_t start_size = 0, end_size = 0;
  rc = tiledb_array_get_non_empty_domain_var_size_from_name_wrapper(
      ctx_, array, dim_name.c_str(), &start_size, &end_size, is_empty);
  CHECK(rc == TILEDB_OK);

  // Get non-empty domain
  start->resize(start_size);
  end->resize(end_size);
  rc = tiledb_array_get_non_empty_domain_var_from_name_wrapper(
      ctx_, array, dim_name.c_str(), &(*start)[0], &(*end)[0], is_empty);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
}

void StringDimsFx::get_est_result_size_var(
    tiledb_array_t* array,
    unsigned dim_idx,
    const std::string& dim_name,
    const std::string& start,
    const std::string& end,
    uint64_t* size_off,
    uint64_t* size_val) {
  tiledb_query_t* query;
  int rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range_var(
      ctx_,
      subarray,
      dim_idx,
      start.data(),
      start.size(),
      end.data(),
      end.size());
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_var(
      ctx_, query, dim_name.c_str(), size_off, size_val);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

void StringDimsFx::read_array_1d(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_layout_t layout,
    const std::string& start,
    const std::string& end,
    std::vector<uint64_t>* d_off,
    std::string* d_val,
    std::vector<int32_t>* a,
    tiledb_query_status_t* status,
    tiledb_query_status_details_t* details) {
  // Create query
  tiledb_query_t* query;
  int rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range_var(
      ctx, subarray, 0, start.data(), start.size(), end.data(), end.size());
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Check range num
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);

  // Check getting range from an invalid range index
  uint64_t start_size = 0, end_size = 0;
  rc = tiledb_subarray_get_range_var_size(
      ctx_, subarray, 0, 2, &start_size, &end_size);
  CHECK(rc == TILEDB_ERR);
  std::vector<char> start_data(start_size);
  std::vector<char> end_data(end_size);
  rc = tiledb_subarray_get_range_var(
      ctx_, subarray, 0, 2, start_data.data(), end_data.data());
  CHECK(rc == TILEDB_ERR);

  // Check ranges
  rc = tiledb_subarray_get_range_var_size(
      ctx_, subarray, 0, 0, &start_size, &end_size);
  CHECK(rc == TILEDB_OK);
  start_data.resize(start_size);
  end_data.resize(end_size);
  rc = tiledb_subarray_get_range_var(
      ctx_, subarray, 0, 0, start_data.data(), end_data.data());
  CHECK(rc == TILEDB_OK);
  CHECK(std::string(start_data.data(), start_data.size()) == start);
  CHECK(std::string(end_data.data(), end_data.size()) == end);

  // Set query buffers
  uint64_t d_off_size = d_off->size() * sizeof(uint64_t);
  uint64_t d_val_size = d_val->size();
  uint64_t a_size = a->size() * sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "d", (void*)d_val->data(), &d_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "d", (uint64_t*)d_off->data(), &d_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "a", a->data(), &a_size);
  REQUIRE(rc == TILEDB_OK);

  // Set layout
  rc = tiledb_query_set_layout(ctx, query, layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  const char* array_uri;
  rc = tiledb_array_get_uri(ctx, array, &array_uri);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Get status
  rc = tiledb_query_get_status(ctx, query, status);
  CHECK(rc == TILEDB_OK);

  // Get details
  rc = tiledb_query_get_status_details(ctx, query, details);
  CHECK(rc == TILEDB_OK);

  // Resize the result buffers
  d_off->resize(d_off_size / sizeof(uint64_t));
  d_val->resize(d_val_size / sizeof(char));
  a->resize(a_size / sizeof(int32_t));

  // Clean up
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

void StringDimsFx::read_array_2d(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_layout_t layout,
    const std::string& d1_start,
    const std::string& d1_end,
    int32_t d2_start,
    int32_t d2_end,
    std::vector<uint64_t>* d1_off,
    std::string* d1_val,
    std::vector<int32_t>* d2,
    std::vector<int32_t>* a,
    tiledb_query_status_t* status) {
  // Create query
  tiledb_query_t* query;
  int rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range_var(
      ctx,
      subarray,
      0,
      d1_start.data(),
      d1_start.size(),
      d1_end.data(),
      d1_end.size());
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx, subarray, 1, &d2_start, &d2_end, nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Check range num d1
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  // Check range num d2
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 1, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);

  // Check getting range from an invalid range index
  uint64_t d1_start_size = 0, d1_end_size = 0;
  rc = tiledb_subarray_get_range_var_size(
      ctx_, subarray, 0, 2, &d1_start_size, &d1_end_size);
  CHECK(rc == TILEDB_ERR);
  std::vector<char> d1_start_data(d1_start_size);
  std::vector<char> d1_end_data(d1_end_size);
  rc = tiledb_subarray_get_range_var(
      ctx_, subarray, 0, 2, d1_start_data.data(), d1_end_data.data());
  CHECK(rc == TILEDB_ERR);

  // Check ranges
  rc = tiledb_subarray_get_range_var_size(
      ctx_, subarray, 0, 0, &d1_start_size, &d1_end_size);
  CHECK(rc == TILEDB_OK);
  d1_start_data.resize(d1_start_size);
  d1_end_data.resize(d1_end_size);
  rc = tiledb_subarray_get_range_var(
      ctx_, subarray, 0, 0, d1_start_data.data(), d1_end_data.data());
  CHECK(rc == TILEDB_OK);
  CHECK(std::string(d1_start_data.data(), d1_start_data.size()) == d1_start);
  CHECK(std::string(d1_end_data.data(), d1_end_data.size()) == d1_end);

  const void *d2_start_data, *d2_end_data, *stride;
  rc = tiledb_subarray_get_range(
      ctx_, subarray, 1, 0, &d2_start_data, &d2_end_data, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int32_t*)d2_start_data == d2_start);
  CHECK(*(int32_t*)d2_end_data == d2_end);
  CHECK(stride == nullptr);

  // Set query buffers
  uint64_t d1_off_size = d1_off->size() * sizeof(uint64_t);
  uint64_t d1_val_size = d1_val->size();
  uint64_t d2_size = d2->size() * sizeof(int32_t);
  uint64_t a_size = a->size() * sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "d1", (void*)d1_val->data(), &d1_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "d1", (uint64_t*)d1_off->data(), &d1_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "d2", d2->data(), &d2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "a", a->data(), &a_size);
  REQUIRE(rc == TILEDB_OK);

  // Set layout
  rc = tiledb_query_set_layout(ctx, query, layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  const char* array_uri;
  rc = tiledb_array_get_uri(ctx, array, &array_uri);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Get status
  rc = tiledb_query_get_status(ctx, query, status);
  CHECK(rc == TILEDB_OK);

  // Resize the result buffers
  d1_off->resize(d1_off_size / sizeof(uint64_t));
  d1_val->resize(d1_val_size / sizeof(char));
  d2->resize(d2_size / sizeof(int32_t));
  a->resize(a_size / sizeof(int32_t));

  // Clean up
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

void StringDimsFx::read_array_2d_default_string_range(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_layout_t layout,
    int32_t d2_start,
    int32_t d2_end,
    std::vector<uint64_t>* d1_off,
    std::string* d1_val,
    std::vector<int32_t>* d2,
    std::vector<int32_t>* a,
    tiledb_query_status_t* status) {
  // Create query
  tiledb_query_t* query;
  int rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  // Add range for int dimension
  rc = tiledb_subarray_add_range(ctx, subarray, 1, &d2_start, &d2_end, nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Set query buffers
  uint64_t d1_off_size = d1_off->size() * sizeof(uint64_t);
  uint64_t d1_val_size = d1_val->size();
  uint64_t d2_size = d2->size() * sizeof(int32_t);
  uint64_t a_size = a->size() * sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "d1", (void*)d1_val->data(), &d1_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "d1", (uint64_t*)d1_off->data(), &d1_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "d2", d2->data(), &d2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "a", a->data(), &a_size);
  REQUIRE(rc == TILEDB_OK);

  // Set layout
  rc = tiledb_query_set_layout(ctx, query, layout);
  REQUIRE(rc == TILEDB_OK);

  const char* array_uri;
  rc = tiledb_array_get_uri(ctx, array, &array_uri);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Get status
  rc = tiledb_query_get_status(ctx, query, status);
  CHECK(rc == TILEDB_OK);

  // Resize the result buffers
  d1_off->resize(d1_off_size / sizeof(uint64_t));
  d1_val->resize(d1_val_size / sizeof(char));
  d2->resize(d2_size / sizeof(int32_t));
  a->resize(a_size / sizeof(int32_t));

  // Clean up
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, array schema",
    "[capi][sparse][string-dims][array-schema][rest]") {
  std::string array_name = prefix_ + "string_dims";

  // Create dimension
  tiledb_domain_t* domain;
  tiledb_dimension_t* d;
  char tmp;
  int rc =
      tiledb_dimension_alloc(ctx_, "d", TILEDB_STRING_ASCII, &tmp, nullptr, &d);
  REQUIRE(rc == TILEDB_ERR);
  rc =
      tiledb_dimension_alloc(ctx_, "d", TILEDB_STRING_ASCII, nullptr, &tmp, &d);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_dimension_alloc(
      ctx_, "d", TILEDB_STRING_ASCII, nullptr, nullptr, &d);
  REQUIRE(rc == TILEDB_OK);

  // Setting cell val num to a TILEDB_STRING_ASCII dimension should error out
  rc = tiledb_dimension_set_cell_val_num(ctx_, d, 4);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_dimension_set_cell_val_num(ctx_, d, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d);
  REQUIRE(rc == TILEDB_OK);

  // Setting a string dimension to a dense array should error out
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_ERR);
  tiledb_array_schema_free(&array_schema);

  // Create sparse array schema
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Set domain to schema
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  REQUIRE(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = array_create_wrapper(array_name, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_array_schema_free(&array_schema);
  tiledb_dimension_free(&d);
  tiledb_domain_free(&domain);
  tiledb_attribute_free(&a);

  // Load array schema and domain
  rc = array_schema_load_wrapper(array_name, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_get_domain(ctx_, array_schema, &domain);
  REQUIRE(rc == TILEDB_OK);

  // Get dimension
  rc = tiledb_domain_get_dimension_from_index(ctx_, domain, 0, &d);
  REQUIRE(rc == TILEDB_OK);

  // Check dimension type, domain and tile extent
  tiledb_datatype_t type;
  rc = tiledb_dimension_get_type(ctx_, d, &type);
  REQUIRE(rc == TILEDB_OK);
  CHECK(type == TILEDB_STRING_ASCII);
  const void *dom, *extent;
  rc = tiledb_dimension_get_domain(ctx_, d, &dom);
  REQUIRE(rc == TILEDB_OK);
  CHECK(dom == nullptr);
  rc = tiledb_dimension_get_tile_extent(ctx_, d, &extent);
  REQUIRE(rc == TILEDB_OK);
  CHECK(extent == nullptr);

  // Clean up
  tiledb_array_schema_free(&array_schema);
  tiledb_domain_free(&domain);
  tiledb_dimension_free(&d);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, check duplicates, global "
    "order",
    "[capi][sparse][string-dims][duplicates][global][rest]") {
  std::string array_name = prefix_ + "string_dims";

  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create and submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);

  char d_data[] = "aabbbbdddd";
  uint64_t d_data_size = sizeof(d_data) - 1;  // Ignore '\0'
  uint64_t d_off[] = {0, 2, 4, 6};
  uint64_t d_off_size = sizeof(d_off);
  int32_t a_data[] = {1, 2, 3, 4};
  uint64_t a_size = sizeof(a_data);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", d_data, &d_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "d", d_off, &d_off_size);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a_data, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, check duplicates, "
    "unordered",
    "[capi][sparse][string-dims][duplicates][unordered][rest]") {
  std::string array_name = prefix_ + +"string_dims";

  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create and submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);

  char d_data[] = "ddddbbaabb";
  uint64_t d_data_size = sizeof(d_data) - 1;  // Ignore '\0'
  uint64_t d_off[] = {0, 4, 6, 8};
  uint64_t d_off_size = sizeof(d_off);
  int32_t a_data[] = {1, 2, 3, 4};
  uint64_t a_size = sizeof(a_data);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", d_data, &d_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "d", d_off, &d_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a_data, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, check global order "
    "violation",
    "[capi][sparse][string-dims][global-order][violation][rest]") {
  std::string array_name = prefix_ + "string_dims";

  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create and submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);

  char d_data[] = "ddddbbbcaa";
  uint64_t d_data_size = sizeof(d_data) - 1;  // Ignore '\0'
  uint64_t d_off[] = {0, 4, 6, 8};
  uint64_t d_off_size = sizeof(d_off);
  int32_t a_data[] = {1, 2, 3, 4};
  uint64_t a_size = sizeof(a_data);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", d_data, &d_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "d", d_off, &d_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a_data, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, errors",
    "[capi][sparse][string-dims][errors][rest-fails][sc-43167]") {
  std::string array_name = prefix_ + "string_dims";

  // Create array
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // ####### WRITE #######

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create and submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);

  char d_data[] = "ccbbddddaa";
  uint64_t d_data_size = sizeof(d_data) - 1;  // Ignore '\0'
  uint64_t d_off[] = {0, 2, 4, 8};
  uint64_t d_off_size = sizeof(d_off);
  int32_t a_data[] = {3, 2, 4, 1};
  uint64_t a_size = sizeof(a_data);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", d_data, &d_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "d", d_off, &d_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a_data, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // ####### CHECK ERRORS #######
  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Get non-empty domain and array max buffer sizes
  int32_t dom[4];
  int32_t is_empty;
  uint64_t size = 1024;
  rc = tiledb_array_get_non_empty_domain(ctx_, array, dom, &is_empty);
  CHECK(rc == TILEDB_ERR);

  // Set subarray and buffer
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* sub;
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, dom);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  int32_t buff[10];
  uint64_t buff_size = sizeof(buff);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, TILEDB_COORDS, buff, &buff_size);
  REQUIRE(rc == TILEDB_ERR);
  int data[1];
  uint64_t data_size = 4;
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", data, &data_size);
  REQUIRE(rc == TILEDB_OK);

  // Get estimated buffer size
  rc = tiledb_query_get_est_result_size(ctx_, query, TILEDB_COORDS, &size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_est_result_size(ctx_, query, "d", &size);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, 1d",
    "[capi][sparse][string-dims][1d][basic][rest]") {
  std::string array_name = prefix_ + "string_dims";

  // Create array
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Write
  std::vector<uint64_t> d_off = {0, 2, 4, 8};
  std::string d_val("ccbbddddaa");
  std::vector<int32_t> a = {3, 2, 4, 1};
  write_array_1d(ctx_, array_name, TILEDB_UNORDERED, d_off, d_val, a);

  // ####### READ #######

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Check proper errors for getting non-empty domain
  char dom[100];
  int32_t is_empty = false;
  uint64_t start_size, end_size;
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 0, dom, &is_empty);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "d", dom, &is_empty);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_array_get_non_empty_domain_var_size_from_index(
      ctx_, array, 2, &start_size, &end_size, &is_empty);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_array_get_non_empty_domain_var_size_from_name(
      ctx_, array, "foo", &start_size, &end_size, &is_empty);
  CHECK(rc == TILEDB_ERR);

  std::string start, end;
  get_non_empty_domain_var(array_name, "d", &start, &end, &is_empty);
  CHECK(is_empty == 0);
  CHECK(start == "aa");
  CHECK(end == "dddd");

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray = nullptr;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  char s1[] = "a";
  char s2[] = "ee";

  // Check we can add empty ranges
  rc = tiledb_subarray_add_range_var(ctx_, subarray, 0, s1, 0, s2, 2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range_var(ctx_, subarray, 0, s1, 1, s2, 0);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range_var(ctx_, subarray, 0, nullptr, 0, s2, 2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range_var(ctx_, subarray, 0, s1, 1, nullptr, 0);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Clean query and re-alloc
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);

  // Check errors when adding range
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, s1, s2, nullptr);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_add_range_var(ctx_, subarray, 1, s1, 1, s2, 2);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_add_range_var(ctx_, subarray, 0, nullptr, 1, s2, 2);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_subarray_add_range_var(ctx_, subarray, 0, s1, 1, nullptr, 2);
  CHECK(rc == TILEDB_ERR);

  // Add string range
  rc = tiledb_subarray_add_range_var(ctx_, subarray, 0, s1, 1, s2, 2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Check error on getting estimated result size
  uint64_t size_off = 0, size_val = 0;
  rc = tiledb_query_get_est_result_size(ctx_, query, "d", &size_off);
  CHECK(rc == TILEDB_ERR);

  // Get estimated result size
  rc = tiledb_query_get_est_result_size_var(
      ctx_, query, "d", &size_off, &size_val);
  CHECK(rc == TILEDB_OK);
  CHECK(size_off == 32);
  CHECK(size_val == 10);

  // Clean query
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);

  // Set layout
  tiledb_layout_t layout = TILEDB_ROW_MAJOR;
  SECTION("Global order") {
    layout = TILEDB_GLOBAL_ORDER;
  }
  SECTION("Row-major") {
    layout = TILEDB_ROW_MAJOR;
  }
  SECTION("Col-major") {
    layout = TILEDB_COL_MAJOR;
  }
  SECTION("Unordered") {
    layout = TILEDB_UNORDERED;
  }

  // Read [a, ee]
  std::vector<uint64_t> r_d_off(10);
  std::string r_d_val;
  r_d_val.resize(20);
  std::vector<int32_t> r_a(10);
  tiledb_query_status_t status;
  tiledb_query_status_details_t details;
  read_array_1d(
      ctx_,
      array,
      layout,
      "a",
      "ee",
      &r_d_off,
      &r_d_val,
      &r_a,
      &status,
      &details);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d_val == "aabbccdddd");
  std::vector<uint64_t> c_d_off = {0, 2, 4, 6};
  CHECK(r_d_off == c_d_off);
  std::vector<int32_t> c_a = {1, 2, 3, 4};
  CHECK(r_a == c_a);

  // Read [aab, cc]
  r_d_off.resize(10);
  r_d_val.resize(20);
  r_a.resize(10);
  read_array_1d(
      ctx_,
      array,
      layout,
      "aab",
      "cc",
      &r_d_off,
      &r_d_val,
      &r_a,
      &status,
      &details);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d_val == "bbcc");
  c_d_off = {0, 2};
  CHECK(r_d_off == c_d_off);
  c_a = {2, 3};
  CHECK(r_a == c_a);

  // Read [aa, cc] - INCOMPLETE
  r_d_off.resize(2);
  r_d_val.resize(20);
  r_a.resize(10);
  read_array_1d(
      ctx_,
      array,
      layout,
      "aa",
      "cc",
      &r_d_off,
      &r_d_val,
      &r_a,
      &status,
      &details);
  CHECK(status == TILEDB_INCOMPLETE);
  CHECK(r_d_val == "aabb");
  c_d_off = {0, 2};
  CHECK(r_d_off == c_d_off);
  c_a = {1, 2};
  CHECK(r_a == c_a);
  CHECK(details.incomplete_reason == TILEDB_REASON_USER_BUFFER_SIZE);

  // Read [aa, cc] - INCOMPLETE, no result
  r_d_off.resize(1);
  r_d_val.resize(1);
  r_a.resize(10);
  read_array_1d(
      ctx_,
      array,
      layout,
      "aa",
      "bb",
      &r_d_off,
      &r_d_val,
      &r_a,
      &status,
      &details);
  CHECK(status == TILEDB_INCOMPLETE);
  CHECK(r_d_val.size() == 0);
  CHECK(r_d_off.size() == 0);
  CHECK(r_a.size() == 0);
  CHECK(details.incomplete_reason == TILEDB_REASON_USER_BUFFER_SIZE);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, 1d, consolidation",
    "[capi][sparse][string-dims][1d][consolidation][non-rest]") {
  std::string array_name = prefix_ + "string_dims";

  // Create array
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Write #1
  std::vector<uint64_t> d_off = {0, 2, 4, 8};
  std::string d_val("ccbbddddaa");
  std::vector<int32_t> a = {3, 2, 4, 1};
  write_array_1d(ctx_, array_name, TILEDB_UNORDERED, d_off, d_val, a);

  // Write #2
  d_off = {0, 1, 2};
  d_val = "abee";
  a = {5, 6, 7};
  write_array_1d(ctx_, array_name, TILEDB_GLOBAL_ORDER, d_off, d_val, a);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Get non-empty domain
  std::string start, end;
  int32_t is_empty;
  get_non_empty_domain_var(array_name, "d", &start, &end, &is_empty);
  CHECK(is_empty == 0);
  CHECK(start == "a");
  CHECK(end == "ee");

  // Get estimated result size
  uint64_t size_off = 0, size_val = 0;
  get_est_result_size_var(array, 0, "d", "a", "ee", &size_off, &size_val);
  CHECK(size_off == 56);
  CHECK(size_val == 14);

  // Set layout
  tiledb_layout_t layout = TILEDB_ROW_MAJOR;
  SECTION("Global order") {
    layout = TILEDB_GLOBAL_ORDER;
  }
  SECTION("Row-major") {
    layout = TILEDB_ROW_MAJOR;
  }
  SECTION("Col-major") {
    layout = TILEDB_COL_MAJOR;
  }
  SECTION("Unordered") {
    layout = TILEDB_UNORDERED;
  }

  // Read [a, ee]
  std::vector<uint64_t> r_d_off(10);
  std::string r_d_val;
  r_d_val.resize(20);
  std::vector<int32_t> r_a(10);
  tiledb_query_status_t status;
  tiledb_query_status_details_t details;
  read_array_1d(
      ctx_,
      array,
      layout,
      "a",
      "ee",
      &r_d_off,
      &r_d_val,
      &r_a,
      &status,
      &details);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d_val == "aaabbbccddddee");
  std::vector<uint64_t> c_d_off = {0, 1, 3, 4, 6, 8, 12};
  CHECK(r_d_off == c_d_off);
  std::vector<int32_t> c_a = {5, 1, 6, 2, 3, 4, 7};
  CHECK(r_a == c_a);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Check number of fragments
  get_num_struct dirs = {ctx_, vfs_, 0};
  auto frag_dir = get_fragment_dir(array_name);
  rc = tiledb_vfs_ls(ctx_, vfs_, frag_dir.c_str(), &get_dir_num, &dirs);
  CHECK(rc == TILEDB_OK);
  CHECK(dirs.num == 2);

  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  rc = tiledb_config_set(
      config, "sm.consolidation.buffer_size", "10000", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate
  rc = tiledb_array_consolidate(ctx_, array_name.c_str(), config);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_vacuum(ctx_, array_name.c_str(), nullptr);
  tiledb_config_free(&config);

  // Check number of fragments
  dirs = {ctx_, vfs_, 0};
  rc = tiledb_vfs_ls(ctx_, vfs_, frag_dir.c_str(), &get_dir_num, &dirs);
  CHECK(rc == TILEDB_OK);
  CHECK(dirs.num == 1);

  // Get non-empty domain
  start = "";
  end = "";
  get_non_empty_domain_var(array_name, "d", &start, &end, &is_empty);
  CHECK(is_empty == 0);
  CHECK(start == "a");
  CHECK(end == "ee");

  // Free array
  tiledb_array_free(&array);

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Read [a, ee]
  r_d_off.resize(10);
  r_d_val.resize(20);
  r_a.resize(10);
  read_array_1d(
      ctx_,
      array,
      layout,
      "a",
      "ee",
      &r_d_off,
      &r_d_val,
      &r_a,
      &status,
      &details);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d_val == "aaabbbccddddee");
  CHECK(r_d_off == c_d_off);
  CHECK(r_a == c_a);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, 1d, allow duplicates",
    "[capi][sparse][string-dims][1d][allow-dups][rest]") {
  std::string array_name = prefix_ + "string_dims";

  // Create array
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      true,
      false);

  // Write
  std::vector<uint64_t> d_off = {0, 2, 4, 8};
  std::string d_val("ccccddddaa");
  std::vector<int32_t> a = {2, 3, 4, 1};
  write_array_1d(ctx_, array_name, TILEDB_UNORDERED, d_off, d_val, a);

  // ####### READ #######

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Check non-empty domain
  std::string start, end;
  int32_t is_empty;
  get_non_empty_domain_var(array_name, "d", &start, &end, &is_empty);
  CHECK(is_empty == 0);
  CHECK(start == "aa");
  CHECK(end == "dddd");

  // Set layout
  tiledb_layout_t layout = TILEDB_ROW_MAJOR;
  SECTION("Global order") {
    layout = TILEDB_GLOBAL_ORDER;
  }
  SECTION("Row-major") {
    layout = TILEDB_ROW_MAJOR;
  }
  SECTION("Col-major") {
    layout = TILEDB_COL_MAJOR;
  }
  SECTION("Unordered") {
    layout = TILEDB_UNORDERED;
  }

  // Read [a, e]
  std::vector<uint64_t> r_d_off(10);
  std::string r_d_val;
  r_d_val.resize(20);
  std::vector<int32_t> r_a(10);
  tiledb_query_status_t status;
  tiledb_query_status_details_t details;
  read_array_1d(
      ctx_,
      array,
      layout,
      "a",
      "e",
      &r_d_off,
      &r_d_val,
      &r_a,
      &status,
      &details);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d_val == "aaccccdddd");
  std::vector<uint64_t> c_d_off = {0, 2, 4, 6};
  CHECK(r_d_off == c_d_off);
  // The ordering of 'a' is undefined for duplicate dimension
  // elements. Check both for dimension element "c".
  std::vector<int32_t> c_a_1 = {1, 3, 2, 4};
  std::vector<int32_t> c_a_2 = {1, 2, 3, 4};
  const bool c_a_matches = r_a == c_a_1 || r_a == c_a_2;
  CHECK(c_a_matches);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, 1d, dedup",
    "[capi][sparse][string-dims][1d][dedup][rest]") {
  std::string array_name = prefix_ + "string_dims";

  // Create array
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Create config
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  int rc = tiledb_config_alloc(&config, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_set(config, "sm.dedup_coords", "true", &error);
  CHECK(rc == TILEDB_OK);

  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
  // reallocate with input config
  vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok();
  tiledb_config_free(&config);

  // Write
  std::vector<uint64_t> d_off = {0, 2, 4, 8};
  std::string d_val("ccccddddaa");
  std::vector<int32_t> a = {2, 3, 4, 1};
  write_array_1d(ctx_, array_name, TILEDB_UNORDERED, d_off, d_val, a);

  // Clean up
  tiledb_config_free(&config);

  // ####### READ #######

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Check non-empty domain
  std::string start, end;
  int32_t is_empty;
  get_non_empty_domain_var(array_name, "d", &start, &end, &is_empty);
  CHECK(is_empty == 0);
  CHECK(start == "aa");
  CHECK(end == "dddd");

  // Set layout
  tiledb_layout_t layout = TILEDB_ROW_MAJOR;
  SECTION("Global order") {
    layout = TILEDB_GLOBAL_ORDER;
  }
  SECTION("Row-major") {
    layout = TILEDB_ROW_MAJOR;
  }
  SECTION("Col-major") {
    layout = TILEDB_COL_MAJOR;
  }
  SECTION("Unordered") {
    layout = TILEDB_UNORDERED;
  }

  // Read [a, e]
  std::vector<uint64_t> r_d_off(10);
  std::string r_d_val;
  r_d_val.resize(20);
  std::vector<int32_t> r_a(10);
  tiledb_query_status_t status;
  tiledb_query_status_details_t details;
  read_array_1d(
      ctx_,
      array,
      layout,
      "a",
      "e",
      &r_d_off,
      &r_d_val,
      &r_a,
      &status,
      &details);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d_val == "aaccdddd");
  std::vector<uint64_t> c_d_off = {0, 2, 4};
  CHECK(r_d_off == c_d_off);
  // Either value for dimension index 'cc' may be de-duped.
  std::vector<int32_t> c_a_1 = {1, 2, 4};
  std::vector<int32_t> c_a_2 = {1, 3, 4};
  const bool c_a_matches = r_a == c_a_1 || r_a == c_a_2;
  CHECK(c_a_matches);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, 2d",
    "[capi][sparse][string-dims][2d][rest]") {
  std::string array_name = prefix_ + "/string_dims";

  // Create array
  int32_t dom[] = {1, 10};
  int32_t extent = 5;
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_STRING_ASCII, TILEDB_INT32},
      {nullptr, dom},
      {nullptr, &extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Write
  tiledb_layout_t write_layout = TILEDB_UNORDERED;
  SECTION("Unordered write") {
    write_layout = TILEDB_UNORDERED;
  }
  SECTION("Global write") {
    write_layout = TILEDB_GLOBAL_ORDER;
  }
  std::vector<uint64_t> d1_off = {0, 2, 4, 6};
  std::string d1_val("aabbccdddd");
  std::vector<int32_t> d2 = {1, 2, 3, 4};
  std::vector<int32_t> a = {11, 12, 13, 14};
  write_array_2d(ctx_, array_name, write_layout, d1_off, d1_val, d2, a);

  // ####### READ #######

  // Check non-empty domain
  std::string start, end;
  int32_t is_empty;
  get_non_empty_domain_var(array_name, "d1", &start, &end, &is_empty);
  CHECK(is_empty == 0);
  CHECK(start == "aa");
  CHECK(end == "dddd");
  std::vector<int32_t> non_empty;
  get_non_empty_domain(array_name, "d2", &non_empty, &is_empty);
  CHECK(is_empty == 0);
  CHECK(non_empty[0] == 1);
  CHECK(non_empty[1] == 4);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Set layout
  tiledb_layout_t layout = TILEDB_ROW_MAJOR;

  SECTION("Global order") {
    layout = TILEDB_GLOBAL_ORDER;
  }
  SECTION("Row-major read") {
    layout = TILEDB_ROW_MAJOR;
  }
  SECTION("Col-major") {
    layout = TILEDB_COL_MAJOR;
  }
  SECTION("Unordered") {
    layout = TILEDB_UNORDERED;
  }

  // Read [a, e], [1, 10]
  std::vector<uint64_t> r_d1_off(10);
  std::string r_d1_val;
  r_d1_val.resize(20);
  std::vector<int32_t> r_d2(10);
  std::vector<int32_t> r_a(10);
  tiledb_query_status_t status;
  read_array_2d(
      ctx_,
      array,
      layout,
      "a",
      "e",
      1,
      10,
      &r_d1_off,
      &r_d1_val,
      &r_d2,
      &r_a,
      &status);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d1_val == "aabbccdddd");
  std::vector<uint64_t> c_d1_off = {0, 2, 4, 6};
  CHECK(r_d1_off == c_d1_off);
  std::vector<int32_t> c_d2 = {1, 2, 3, 4};
  CHECK(r_d2 == c_d2);
  std::vector<int32_t> c_a = {11, 12, 13, 14};
  CHECK(r_a == c_a);

  // Read [a, cc], [2, 3]
  r_d1_off.resize(10);
  r_d1_val.resize(20);
  r_d2.resize(10);
  r_a.resize(10);
  read_array_2d(
      ctx_,
      array,
      layout,
      "a",
      "cc",
      2,
      3,
      &r_d1_off,
      &r_d1_val,
      &r_d2,
      &r_a,
      &status);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d1_val == "bbcc");
  c_d1_off = {0, 2};
  CHECK(r_d1_off == c_d1_off);
  c_d2 = {2, 3};
  CHECK(r_d2 == c_d2);
  c_a = {12, 13};
  CHECK(r_a == c_a);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Write again
  d1_off = {0, 1, 2};
  d1_val = "abff";
  d2 = {2, 2, 3};
  a = {15, 16, 17};
  write_array_2d(ctx_, array_name, write_layout, d1_off, d1_val, d2, a);

  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Create config
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  rc = tiledb_config_alloc(&config, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.mode", "fragment_meta", &error);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_set(
      config, "sm.mem.consolidation.buffers_weight", "1", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.mem.consolidation.reader_weight", "5000", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.mem.consolidation.writer_weight", "5000", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Consolidate fragment metadata
  rc = tiledb_array_consolidate(ctx_, array_name.c_str(), config);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Read [a, ff], [1, 10]
  r_d1_off.resize(20);
  r_d1_val.resize(20);
  r_d2.resize(20);
  r_a.resize(20);
  read_array_2d(
      ctx_,
      array,
      TILEDB_GLOBAL_ORDER,
      "a",
      "ff",
      1,
      10,
      &r_d1_off,
      &r_d1_val,
      &r_d2,
      &r_a,
      &status);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d1_val == "aaabbbccddddff");
  c_d1_off = {0, 1, 3, 4, 6, 8, 12};
  CHECK(r_d1_off == c_d1_off);
  c_d2 = {2, 1, 2, 2, 3, 4, 3};
  CHECK(r_d2 == c_d2);
  c_a = {15, 11, 16, 12, 13, 14, 17};
  CHECK(r_a == c_a);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);

  if (!fs_vec_[0]->is_rest()) {
    rc =
        tiledb_config_set(config, "sm.consolidation.mode", "fragments", &error);
    CHECK(rc == TILEDB_OK);

    // Consolidate
    rc = tiledb_array_consolidate(ctx_, array_name.c_str(), config);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_vacuum(ctx_, array_name.c_str(), nullptr);
    CHECK(rc == TILEDB_OK);

    // Open array
    rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    CHECK(rc == TILEDB_OK);

    // Read [a, ff], [1, 10]
    r_d1_off.resize(20);
    r_d1_val.resize(20);
    r_d2.resize(20);
    r_a.resize(20);
    read_array_2d(
        ctx_,
        array,
        TILEDB_GLOBAL_ORDER,
        "a",
        "ff",
        1,
        10,
        &r_d1_off,
        &r_d1_val,
        &r_d2,
        &r_a,
        &status);
    CHECK(status == TILEDB_COMPLETED);
    CHECK(r_d1_val == "aaabbbccddddff");
    c_d1_off = {0, 1, 3, 4, 6, 8, 12};
    CHECK(r_d1_off == c_d1_off);
    c_d2 = {2, 1, 2, 2, 3, 4, 3};
    CHECK(r_d2 == c_d2);
    c_a = {15, 11, 16, 12, 13, 14, 17};
    CHECK(r_a == c_a);

    // Close array
    rc = tiledb_array_close(ctx_, array);
    CHECK(rc == TILEDB_OK);

    // Clean up
    tiledb_array_free(&array);
    tiledb_config_free(&config);
  }
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test multiple var size global writes 1",
    "[capi][sparse][var-size][multiple-global-writes-1][rest-fails][sc-"
    "43168]") {
  std::string array_name = prefix_ + "string_dims";

  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      1,
      false,
      false);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create and submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);

  // Write "a, 1"
  char d_data[] = "abcd";
  uint64_t d_data_size = 1;
  uint64_t d_off[] = {0, 1, 2, 3};
  uint64_t d_off_size = sizeof(uint64_t);
  int32_t a_data[] = {1, 2, 3, 4};
  uint64_t a_size = sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", d_data, &d_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "d", d_off, &d_off_size);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a_data, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Write "b, 2"
  d_data[0] = 'b';
  a_data[0] = 2;

  // Submit query
  rc = tiledb_query_submit_and_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Read [a, ee]
  std::vector<uint64_t> r_d_off(10);
  std::string r_d_val;
  r_d_val.resize(20);
  std::vector<int32_t> r_a(10);
  tiledb_query_status_t status;
  tiledb_query_status_details_t details;
  read_array_1d(
      ctx_,
      array,
      TILEDB_GLOBAL_ORDER,
      "a",
      "e",
      &r_d_off,
      &r_d_val,
      &r_a,
      &status,
      &details);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d_val == "ab");
  std::vector<uint64_t> c_d_off = {0, 1};
  CHECK(r_d_off == c_d_off);
  std::vector<int32_t> c_a = {1, 2};
  CHECK(r_a == c_a);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test multiple var size global writes 2",
    "[capi][sparse][var-size][multiple-global-writes-2][rest-fails][sc-"
    "43168]") {
  std::string array_name = prefix_ + "string_dims";

  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d"},
      {TILEDB_STRING_ASCII},
      {nullptr},
      {nullptr},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      1,
      false,
      false);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create and submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);

  // Write "a, 1"
  char d_data[] = "abcd";
  uint64_t d_data_size = 1;
  uint64_t d_off[] = {0, 1, 2, 3};
  uint64_t d_off_size = sizeof(uint64_t);
  int32_t a_data[] = {1, 2, 3, 4};
  uint64_t a_size = sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d", d_data, &d_data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "d", d_off, &d_off_size);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a_data, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Write "b, 2"
  d_data[0] = 'b';
  a_data[0] = 2;

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Write c, 3; d, 4 and e, 5.
  d_data[0] = 'c';
  d_data[1] = 'd';
  d_data[2] = 'e';
  d_data_size = 3;
  d_off_size = 3 * sizeof(uint64_t);
  a_data[0] = 3;
  a_data[1] = 4;
  a_data[2] = 5;
  a_size = 3 * sizeof(int32_t);

  // Submit query
  rc = tiledb_query_submit_and_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Read [a, ee]
  std::vector<uint64_t> r_d_off(10);
  std::string r_d_val;
  r_d_val.resize(20);
  std::vector<int32_t> r_a(10);
  tiledb_query_status_t status;
  tiledb_query_status_details_t details;
  read_array_1d(
      ctx_,
      array,
      TILEDB_GLOBAL_ORDER,
      "a",
      "e",
      &r_d_off,
      &r_d_val,
      &r_a,
      &status,
      &details);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d_val == "abcde");
  std::vector<uint64_t> c_d_off = {0, 1, 2, 3, 4};
  CHECK(r_d_off == c_d_off);
  std::vector<int32_t> c_a = {1, 2, 3, 4, 5};
  CHECK(r_a == c_a);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    StringDimsFx,
    "C API: Test sparse array with string dimensions, 2d, default ranges",
    "[capi][sparse][string-dims][2d][default-ranges][rest]") {
  std::string array_name = prefix_ + "string_dims";

  // Create array
  int32_t dom[] = {1, 10};
  int32_t extent = 5;
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_STRING_ASCII, TILEDB_INT32},
      {nullptr, dom},
      {nullptr, &extent},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      false);

  // Write
  tiledb_layout_t write_layout = TILEDB_UNORDERED;
  SECTION("Unordered write") {
    write_layout = TILEDB_UNORDERED;
  }
  SECTION("Global write") {
    write_layout = TILEDB_GLOBAL_ORDER;
  }
  std::vector<uint64_t> d1_off = {0, 2, 4, 6};
  std::string d1_val("aabbccdddd");
  std::vector<int32_t> d2 = {1, 2, 3, 4};
  std::vector<int32_t> a = {11, 12, 13, 14};
  write_array_2d(ctx_, array_name, write_layout, d1_off, d1_val, d2, a);

  // ####### READ #######

  // Check non-empty domain
  std::string start, end;
  int32_t is_empty;
  get_non_empty_domain_var(array_name, "d1", &start, &end, &is_empty);
  CHECK(is_empty == 0);
  CHECK(start == "aa");
  CHECK(end == "dddd");
  std::vector<int32_t> non_empty;
  get_non_empty_domain(array_name, "d2", &non_empty, &is_empty);
  CHECK(is_empty == 0);
  CHECK(non_empty[0] == 1);
  CHECK(non_empty[1] == 4);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Set layout
  tiledb_layout_t layout = TILEDB_ROW_MAJOR;

  SECTION("Global order") {
    layout = TILEDB_GLOBAL_ORDER;
  }
  SECTION("Row-major read") {
    layout = TILEDB_ROW_MAJOR;
  }
  SECTION("Col-major") {
    layout = TILEDB_COL_MAJOR;
  }
  SECTION("Unordered") {
    layout = TILEDB_UNORDERED;
  }

  // Read [a, e], [1, 10]
  std::vector<uint64_t> r_d1_off(10);
  std::string r_d1_val;
  r_d1_val.resize(20);
  std::vector<int32_t> r_d2(10);
  std::vector<int32_t> r_a(10);
  tiledb_query_status_t status;
  read_array_2d_default_string_range(
      ctx_, array, layout, 1, 10, &r_d1_off, &r_d1_val, &r_d2, &r_a, &status);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d1_val == "aabbccdddd");
  std::vector<uint64_t> c_d1_off = {0, 2, 4, 6};
  CHECK(r_d1_off == c_d1_off);
  std::vector<int32_t> c_d2 = {1, 2, 3, 4};
  CHECK(r_d2 == c_d2);
  std::vector<int32_t> c_a = {11, 12, 13, 14};
  CHECK(r_a == c_a);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Write again
  d1_off = {0, 1, 2};
  d1_val = "abff";
  d2 = {2, 2, 3};
  a = {15, 16, 17};
  write_array_2d(ctx_, array_name, write_layout, d1_off, d1_val, d2, a);

  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Create config
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  rc = tiledb_config_alloc(&config, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  rc = tiledb_config_set(
      config, "sm.consolidation.mode", "fragment_meta", &error);
  CHECK(rc == TILEDB_OK);

  // Consolidate fragment metadata
  rc = tiledb_array_consolidate(ctx_, array_name.c_str(), config);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Read [a, ff], [1, 10]
  r_d1_off.resize(20);
  r_d1_val.resize(20);
  r_d2.resize(20);
  r_a.resize(20);
  read_array_2d_default_string_range(
      ctx_,
      array,
      TILEDB_GLOBAL_ORDER,
      1,
      10,
      &r_d1_off,
      &r_d1_val,
      &r_d2,
      &r_a,
      &status);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(r_d1_val == "aaabbbccddddff");
  c_d1_off = {0, 1, 3, 4, 6, 8, 12};
  CHECK(r_d1_off == c_d1_off);
  c_d2 = {2, 1, 2, 2, 3, 4, 3};
  CHECK(r_d2 == c_d2);
  c_a = {15, 11, 16, 12, 13, 14, 17};
  CHECK(r_a == c_a);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Clean up
  tiledb_config_free(&config);
}
