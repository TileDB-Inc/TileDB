/**
 * @file   unit-capi-sparse_heter.cc
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
 * Tests of C API for sparse arrays with heterogeneous domains.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/enums/serialization_type.h"

#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

typedef std::pair<std::string, uint64_t> EstSize;

struct SparseHeterFx {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  // Serialization parameters
  bool serialize_ = false;

  // Path to prepend to array name according to filesystem/mode
  std::string prefix_;

  // Functions
  SparseHeterFx();
  ~SparseHeterFx();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void write_sparse_array_float_int64(
      const std::string& array_name,
      tiledb_layout_t layout,
      const std::vector<float>& buff_d1,
      const std::vector<int64_t>& buff_d2,
      const std::vector<int32_t>& buff_a);
  void write_sparse_array_int64_float(
      const std::string& array_name,
      tiledb_layout_t layout,
      const std::vector<int64_t>& buff_d1,
      const std::vector<float>& buff_d2,
      const std::vector<int32_t>& buff_a);
  void check_non_empty_domain_float_int64(
      const std::string& path,
      const float* dom_f,
      const int64_t* dom_i,
      bool is_empty);
  void check_non_empty_domain_int64_float(
      const std::string& path,
      const int64_t* dom_i,
      const float* dom_f,
      bool is_empty);
  void check_est_result_size_float_int64(
      const std::string& array_name,
      const float* subarray_f,
      const int64_t* subarray_i,
      const std::vector<EstSize>& sizes);
  void check_est_result_size_int64_float(
      const std::string& array_name,
      const int64_t* subarray_i,
      const float* subarray_f,
      const std::vector<EstSize>& sizes);
  void check_read_sparse_array_float_int64(
      const std::string& array_name,
      const float* subarray_f,
      const int64_t* subarray_i,
      tiledb_layout_t layout,
      const std::vector<float>& buff_d1,
      const std::vector<int64_t>& buff_d2,
      const std::vector<int32_t>& buff_a);
  void check_read_sparse_array_int64_float(
      const std::string& array_name,
      const int64_t* subarray_i,
      const float* subarray_f,
      tiledb_layout_t layout,
      const std::vector<int64_t>& buff_d1,
      const std::vector<float>& buff_d2,
      const std::vector<int32_t>& buff_a);
  int tiledb_array_get_non_empty_domain_from_index_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      uint32_t index,
      void* domain,
      int32_t* is_empty);
  int tiledb_array_get_non_empty_domain_from_name_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_array_t* array,
      const char* name,
      void* domain,
      int32_t* is_empty);
  int32_t tiledb_query_get_est_result_size_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      const char* name,
      uint64_t* size);
  int32_t tiledb_query_get_est_result_size_var_wrapper(
      tiledb_ctx_t* ctx,
      tiledb_query_t* query,
      const char* name,
      uint64_t* size_off,
      uint64_t* size_val);
};

SparseHeterFx::SparseHeterFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
  auto temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  prefix_ = vfs_array_uri(fs_vec_[0], "sparse-heter-fx", ctx_);
}

SparseHeterFx::~SparseHeterFx() {
  remove_temp_dir(fs_vec_[0]->temp_dir());
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

int SparseHeterFx::tiledb_array_get_non_empty_domain_from_index_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t index,
    void* domain,
    int32_t* is_empty) {
  int ret = tiledb_array_get_non_empty_domain_from_index(
      ctx, array, index, domain, is_empty);
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
  return tiledb_array_get_non_empty_domain_from_index(
      ctx, array, index, domain, is_empty);
}

int SparseHeterFx::tiledb_array_get_non_empty_domain_from_name_wrapper(
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

int32_t SparseHeterFx::tiledb_query_get_est_result_size_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* size) {
  int ret = tiledb_query_get_est_result_size(ctx, query, name, size);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_)
    return ret;

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_query_est_result_sizes(
          ctx,
          query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_query_est_result_sizes(
          ctx,
          query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          buff) == TILEDB_OK);

  tiledb_buffer_free(&buff);
  return tiledb_query_get_est_result_size(ctx, query, name, size);
}

int32_t SparseHeterFx::tiledb_query_get_est_result_size_var_wrapper(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val) {
  int ret = tiledb_query_get_est_result_size_var(
      ctx, query, name, size_off, size_val);
#ifndef TILEDB_SERIALIZATION
  return ret;
#endif

  if (ret != TILEDB_OK || !serialize_)
    return ret;

  // Serialize the non_empty_domain
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_query_est_result_sizes(
          ctx,
          query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &buff) == TILEDB_OK);

  // Deserialize to validate we can round-trip
  REQUIRE(
      tiledb_deserialize_query_est_result_sizes(
          ctx,
          query,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          buff) == TILEDB_OK);

  tiledb_buffer_free(&buff);
  return tiledb_query_get_est_result_size_var(
      ctx, query, name, size_off, size_val);
}

void SparseHeterFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SparseHeterFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SparseHeterFx::check_non_empty_domain_float_int64(
    const std::string& path,
    const float* dom_f,
    const int64_t* dom_i,
    bool is_empty) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Get non-empty domain for each dimension
  float dom_f_r[2];
  int64_t dom_i_r[2];
  int is_empty_r;
  rc = tiledb_array_get_non_empty_domain_from_index_wrapper(
      ctx_, array, 0, dom_f_r, &is_empty_r);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty_r == (int)is_empty);
  rc = tiledb_array_get_non_empty_domain_from_name_wrapper(
      ctx_, array, "d2", dom_i_r, &is_empty_r);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty_r == (int)is_empty);
  if (!is_empty) {
    CHECK(!std::memcmp(dom_f, dom_f_r, sizeof(dom_f_r)));
    CHECK(!std::memcmp(dom_i, dom_i_r, sizeof(dom_i_r)));
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
}

void SparseHeterFx::check_non_empty_domain_int64_float(
    const std::string& path,
    const int64_t* dom_i,
    const float* dom_f,
    bool is_empty) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Get non-empty domain for each dimension
  float dom_f_r[2];
  int64_t dom_i_r[2];
  int32_t is_empty_r;
  rc = tiledb_array_get_non_empty_domain_from_name_wrapper(
      ctx_, array, "d1", dom_i_r, &is_empty_r);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty_r == (int)is_empty);
  rc = tiledb_array_get_non_empty_domain_from_index_wrapper(
      ctx_, array, 1, dom_f_r, &is_empty_r);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty_r == (int)is_empty);
  if (!is_empty) {
    CHECK(!std::memcmp(dom_f, dom_f_r, sizeof(dom_f_r)));
    CHECK(!std::memcmp(dom_i, dom_i_r, sizeof(dom_i_r)));
  }

  // Errors
  uint64_t start_size, end_size;
  rc = tiledb_array_get_non_empty_domain_var_size_from_index(
      ctx_, array, 0, &start_size, &end_size, &is_empty_r);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_array_get_non_empty_domain_var_size_from_name(
      ctx_, array, "d1", &start_size, &end_size, &is_empty_r);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
}

void SparseHeterFx::check_est_result_size_float_int64(
    const std::string& array_name,
    const float* subarray_f,
    const int64_t* subarray_i,
    const std::vector<EstSize>& sizes) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 0, &subarray_f[0], &subarray_f[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 1, &subarray_i[0], &subarray_i[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Check error for zipped coordinates
  uint64_t size_r;
  rc = tiledb_query_get_est_result_size_wrapper(
      ctx_, query, array_name.c_str(), &size_r);
  CHECK(rc == TILEDB_ERR);

  // Check sizes
  for (const auto& s : sizes) {
    rc = tiledb_query_get_est_result_size_wrapper(
        ctx_, query, s.first.c_str(), &size_r);
    CHECK(rc == TILEDB_OK);
    CHECK(size_r == s.second);
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

void SparseHeterFx::check_est_result_size_int64_float(
    const std::string& array_name,
    const int64_t* subarray_i,
    const float* subarray_f,
    const std::vector<EstSize>& sizes) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 0, &subarray_i[0], &subarray_i[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 1, &subarray_f[0], &subarray_f[1], nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Check error for zipped coordinates
  uint64_t size_r;
  rc = tiledb_query_get_est_result_size_wrapper(
      ctx_, query, array_name.c_str(), &size_r);
  CHECK(rc == TILEDB_ERR);

  // Check sizes
  for (const auto& s : sizes) {
    rc = tiledb_query_get_est_result_size_wrapper(
        ctx_, query, s.first.c_str(), &size_r);
    CHECK(rc == TILEDB_OK);
    CHECK(size_r == s.second);
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

void SparseHeterFx::write_sparse_array_float_int64(
    const std::string& array_name,
    tiledb_layout_t layout,
    const std::vector<float>& buff_d1,
    const std::vector<int64_t>& buff_d2,
    const std::vector<int32_t>& buff_a) {
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
  uint64_t d1_size = buff_d1.size() * sizeof(float);
  uint64_t d2_size = buff_d2.size() * sizeof(int64_t);
  uint64_t a_size = buff_a.size() * sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", (void*)buff_d1.data(), &d1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", (void*)buff_d2.data(), &d2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", (void*)buff_a.data(), &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  if (layout == TILEDB_GLOBAL_ORDER) {
    rc = tiledb_query_submit_and_finalize(ctx_, query);
  } else {
    rc = tiledb_query_submit(ctx_, query);
  }
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseHeterFx::write_sparse_array_int64_float(
    const std::string& array_name,
    tiledb_layout_t layout,
    const std::vector<int64_t>& buff_d1,
    const std::vector<float>& buff_d2,
    const std::vector<int32_t>& buff_a) {
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
  uint64_t d1_size = buff_d1.size() * sizeof(int64_t);
  uint64_t d2_size = buff_d2.size() * sizeof(float);
  uint64_t a_size = buff_a.size() * sizeof(int32_t);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", (void*)buff_d1.data(), &d1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", (void*)buff_d2.data(), &d2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", (void*)buff_a.data(), &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, layout);
  REQUIRE(rc == TILEDB_OK);

  if (layout == TILEDB_GLOBAL_ORDER) {
    rc = tiledb_query_submit_and_finalize(ctx_, query);
  } else {
    rc = tiledb_query_submit(ctx_, query);
  }
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseHeterFx::check_read_sparse_array_float_int64(
    const std::string& array_name,
    const float* subarray_f,
    const int64_t* subarray_i,
    tiledb_layout_t layout,
    const std::vector<float>& buff_d1,
    const std::vector<int64_t>& buff_d2,
    const std::vector<int32_t>& buff_a) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  float buff_d1_r[40];
  int64_t buff_d2_r[40];
  int32_t buff_a_r[40];
  uint64_t d1_size = sizeof(buff_d1_r);
  uint64_t d2_size = sizeof(buff_d2_r);
  uint64_t a_size = sizeof(buff_a_r);

  // Create and submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d1", buff_d1_r, &d1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d2", buff_d2_r, &d2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", buff_a_r, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, layout);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 0, &subarray_f[0], &subarray_f[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 1, &subarray_i[0], &subarray_i[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Check correctness
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(d1_size == buff_d1.size() * sizeof(float));
  CHECK(d2_size == buff_d2.size() * sizeof(int64_t));
  CHECK(a_size == buff_a.size() * sizeof(int32_t));
  CHECK(!std::memcmp(&buff_d1[0], &buff_d1_r[0], d1_size));
  CHECK(!std::memcmp(&buff_d2[0], &buff_d2_r[0], d2_size));
  CHECK(!std::memcmp(&buff_a[0], &buff_a_r[0], a_size));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

void SparseHeterFx::check_read_sparse_array_int64_float(
    const std::string& array_name,
    const int64_t* subarray_i,
    const float* subarray_f,
    tiledb_layout_t layout,
    const std::vector<int64_t>& buff_d1,
    const std::vector<float>& buff_d2,
    const std::vector<int32_t>& buff_a) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int64_t buff_d1_r[40];
  float buff_d2_r[40];
  int32_t buff_a_r[40];
  uint64_t d1_size = sizeof(buff_d1_r);
  uint64_t d2_size = sizeof(buff_d2_r);
  uint64_t a_size = sizeof(buff_a_r);

  // Create and submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d1", buff_d1_r, &d1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d2", buff_d2_r, &d2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", buff_a_r, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, layout);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 0, &subarray_i[0], &subarray_i[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(
      ctx_, subarray, 1, &subarray_f[0], &subarray_f[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Check correctness
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);
  CHECK(d1_size == buff_d1.size() * sizeof(int64_t));
  CHECK(d2_size == buff_d2.size() * sizeof(float));
  CHECK(a_size == buff_a.size() * sizeof(int32_t));
  CHECK(!std::memcmp(&buff_d1[0], &buff_d1_r[0], d1_size));
  CHECK(!std::memcmp(&buff_d2[0], &buff_d2_r[0], d2_size));
  CHECK(!std::memcmp(&buff_a[0], &buff_a_r[0], a_size));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseHeterFx,
    "C API: Test sparse array with heterogeneous domains (float, int64)",
    "[capi][sparse][heter][float-int64][non-rest]") {
  std::string array_name = prefix_ + "sparse_array_heter";

  // Create array
  float dom_f[] = {1.0f, 20.0f};
  float extent_f = 5.0f;
  int64_t dom_i[] = {1, 30};
  int64_t extent_i = 5;
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_FLOAT32, TILEDB_INT64},
      {dom_f, dom_i},
      {&extent_f, &extent_i},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      serialize_);

  // Get non-empty domain when array is empty
  std::vector<float> c_dom_f = {1.f, 2.f};
  std::vector<int64_t> c_dom_i = {1, 2};
  check_non_empty_domain_float_int64(
      array_name, &c_dom_f[0], &c_dom_i[0], true);

  // ####### FIRST WRITE #######

  // Write in global order
  std::vector<float> buff_d1 = {1.1f, 1.2f, 1.3f, 1.4f};
  std::vector<int64_t> buff_d2 = {1, 2, 3, 4};
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  write_sparse_array_float_int64(
      array_name, TILEDB_GLOBAL_ORDER, buff_d1, buff_d2, buff_a);

  // Check non-empty domain
  c_dom_f = {1.1f, 1.4f};
  c_dom_i = {1, 4};
  check_non_empty_domain_float_int64(
      array_name, &c_dom_f[0], &c_dom_i[0], false);

  std::vector<EstSize> sizes = {
      EstSize("d1", 8), EstSize("d2", 16), EstSize("a", 8)};
  std::vector<float> subarray_f = {1.1f, 1.4f};
  std::vector<int64_t> subarray_i = {1, 2};
  check_est_result_size_float_int64(
      array_name, subarray_f.data(), subarray_i.data(), sizes);
  sizes = {EstSize("d1", 16), EstSize("d2", 32), EstSize("a", 16)};
  subarray_f = {1.1f, 1.4f};
  subarray_i = {1, 4};
  check_est_result_size_float_int64(
      array_name, subarray_f.data(), subarray_i.data(), sizes);

  // Read in global order
  std::vector<float> buff_d1_r = {1.1f, 1.2f, 1.3f, 1.4f};
  std::vector<int64_t> buff_d2_r = {1, 2, 3, 4};
  std::vector<int32_t> buff_a_r = {1, 2, 3, 4};
  subarray_f = {1.1f, 1.4f};
  subarray_i = {1, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_f = {1.3f, 1.4f};
  subarray_i = {3, 4};
  buff_d1_r = {1.3f, 1.4f};
  buff_d2_r = {3, 4};
  buff_a_r = {3, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in row-major order
  buff_d1_r = {1.1f, 1.2f, 1.3f, 1.4f};
  buff_d2_r = {1, 2, 3, 4};
  buff_a_r = {1, 2, 3, 4};
  subarray_f = {1.1f, 1.4f};
  subarray_i = {1, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_f = {1.3f, 1.4f};
  subarray_i = {3, 4};
  buff_d1_r = {1.3f, 1.4f};
  buff_d2_r = {3, 4};
  buff_a_r = {3, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in col-major order
  buff_d1_r = {1.1f, 1.2f, 1.3f, 1.4f};
  buff_d2_r = {1, 2, 3, 4};
  buff_a_r = {1, 2, 3, 4};
  subarray_f = {1.1f, 1.4f};
  subarray_i = {1, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_f = {1.3f, 1.4f};
  subarray_i = {3, 4};
  buff_d1_r = {1.3f, 1.4f};
  buff_d2_r = {3, 4};
  buff_a_r = {3, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // ####### SECOND WRITE #######

  // Write unordered
  buff_d1 = {1.2f, 1.5f};
  buff_d2 = {6, 3};
  buff_a = {6, 7};
  write_sparse_array_float_int64(
      array_name, TILEDB_UNORDERED, buff_d1, buff_d2, buff_a);

  // Check non-empty domain
  c_dom_f = {1.1f, 1.5f};
  c_dom_i = {1, 6};
  check_non_empty_domain_float_int64(
      array_name, &c_dom_f[0], &c_dom_i[0], false);

  // Read in global order
  buff_d1_r = {1.1f, 1.2f, 1.3f, 1.4f, 1.5f, 1.2f};
  buff_d2_r = {1, 2, 3, 4, 3, 6};
  buff_a_r = {1, 2, 3, 4, 7, 6};
  subarray_f = {1.1f, 1.5f};
  subarray_i = {1, 10};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_f = {1.3f, 1.4f};
  subarray_i = {3, 4};
  buff_d1_r = {1.3f, 1.4f};
  buff_d2_r = {3, 4};
  buff_a_r = {3, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in row-major
  buff_d1_r = {1.1f, 1.2f, 1.2f, 1.3f, 1.4f, 1.5f};
  buff_d2_r = {1, 2, 6, 3, 4, 3};
  buff_a_r = {1, 2, 6, 3, 4, 7};
  subarray_f = {1.1f, 1.5f};
  subarray_i = {1, 10};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_f = {1.3f, 1.4f};
  subarray_i = {3, 4};
  buff_d1_r = {1.3f, 1.4f};
  buff_d2_r = {3, 4};
  buff_a_r = {3, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in col-major
  buff_d1_r = {1.1f, 1.2f, 1.3f, 1.5f, 1.4f, 1.2f};
  buff_d2_r = {1, 2, 3, 3, 4, 6};
  buff_a_r = {1, 2, 3, 7, 4, 6};
  subarray_f = {1.1f, 1.5f};
  subarray_i = {1, 10};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_f = {1.3f, 1.4f};
  subarray_i = {3, 4};
  buff_d1_r = {1.3f, 1.4f};
  buff_d2_r = {3, 4};
  buff_a_r = {3, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // ####### CONSOLIDATE #######

  // Consolidate fragments
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  auto rc = tiledb_config_set(
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

  rc = tiledb_array_consolidate(ctx_, array_name.c_str(), config);
  CHECK(rc == TILEDB_OK);

  tiledb_config_free(&config);

  // Check non-empty domain
  c_dom_f = {1.1f, 1.5f};
  c_dom_i = {1, 6};
  check_non_empty_domain_float_int64(
      array_name, &c_dom_f[0], &c_dom_i[0], false);

  // Read in global order
  buff_d1_r = {1.1f, 1.2f, 1.3f, 1.4f, 1.5f, 1.2f};
  buff_d2_r = {1, 2, 3, 4, 3, 6};
  buff_a_r = {1, 2, 3, 4, 7, 6};
  subarray_f = {1.1f, 1.5f};
  subarray_i = {1, 10};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_f = {1.3f, 1.4f};
  subarray_i = {3, 4};
  buff_d1_r = {1.3f, 1.4f};
  buff_d2_r = {3, 4};
  buff_a_r = {3, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in row-major
  buff_d1_r = {1.1f, 1.2f, 1.2f, 1.3f, 1.4f, 1.5f};
  buff_d2_r = {1, 2, 6, 3, 4, 3};
  buff_a_r = {1, 2, 6, 3, 4, 7};
  subarray_f = {1.1f, 1.5f};
  subarray_i = {1, 10};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_f = {1.3f, 1.4f};
  subarray_i = {3, 4};
  buff_d1_r = {1.3f, 1.4f};
  buff_d2_r = {3, 4};
  buff_a_r = {3, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in col-major
  buff_d1_r = {1.1f, 1.2f, 1.3f, 1.5f, 1.4f, 1.2f};
  buff_d2_r = {1, 2, 3, 3, 4, 6};
  buff_a_r = {1, 2, 3, 7, 4, 6};
  subarray_f = {1.1f, 1.5f};
  subarray_i = {1, 10};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_f = {1.3f, 1.4f};
  subarray_i = {3, 4};
  buff_d1_r = {1.3f, 1.4f};
  buff_d2_r = {3, 4};
  buff_a_r = {3, 4};
  check_read_sparse_array_float_int64(
      array_name,
      &subarray_f[0],
      &subarray_i[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
}

TEST_CASE_METHOD(
    SparseHeterFx,
    "C API: Test sparse array with heterogeneous domains (int64, float)",
    "[capi][sparse][heter][int64-float][non-rest]") {
  std::string array_name = prefix_ + "sparse_array_heter";

  // Create array
  float dom_f[] = {1.0f, 20.0f};
  float extent_f = 5.0f;
  int64_t dom_i[] = {1, 30};
  int64_t extent_i = 5;
  create_array(
      ctx_,
      array_name,
      TILEDB_SPARSE,
      {"d1", "d2"},
      {TILEDB_INT64, TILEDB_FLOAT32},
      {dom_i, dom_f},
      {&extent_i, &extent_f},
      {"a"},
      {TILEDB_INT32},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2,
      false,
      serialize_);

  // Get non-empty domain when array is empty
  std::vector<float> c_dom_f = {1.f, 2.f};
  std::vector<int64_t> c_dom_i = {1, 2};
  check_non_empty_domain_int64_float(
      array_name, &c_dom_i[0], &c_dom_f[0], true);

  // ####### FIRST WRITE #######

  // Write in global order
  std::vector<int64_t> buff_d1 = {1, 2, 3, 4};
  std::vector<float> buff_d2 = {1.1f, 1.2f, 1.3f, 1.4f};
  std::vector<int32_t> buff_a = {1, 2, 3, 4};
  write_sparse_array_int64_float(
      array_name, TILEDB_GLOBAL_ORDER, buff_d1, buff_d2, buff_a);

  // Check non-empty domain
  c_dom_f = {1.1f, 1.4f};
  c_dom_i = {1, 4};
  check_non_empty_domain_int64_float(
      array_name, &c_dom_i[0], &c_dom_f[0], false);

  std::vector<EstSize> sizes = {
      EstSize("d1", 16), EstSize("d2", 8), EstSize("a", 8)};
  std::vector<float> subarray_f = {1.1f, 1.4f};
  std::vector<int64_t> subarray_i = {1, 2};
  check_est_result_size_int64_float(
      array_name, subarray_i.data(), subarray_f.data(), sizes);
  sizes = {EstSize("d1", 32), EstSize("d2", 16), EstSize("a", 16)};
  subarray_f = {1.1f, 1.4f};
  subarray_i = {1, 4};
  check_est_result_size_int64_float(
      array_name, subarray_i.data(), subarray_f.data(), sizes);

  // Read in global order
  std::vector<int64_t> buff_d1_r = {1, 2, 3, 4};
  std::vector<float> buff_d2_r = {1.1f, 1.2f, 1.3f, 1.4f};
  std::vector<int32_t> buff_a_r = {1, 2, 3, 4};
  subarray_i = {1, 4};
  subarray_f = {1.1f, 1.4f};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_f = {1.3f, 1.4f};
  subarray_i = {3, 4};
  buff_d1_r = {3, 4};
  buff_d2_r = {1.3f, 1.4f};
  buff_a_r = {3, 4};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in row-major order
  buff_d1_r = {1, 2, 3, 4};
  buff_d2_r = {1.1f, 1.2f, 1.3f, 1.4f};
  buff_a_r = {1, 2, 3, 4};
  subarray_f = {1.1f, 1.4f};
  subarray_i = {1, 4};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_i = {3, 4};
  subarray_f = {1.3f, 1.4f};
  buff_d1_r = {3, 4};
  buff_d2_r = {1.3f, 1.4f};
  buff_a_r = {3, 4};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in col-major order
  buff_d1_r = {1, 2, 3, 4};
  buff_d2_r = {1.1f, 1.2f, 1.3f, 1.4f};
  buff_a_r = {1, 2, 3, 4};
  subarray_i = {1, 4};
  subarray_f = {1.1f, 1.4f};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_i = {3, 4};
  subarray_f = {1.3f, 1.4f};
  buff_d1_r = {3, 4};
  buff_d2_r = {1.3f, 1.4f};
  buff_a_r = {3, 4};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // ####### SECOND WRITE #######

  // Write unordered
  buff_d1 = {6, 3};
  buff_d2 = {1.2f, 1.5f};
  buff_a = {6, 7};
  write_sparse_array_int64_float(
      array_name, TILEDB_UNORDERED, buff_d1, buff_d2, buff_a);

  // Check non-empty domain
  c_dom_i = {1, 6};
  c_dom_f = {1.1f, 1.5f};
  check_non_empty_domain_int64_float(
      array_name, &c_dom_i[0], &c_dom_f[0], false);

  // Read in global order
  buff_d1_r = {1, 2, 3, 3, 4, 6};
  buff_d2_r = {1.1f, 1.2f, 1.3f, 1.5f, 1.4f, 1.2f};
  buff_a_r = {1, 2, 3, 7, 4, 6};
  subarray_i = {1, 10};
  subarray_f = {1.1f, 1.5f};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_i = {3, 4};
  subarray_f = {1.3f, 1.4f};
  buff_d1_r = {3, 4};
  buff_d2_r = {1.3f, 1.4f};
  buff_a_r = {3, 4};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in row-major
  buff_d1_r = {1, 2, 3, 3, 4, 6};
  buff_d2_r = {1.1f, 1.2f, 1.3f, 1.5f, 1.4f, 1.2f};
  buff_a_r = {1, 2, 3, 7, 4, 6};
  subarray_i = {1, 10};
  subarray_f = {1.1f, 1.5f};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_i = {3, 4};
  subarray_f = {1.3f, 1.4f};
  buff_d1_r = {3, 4};
  buff_d2_r = {1.3f, 1.4f};
  buff_a_r = {3, 4};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in col-major
  buff_d1_r = {1, 2, 6, 3, 4, 3};
  buff_d2_r = {1.1f, 1.2f, 1.2f, 1.3f, 1.4f, 1.5f};
  buff_a_r = {1, 2, 6, 3, 4, 7};
  subarray_f = {1.1f, 1.5f};
  subarray_i = {1, 10};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_i = {3, 4};
  subarray_f = {1.3f, 1.4f};
  buff_d1_r = {3, 4};
  buff_d2_r = {1.3f, 1.4f};
  buff_a_r = {3, 4};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // ####### CONSOLIDATE #######

  // Consolidate fragments
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  auto rc = tiledb_config_set(
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

  rc = tiledb_array_consolidate(ctx_, array_name.c_str(), config);
  CHECK(rc == TILEDB_OK);

  tiledb_config_free(&config);

  // Check non-empty domain
  c_dom_i = {1, 6};
  c_dom_f = {1.1f, 1.5f};
  check_non_empty_domain_int64_float(
      array_name, &c_dom_i[0], &c_dom_f[0], false);

  // Read in global order
  buff_d1_r = {1, 2, 3, 3, 4, 6};
  buff_d2_r = {1.1f, 1.2f, 1.3f, 1.5f, 1.4f, 1.2f};
  buff_a_r = {1, 2, 3, 7, 4, 6};
  subarray_i = {1, 10};
  subarray_f = {1.1f, 1.5f};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_i = {3, 4};
  subarray_f = {1.3f, 1.4f};
  buff_d1_r = {3, 4};
  buff_d2_r = {1.3f, 1.4f};
  buff_a_r = {3, 4};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_GLOBAL_ORDER,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in row-major
  buff_d1_r = {1, 2, 3, 3, 4, 6};
  buff_d2_r = {1.1f, 1.2f, 1.3f, 1.5f, 1.4f, 1.2f};
  buff_a_r = {1, 2, 3, 7, 4, 6};
  subarray_i = {1, 10};
  subarray_f = {1.1f, 1.5f};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_i = {3, 4};
  subarray_f = {1.3f, 1.4f};
  buff_d1_r = {3, 4};
  buff_d2_r = {1.3f, 1.4f};
  buff_a_r = {3, 4};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_ROW_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);

  // Read in col-major
  buff_d1_r = {1, 2, 6, 3, 4, 3};
  buff_d2_r = {1.1f, 1.2f, 1.2f, 1.3f, 1.4f, 1.5f};
  buff_a_r = {1, 2, 6, 3, 4, 7};
  subarray_f = {1.1f, 1.5f};
  subarray_i = {1, 10};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
  subarray_i = {3, 4};
  subarray_f = {1.3f, 1.4f};
  buff_d1_r = {3, 4};
  buff_d2_r = {1.3f, 1.4f};
  buff_a_r = {3, 4};
  check_read_sparse_array_int64_float(
      array_name,
      &subarray_i[0],
      &subarray_f[0],
      TILEDB_COL_MAJOR,
      buff_d1_r,
      buff_d2_r,
      buff_a_r);
}
