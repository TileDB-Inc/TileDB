/**
 * @file   unit-capi-sparse_array.cc
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
 * Tests of C API for sparse array operations.
 */

#ifdef TILEDB_SERIALIZATION
#include <capnp/message.h>
#include "tiledb/sm/serialization/array_directory.h"
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include <test/support/tdb_catch.h>
#include "test/support/src/error_helpers.h"
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"

#include "test/support/src/helpers.h"

#include <cassert>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>

using namespace tiledb::test;

using Asserter = tiledb::test::AsserterCatch;

const uint64_t DIM_DOMAIN[4] = {1, 4, 1, 4};

struct SparseArrayFx {
  // Constant parameters
  std::string ATTR_NAME = "a";
  const char* DIM1_NAME = "x";
  const char* DIM2_NAME = "y";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const tiledb_array_type_t ARRAY_TYPE = TILEDB_SPARSE;
  int COMPRESSION_LEVEL = -1;
  int ITER_NUM = 5;
  const std::string ARRAY = "sparse_array";

  tiledb_encryption_type_t encryption_type = TILEDB_NO_ENCRYPTION;
  const char* encryption_key = nullptr;

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  // Path to prepend to array name according to filesystem/mode
  std::string prefix_;

  // Functions
  SparseArrayFx();
  ~SparseArrayFx();

  void create_sparse_array(
      const std::string& array_name,
      tiledb_layout_t layout = TILEDB_ROW_MAJOR,
      const uint64_t* dim_domain = DIM_DOMAIN);
  void check_sparse_array_unordered_with_duplicates_error(
      const std::string& array_name);
  void check_sparse_array_unordered_with_duplicates_no_check(
      const std::string& array_name);
  void check_sparse_array_unordered_with_duplicates_dedup(
      const std::string& array_name);
  void check_sparse_array_unordered_with_all_duplicates_dedup(
      const std::string& array_name);
  void check_sparse_array_global_with_duplicates_error(
      const std::string& array_name);
  void check_sparse_array_global_with_duplicates_no_check(
      const std::string& array_name);
  void check_sparse_array_global_with_duplicates_dedup(
      const std::string& array_name);
  void check_sparse_array_global_with_all_duplicates_dedup(
      const std::string& array_name);
  void check_sparse_array_no_results(const std::string& array_name);
  void check_non_empty_domain(const std::string& array_name);
  void check_invalid_offsets(const std::string& array_name);
  void write_partial_sparse_array(const std::string& array_name);
  void write_sparse_array_missing_attributes(const std::string& array_name);
  void write_sparse_array(const std::string& array_name);
  void write_sparse_array(
      const std::string& array_name,
      const std::vector<uint64_t>& coords_dim1,
      const std::vector<uint64_t>& coords_dim2,
      const std::vector<int>& a1,
      const std::vector<uint64_t>& a2_off,
      const std::vector<char>& a2_val,
      const std::vector<float>& a3);
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void check_sorted_reads(
      const std::string& array_name,
      tiledb_filter_type_t compressor,
      tiledb_layout_t tile_order,
      tiledb_layout_t cell_order);
  bool is_array(const std::string& array_name);

  /**
   * Creates a 2D sparse array.
   *
   * @param array_name The array name.
   * @param tile_extent_0 The tile extent along the first dimension.
   * @param tile_extent_1 The tile extent along the second dimension.
   * @param domain_0_lo The smallest value of the first dimension domain.
   * @param domain_0_hi The largest value of the first dimension domain.
   * @param domain_1_lo The smallest value of the second dimension domain.
   * @param domain_1_hi The largest value of the second dimension domain.
   * @param capacity The tile capacity.
   * @param cell_order The cell order.
   * @param tile_order The tile order.
   */
  void create_sparse_array_2D(
      const std::string& array_name,
      const int64_t tile_extent_0,
      const int64_t tile_extent_1,
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const uint64_t capacity,
      const tiledb_filter_type_t compressor,
      const tiledb_layout_t cell_order,
      const tiledb_layout_t tile_order);

  /**
   * Reads a subarray oriented by the input boundaries and outputs the buffer
   * containing the attribute values of the corresponding cells.
   *
   * @param array_name The array name.
   * @param domain_0_lo The smallest value of the first dimension domain to
   *     be read.
   * @param domain_0_hi The largest value of the first dimension domain to
   *     be read.
   * @param domain_1_lo The smallest value of the second dimension domain to
   *     be read.
   * @param domain_1_hi The largest value of the second dimension domain to
   *     be read.
   * @param read_mode The read mode.
   * @return The buffer with the read values. Note that this function is
   *     creating the buffer with 'new'. Therefore, make sure to properly delete
   *     it in the caller. On error, it returns NULL.
   */
  int* read_sparse_array_2D(
      const std::string& array_name,
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const tiledb_query_type_t query_type,
      const tiledb_layout_t query_layout);

  /**
   * Write random values in unsorted mode. The buffer is initialized with each
   * cell being equalt to row_id*domain_size_1+col_id.
   *
   * @param array_name The array name.
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   */
  void write_sparse_array_unsorted_2D(
      const std::string& array_name,
      const int64_t domain_size_0,
      const int64_t domain_size_1);

  void test_random_subarrays(
      const std::string& array_name,
      int64_t domain_size_0,
      int64_t domain_size_1,
      int iter_num);
};

SparseArrayFx::SparseArrayFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
  std::srand(0);

  for (const auto& fs : fs_vec_)
    create_temp_dir(fs->temp_dir());

  prefix_ = vfs_array_uri(fs_vec_[0], fs_vec_[0]->temp_dir());
}

SparseArrayFx::~SparseArrayFx() {
  for (const auto& fs : fs_vec_)
    remove_temp_dir(fs->temp_dir());

  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void SparseArrayFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SparseArrayFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SparseArrayFx::create_sparse_array_2D(
    const std::string& array_name,
    const int64_t tile_extent_0,
    const int64_t tile_extent_1,
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const uint64_t capacity,
    const tiledb_filter_type_t compressor,
    const tiledb_layout_t cell_order,
    const tiledb_layout_t tile_order) {
  // Prepare and set the array schema object and data structures
  int64_t dim_domain[] = {domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};

  // Create attribute
  tiledb_attribute_t* a;
  int rc = tiledb_attribute_alloc(ctx_, ATTR_NAME.c_str(), ATTR_TYPE, &a);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_t* filter;
  tiledb_filter_list_t* list;
  rc = tiledb_filter_alloc(ctx_, compressor, &filter);
  CHECK(rc == TILEDB_OK);
  if (compressor != TILEDB_FILTER_NONE) {
    rc = tiledb_filter_set_option(
        ctx_, filter, TILEDB_COMPRESSION_LEVEL, &COMPRESSION_LEVEL);
    CHECK(rc == TILEDB_OK);
  }
  rc = tiledb_filter_list_alloc(ctx_, &list);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, list, filter);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_filter_list(ctx_, a, list);
  CHECK(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx_, DIM1_NAME, TILEDB_INT64, &dim_domain[0], &tile_extent_0, &d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, DIM2_NAME, TILEDB_INT64, &dim_domain[2], &tile_extent_1, &d2);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, ARRAY_TYPE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, capacity);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, tile_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Create the array
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_ctx_free(&ctx_);
    tiledb_vfs_free(&vfs_);
    tiledb_config_t* config;
    tiledb_error_t* error = nullptr;
    rc = tiledb_config_alloc(&config, &error);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(error == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    rc = tiledb_config_set(
        config, "sm.encryption_type", encryption_type_string.c_str(), &error);
    REQUIRE(error == nullptr);
    rc = tiledb_config_set(config, "sm.encryption_key", encryption_key, &error);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(error == nullptr);
    REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok());
    tiledb_config_free(&config);
  }
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&list);
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

int* SparseArrayFx::read_sparse_array_2D(
    const std::string& array_name,
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const tiledb_query_type_t query_type,
    const tiledb_layout_t query_layout) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_open(ctx_, array, query_type);
  CHECK(rc == TILEDB_OK);

  // Prepare the buffers that will store the result
  uint64_t buffer_size = 100 * 1024 * 1024;
  auto buffer = new int[buffer_size / sizeof(int)];
  REQUIRE(buffer != nullptr);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, query_type, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, ATTR_NAME.c_str(), buffer, &buffer_size);
  REQUIRE(rc == TILEDB_OK);

  // Set a subarray
  int64_t s0[] = {domain_0_lo, domain_0_hi};
  int64_t s1[] = {domain_1_lo, domain_1_hi};
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);

  // Success - return the created buffer
  return buffer;
}

void SparseArrayFx::write_sparse_array(const std::string& array_name) {
  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t buffer_coords_dim1[] = {1, 1, 1, 2, 3, 4, 3, 3};
  uint64_t buffer_coords_dim2[] = {1, 2, 4, 3, 1, 2, 3, 4};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_var_a2,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
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

void SparseArrayFx::write_sparse_array(
    const std::string& array_name,
    const std::vector<uint64_t>& coords_dim1,
    const std::vector<uint64_t>& coords_dim2,
    const std::vector<int>& a1,
    const std::vector<uint64_t>& a2_off,
    const std::vector<char>& a2_val,
    const std::vector<float>& a3) {
  const void* buffers[] = {
      a1.data(),
      a2_off.data(),
      a2_val.data(),
      a3.data(),
      coords_dim1.data(),
      coords_dim2.data()};
  uint64_t buffer_sizes[] = {
      a1.size() * sizeof(int),
      a2_off.size() * sizeof(uint64_t),
      a2_val.size() * sizeof(char),
      a3.size() * sizeof(float),
      coords_dim1.size() * sizeof(uint64_t),
      coords_dim2.size() * sizeof(uint64_t)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], (void*)buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], (void*)buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], (void*)buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], (void*)buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], (void*)buffers[5], &buffer_sizes[5]);
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

void SparseArrayFx::write_sparse_array_unsorted_2D(
    const std::string& array_name,
    const int64_t domain_size_0,
    const int64_t domain_size_1) {
  // Generate random attribute values and coordinates for sparse write
  int64_t cell_num = domain_size_0 * domain_size_1;
  auto buffer_a1 = new int[cell_num];
  auto buffer_coords_dim1 = new int64_t[cell_num];
  auto buffer_coords_dim2 = new int64_t[cell_num];
  int64_t coords_index = 0L;
  for (int64_t i = 0; i < domain_size_0; ++i) {
    for (int64_t j = 0; j < domain_size_1; ++j) {
      buffer_a1[i * domain_size_1 + j] = i * domain_size_1 + j;
      buffer_coords_dim1[coords_index] = i;
      buffer_coords_dim2[coords_index] = j;
      coords_index++;
    }
  }

  // Prepare buffers
  void* buffers[] = {buffer_a1, buffer_coords_dim1, buffer_coords_dim2};
  uint64_t buffer_sizes[3];
  buffer_sizes[0] = cell_num * sizeof(int);
  buffer_sizes[1] = cell_num * sizeof(int64_t);
  buffer_sizes[2] = cell_num * sizeof(int64_t);

  // Set attributes
  const char* attributes[] = {ATTR_NAME.c_str(), DIM1_NAME, DIM2_NAME};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[1], &buffer_sizes[1]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[2], &buffer_sizes[2]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Clean up
  delete[] buffer_a1;
  delete[] buffer_coords_dim1;
  delete[] buffer_coords_dim2;
}

void SparseArrayFx::test_random_subarrays(
    const std::string& array_name,
    int64_t domain_size_0,
    int64_t domain_size_1,
    int iter_num) {
  // write array_schema_latest cells with value = row id * columns + col id to
  // disk
  write_sparse_array_unsorted_2D(array_name, domain_size_0, domain_size_1);

  // test random subarrays and check with corresponding value set by
  // row_id*dim1+col_id. top left corner is always 4,4.
  int64_t d0_lo = 4;
  int64_t d0_hi = 0;
  int64_t d1_lo = 4;
  int64_t d1_hi = 0;
  int64_t height = 0, width = 0;

  for (int iter = 0; iter < iter_num; ++iter) {
    height = rand() % (domain_size_0 - d0_lo);
    width = rand() % (domain_size_1 - d1_lo);
    d0_hi = d0_lo + height;
    d1_hi = d1_lo + width;
    int64_t index = 0;

    // Read subarray
    int* buffer = read_sparse_array_2D(
        array_name, d0_lo, d0_hi, d1_lo, d1_hi, TILEDB_READ, TILEDB_ROW_MAJOR);
    CHECK(buffer != nullptr);

    // check
    bool allok = true;
    for (int64_t i = d0_lo; i <= d0_hi; ++i) {
      for (int64_t j = d1_lo; j <= d1_hi; ++j) {
        bool match = buffer[index] == i * domain_size_1 + j;
        if (!match) {
          allok = false;
          std::cout << "mismatch: " << i << "," << j << "=" << buffer[index]
                    << "!=" << ((i * domain_size_1 + j)) << "\n";
          break;
        }
        ++index;
      }
      if (!allok)
        break;
    }
    // clean up
    delete[] buffer;
    CHECK(allok);
  }
}

void SparseArrayFx::check_sorted_reads(
    const std::string& array_name,
    tiledb_filter_type_t compressor,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order) {
  // Parameters used in this test
  int64_t domain_size_0 = 2500;
  int64_t domain_size_1 = 500;
  int64_t tile_extent_0 = 50;
  int64_t tile_extent_1 = 50;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  int64_t capacity = 25000;
  int iter_num = (compressor != TILEDB_FILTER_BZIP2) ? ITER_NUM : 1;

  create_sparse_array_2D(
      array_name,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      compressor,
      tile_order,
      cell_order);
  test_random_subarrays(array_name, domain_size_0, domain_size_1, iter_num);
}

void SparseArrayFx::create_sparse_array(
    const std::string& array_name,
    tiledb_layout_t layout,
    const uint64_t* dim_domain) {
  // Create dimensions
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Set up filter list
  tiledb_filter_t* filter;
  rc = tiledb_filter_alloc(ctx_, TILEDB_FILTER_BZIP2, &filter);
  REQUIRE(rc == TILEDB_OK);
  int level = 5;
  rc = tiledb_filter_set_option(ctx_, filter, TILEDB_COMPRESSION_LEVEL, &level);
  REQUIRE(rc == TILEDB_OK);
  tiledb_filter_list_t* filter_list;
  rc = tiledb_filter_list_alloc(ctx_, &filter_list);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, filter_list, filter);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_set_filter_list(ctx_, d1, filter_list);
  REQUIRE(rc == TILEDB_OK);

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
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_CHAR, &a2);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a2, TILEDB_FILTER_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  tiledb_attribute_t* a3;
  rc = tiledb_attribute_alloc(ctx_, "a3", TILEDB_FLOAT32, &a3);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a3, TILEDB_FILTER_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, layout);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, layout);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a3);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_attribute_free(&a3);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&filter_list);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void SparseArrayFx::check_sparse_array_unordered_with_duplicates_error(
    const std::string& array_name) {
  // Prepare cell buffers
  int buffer_a1[] = {7, 5, 0, 6, 4, 3, 1, 2};
  uint64_t buffer_a2[] = {0, 4, 6, 7, 10, 11, 15, 17};
  char buffer_a2_var[] = "hhhhffagggeddddbbccc";
  float buffer_a3[] = {
      7.1f,
      7.2f,
      5.1f,
      5.2f,
      0.1f,
      0.2f,
      6.1f,
      6.2f,
      4.1f,
      4.2f,
      3.1f,
      3.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f};
  uint64_t buffer_coords_dim1[] = {3, 4, 1, 3, 3, 2, 1, 1};
  uint64_t buffer_coords_dim2[] = {4, 2, 1, 3, 3, 3, 2, 4};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_a2_var,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_a2_var) - 1,
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseArrayFx::check_sparse_array_unordered_with_duplicates_no_check(
    const std::string& array_name) {
  // Create TileDB context with config
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.check_coord_dups", "false", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);

  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
  // reallocate with input config
  vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok();
  tiledb_config_free(&config);

  // Prepare cell buffers
  int buffer_a1[] = {7, 5, 0, 6, 4, 3, 1, 2};
  uint64_t buffer_a2[] = {0, 4, 6, 7, 10, 11, 15, 17};
  char buffer_a2_var[] = "hhhhffagggeddddbbccc";
  float buffer_a3[] = {
      7.1f,
      7.2f,
      5.1f,
      5.2f,
      0.1f,
      0.2f,
      6.1f,
      6.2f,
      4.1f,
      4.2f,
      3.1f,
      3.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f};
  uint64_t buffer_coords_dim1[] = {3, 4, 1, 3, 3, 2, 1, 1};
  uint64_t buffer_coords_dim2[] = {4, 2, 1, 3, 3, 3, 2, 4};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_a2_var,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_a2_var) - 1,
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query - this is unsafe but it should be passing
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseArrayFx::check_sparse_array_unordered_with_duplicates_dedup(
    const std::string& array_name) {
  // Create TileDB context, setting
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.dedup_coords", "true", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);

  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
  // reallocate with input config
  vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok();
  tiledb_config_free(&config);

  // Prepare cell buffers for WRITE
  int buffer_a1[] = {7, 5, 0, 6, 6, 3, 1, 2};
  uint64_t buffer_a2[] = {0, 4, 6, 7, 10, 13, 17, 19};
  char buffer_a2_var[] = "hhhhffaggggggddddbbccc";
  float buffer_a3[] = {
      7.1f,
      7.2f,
      5.1f,
      5.2f,
      0.1f,
      0.2f,
      6.1f,
      6.2f,
      6.1f,
      6.2f,
      3.1f,
      3.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f};
  uint64_t buffer_coords_dim1[] = {3, 4, 1, 3, 3, 2, 1, 1};
  uint64_t buffer_coords_dim2[] = {4, 2, 1, 3, 3, 3, 2, 4};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_a2_var,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_a2_var) - 1,
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit WRITE query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);

  // Prepare cell buffers for READ
  int r_buffer_a1[20];
  uint64_t r_buffer_a2[20];
  char r_buffer_a2_var[40];
  float r_buffer_a3[40];
  uint64_t r_buffer_coords_dim1[20];
  uint64_t r_buffer_coords_dim2[20];
  void* r_buffers[] = {
      r_buffer_a1,
      r_buffer_a2,
      r_buffer_a2_var,
      r_buffer_a3,
      r_buffer_coords_dim1,
      r_buffer_coords_dim2};
  uint64_t r_buffer_sizes[] = {
      sizeof(r_buffer_a1),
      sizeof(r_buffer_a2),
      sizeof(r_buffer_a2_var),
      sizeof(r_buffer_a3),
      sizeof(r_buffer_coords_dim1)};

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create READ query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], r_buffers[0], &r_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], r_buffers[2], &r_buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)r_buffers[1], &r_buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], r_buffers[3], &r_buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], r_buffers[4], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], r_buffers[5], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);

  // Submit READ query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  int c_buffer_a1[] = {0, 1, 2, 3, 6, 7, 5};
  uint64_t c_buffer_a2[] = {0, 1, 3, 6, 10, 13, 17};
  char c_buffer_a2_var[] = "abbcccddddggghhhhff";
  uint64_t c_buffer_coords_dim1[] = {1, 1, 1, 2, 3, 3, 4};
  uint64_t c_buffer_coords_dim2[] = {1, 2, 4, 3, 3, 4, 2};
  float c_buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f,
      5.1f,
      5.2f};
  CHECK(!memcmp(r_buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(r_buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(r_buffer_a2_var, c_buffer_a2_var, sizeof(c_buffer_a2_var) - 1));
  CHECK(!memcmp(r_buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(
      r_buffer_coords_dim1,
      c_buffer_coords_dim1,
      sizeof(c_buffer_coords_dim1)));
  CHECK(!memcmp(
      r_buffer_coords_dim2,
      c_buffer_coords_dim2,
      sizeof(c_buffer_coords_dim2)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseArrayFx::check_sparse_array_unordered_with_all_duplicates_dedup(
    const std::string& array_name) {
  // Create TileDB context, setting
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.dedup_coords", "true", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);

  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
  // reallocate with input config
  vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok();
  tiledb_config_free(&config);

  // Prepare cell buffers for WRITE
  int buffer_a1[] = {0, 0, 0, 0, 0, 0, 0, 0};
  uint64_t buffer_a2[] = {0, 1, 2, 3, 4, 5, 6, 7};
  char buffer_a2_var[] = "aaaaaaaa";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f};
  uint64_t buffer_coords_dim1[] = {3, 3, 3, 3, 3, 3, 3, 3};
  uint64_t buffer_coords_dim2[] = {4, 4, 4, 4, 4, 4, 4, 4};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_a2_var,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_a2_var) - 1,
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit WRITE query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);

  // Prepare cell buffers for READ
  int r_buffer_a1[20];
  uint64_t r_buffer_a2[20];
  char r_buffer_a2_var[40];
  float r_buffer_a3[40];
  uint64_t r_buffer_coords_dim1[20];
  uint64_t r_buffer_coords_dim2[20];
  void* r_buffers[] = {
      r_buffer_a1,
      r_buffer_a2,
      r_buffer_a2_var,
      r_buffer_a3,
      r_buffer_coords_dim1,
      r_buffer_coords_dim2};
  uint64_t r_buffer_sizes[] = {
      sizeof(r_buffer_a1),
      sizeof(r_buffer_a2),
      sizeof(r_buffer_a2_var),
      sizeof(r_buffer_a3),
      sizeof(r_buffer_coords_dim1)};

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create READ query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], r_buffers[0], &r_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], r_buffers[2], &r_buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)r_buffers[1], &r_buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], r_buffers[3], &r_buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], r_buffers[4], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], r_buffers[5], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);

  // Submit READ query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Check correctness
  int c_buffer_a1[] = {0};
  uint64_t c_buffer_a2[] = {0};
  char c_buffer_a2_var[] = "a";
  float c_buffer_a3[] = {0.1f, 0.2f};
  uint64_t c_buffer_coords_dim1[] = {3};
  uint64_t c_buffer_coords_dim2[] = {4};
  CHECK(!memcmp(r_buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(r_buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(r_buffer_a2_var, c_buffer_a2_var, sizeof(c_buffer_a2_var) - 1));
  CHECK(!memcmp(r_buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(
      r_buffer_coords_dim1,
      c_buffer_coords_dim1,
      sizeof(c_buffer_coords_dim1)));
  CHECK(!memcmp(
      r_buffer_coords_dim2,
      c_buffer_coords_dim2,
      sizeof(c_buffer_coords_dim2)));
}

void SparseArrayFx::check_sparse_array_global_with_duplicates_error(
    const std::string& array_name) {
  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t buffer_coords_dim1[] = {1, 1, 1, 2, 4, 3, 3, 3};
  uint64_t buffer_coords_dim2[] = {1, 2, 4, 3, 2, 3, 3, 4};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_var_a2,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit_and_finalize(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseArrayFx::check_sparse_array_global_with_duplicates_no_check(
    const std::string& array_name) {
  // Create TileDB context, setting
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.check_coord_dups", "false", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);

  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
  // reallocate with input config
  vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok();
  tiledb_config_free(&config);

  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t buffer_coords_dim1[] = {1, 1, 1, 2, 4, 3, 3, 3};
  uint64_t buffer_coords_dim2[] = {1, 2, 4, 3, 2, 3, 3, 4};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_var_a2,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query - this is unsafe but it should be passing
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

void SparseArrayFx::check_sparse_array_global_with_duplicates_dedup(
    const std::string& array_name) {
  // Create TileDB context, setting
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.dedup_coords", "true", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);

  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
  // reallocate with input config
  vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok();
  tiledb_config_free(&config);

  // Prepare cell buffers for WRITE
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 5, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 14, 17};
  char buffer_var_a2[] = "abbcccddddegggggghhhh";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      5.1f,
      5.2f,
      7.1f,
      7.2f};
  uint64_t buffer_coords_dim1[] = {1, 1, 1, 2, 4, 3, 3, 3};
  uint64_t buffer_coords_dim2[] = {1, 2, 4, 3, 2, 3, 3, 4};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_var_a2,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit WRITE query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);

  // Prepare cell buffers for READ
  int r_buffer_a1[20];
  uint64_t r_buffer_a2[20];
  char r_buffer_a2_var[40];
  float r_buffer_a3[40];
  uint64_t r_buffer_coords_dim1[20];
  uint64_t r_buffer_coords_dim2[20];
  void* r_buffers[] = {
      r_buffer_a1,
      r_buffer_a2,
      r_buffer_a2_var,
      r_buffer_a3,
      r_buffer_coords_dim1,
      r_buffer_coords_dim2};
  uint64_t r_buffer_sizes[] = {
      sizeof(r_buffer_a1),
      sizeof(r_buffer_a2),
      sizeof(r_buffer_a2_var),
      sizeof(r_buffer_a3),
      sizeof(r_buffer_coords_dim1)};

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create READ query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], r_buffers[0], &r_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], r_buffers[2], &r_buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)r_buffers[1], &r_buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], r_buffers[3], &r_buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], r_buffers[4], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], r_buffers[5], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);

  // Submit READ query
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

  // Check correctness
  int c_buffer_a1[] = {0, 1, 2, 3, 5, 7, 4};
  uint64_t c_buffer_a2[] = {0, 1, 3, 6, 10, 13, 17};
  char c_buffer_a2_var[] = "abbcccddddggghhhhe";
  float c_buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      5.1f,
      5.2f,
      7.1f,
      7.2f,
      4.1f,
      4.2f};
  uint64_t c_buffer_coords_dim1[] = {1, 1, 1, 2, 3, 3, 4};
  uint64_t c_buffer_coords_dim2[] = {1, 2, 4, 3, 3, 4, 2};

  CHECK(!memcmp(r_buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(r_buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(r_buffer_a2_var, c_buffer_a2_var, sizeof(c_buffer_a2_var) - 1));
  CHECK(!memcmp(r_buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(
      r_buffer_coords_dim1,
      c_buffer_coords_dim1,
      sizeof(c_buffer_coords_dim1)));
  CHECK(!memcmp(
      r_buffer_coords_dim2,
      c_buffer_coords_dim2,
      sizeof(c_buffer_coords_dim2)));
}

void SparseArrayFx::check_sparse_array_global_with_all_duplicates_dedup(
    const std::string& array_name) {
  // Create TileDB context, setting
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.dedup_coords", "true", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);

  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
  // reallocate with input config
  vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok();
  tiledb_config_free(&config);

  // Prepare cell buffers for WRITE
  int buffer_a1[] = {0, 0, 0, 0, 0, 0, 0, 0};
  uint64_t buffer_a2[] = {0, 1, 2, 3, 4, 5, 6, 7};
  char buffer_var_a2[] = "aaaaaaaa";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f,
      0.1f,
      0.2f};
  uint64_t buffer_coords_dim1[] = {1, 1, 1, 1, 1, 1, 1};
  uint64_t buffer_coords_dim2[] = {2, 2, 2, 2, 2, 2, 2};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_var_a2,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit WRITE query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);

  // Prepare cell buffers for READ
  int r_buffer_a1[20];
  uint64_t r_buffer_a2[20];
  char r_buffer_a2_var[40];
  float r_buffer_a3[40];
  uint64_t r_buffer_coords_dim1[20];
  uint64_t r_buffer_coords_dim2[20];
  void* r_buffers[] = {
      r_buffer_a1,
      r_buffer_a2,
      r_buffer_a2_var,
      r_buffer_a3,
      r_buffer_coords_dim1,
      r_buffer_coords_dim2};
  uint64_t r_buffer_sizes[] = {
      sizeof(r_buffer_a1),
      sizeof(r_buffer_a2),
      sizeof(r_buffer_a2_var),
      sizeof(r_buffer_a3),
      sizeof(r_buffer_coords_dim1)};

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create READ query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], r_buffers[0], &r_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], r_buffers[2], &r_buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)r_buffers[1], &r_buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], r_buffers[3], &r_buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], r_buffers[4], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], r_buffers[5], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);

  // Submit READ query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Check correctness
  int c_buffer_a1[] = {0};
  uint64_t c_buffer_a2[] = {0};
  char c_buffer_a2_var[] = "a";
  float c_buffer_a3[] = {0.1f, 0.2f};
  uint64_t c_buffer_coords_dim1[] = {1};
  uint64_t c_buffer_coords_dim2[] = {2};

  CHECK(!memcmp(r_buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(r_buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(r_buffer_a2_var, c_buffer_a2_var, sizeof(c_buffer_a2_var) - 1));
  CHECK(!memcmp(r_buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(
      r_buffer_coords_dim1,
      c_buffer_coords_dim1,
      sizeof(c_buffer_coords_dim1)));
  CHECK(!memcmp(
      r_buffer_coords_dim2,
      c_buffer_coords_dim2,
      sizeof(c_buffer_coords_dim2)));
}

void SparseArrayFx::check_non_empty_domain(const std::string& array_name) {
  create_sparse_array(array_name);

  // Check empty domain
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int is_empty;
  uint64_t domain[4];
  rc = tiledb_array_get_non_empty_domain(ctx_, array, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 1);
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 0, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 1);
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 5, domain, &is_empty);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "d1", domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 1);
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "foo", domain, &is_empty);
  CHECK(rc == TILEDB_ERR);

  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Write
  write_partial_sparse_array(array_name);

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Check non-empty domain
  rc = tiledb_array_get_non_empty_domain(ctx_, array, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  uint64_t c_domain[] = {3, 4, 2, 4};
  CHECK(!memcmp(domain, c_domain, sizeof(c_domain)));

  // Check non-empty domain from index
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 0, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  uint64_t c_domain_0[] = {3, 4};
  CHECK(!memcmp(domain, c_domain_0, sizeof(c_domain_0)));
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 1, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  uint64_t c_domain_1[] = {2, 4};
  CHECK(!memcmp(domain, c_domain_1, sizeof(c_domain_1)));
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 4, domain, &is_empty);
  CHECK(rc == TILEDB_ERR);

  // Check non-empty domain from name
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "d1", domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  CHECK(!memcmp(domain, c_domain_0, sizeof(c_domain_0)));
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "d2", domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  CHECK(!memcmp(domain, c_domain_1, sizeof(c_domain_1)));
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "foo", domain, &is_empty);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void SparseArrayFx::check_invalid_offsets(const std::string& array_name) {
  uint64_t buffer_a2[] = {0, 4, 6};
  char buffer_a2_var[] = "hhhhffa";
  uint64_t buffer_coords[] = {3, 4, 4, 2, 3, 3};
  void* buffers[] = {buffer_a2, buffer_a2_var, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a2), sizeof(buffer_a2_var) - 1, sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);

  // Check empty single cell error
  buffer_a2[0] = 0;

  uint64_t a2_buffer_size = 0;
  uint64_t a2_buffer_offset_size = 1 * sizeof(uint64_t);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffers[1], &a2_buffer_size);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", (uint64_t*)buffers[0], &a2_buffer_offset_size);
  CHECK(tiledb_query_submit(ctx_, query) == TILEDB_ERR);

  // Check non-ascending offsets error
  buffer_a2[0] = 0;
  buffer_a2[1] = 6;
  buffer_a2[2] = 4;
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffers[1], &buffer_sizes[1]);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", (uint64_t*)buffers[0], &buffer_sizes[0]);
  CHECK(tiledb_query_submit(ctx_, query) == TILEDB_ERR);

  // Check out-of-bounds offsets error
  buffer_a2[0] = 0;
  buffer_a2[1] = 4;
  buffer_a2[2] = 8;
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffers[1], &buffer_sizes[1]);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", (uint64_t*)buffers[0], &buffer_sizes[0]);
  CHECK(tiledb_query_submit(ctx_, query) == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseArrayFx::check_sparse_array_no_results(
    const std::string& array_name) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Prepare the buffers that will store the result
  uint64_t buffer_size = 1;
  auto buffer = new int[buffer_size / sizeof(int)];
  REQUIRE(buffer != nullptr);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", buffer, &buffer_size);
  REQUIRE(rc == TILEDB_OK);

  // Set subarray
  uint64_t s0[] = {1, 2};
  uint64_t s1[] = {1, 2};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Check that it has no results before submission
  int has_results;
  rc = tiledb_query_has_results(ctx_, query, &has_results);
  CHECK(rc == TILEDB_OK);
  CHECK(!has_results);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Check that it has no results also after submission
  rc = tiledb_query_has_results(ctx_, query, &has_results);
  CHECK(rc == TILEDB_OK);
  CHECK(!has_results);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
  delete[] buffer;
}

void SparseArrayFx::write_partial_sparse_array(const std::string& array_name) {
  // Prepare cell buffers
  int buffer_a1[] = {7, 5, 0};
  uint64_t buffer_a2[] = {0, 4, 6};
  char buffer_a2_var[] = "hhhhffa";
  float buffer_a3[] = {7.1f, 7.2f, 5.1f, 5.2f, 0.1f, 0.2f};
  uint64_t buffer_coords_dim1[] = {3, 4, 3};
  uint64_t buffer_coords_dim2[] = {4, 2, 3};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_a2_var,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_a2_var) - 1,
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[4], buffers[5], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseArrayFx::write_sparse_array_missing_attributes(
    const std::string& array_name) {
  // Prepare cell buffers
  int buffer_a1[] = {7, 5, 0};
  uint64_t buffer_a2[] = {0, 4, 6};
  char buffer_a2_var[] = "hhhhffa";
  float buffer_a3[] = {7.1f, 7.2f, 5.1f, 5.2f, 0.1f, 0.2f};
  uint64_t buffer_coords_dim1[] = {3, 4, 3};
  uint64_t buffer_coords_dim2[] = {4, 2, 3};
  void* buffers[] = {
      buffer_a1,
      buffer_a2,
      buffer_a2_var,
      buffer_a3,
      buffer_coords_dim1,
      buffer_coords_dim2};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_a2_var) - 1,
      sizeof(buffer_a3),
      sizeof(buffer_coords_dim1)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", "d1", "d2"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  // Omit setting the coordinates

  // Submit query - This should fail
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, sorted reads",
    "[capi][sparse][sorted-reads][longtest][rest]") {
  // TODO: refactor for each supported FS.
  std::string array_name = prefix_ + ARRAY;

  SECTION("- no compression, row/row-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_NONE, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  }

  SECTION("- no compression, col/col-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_NONE, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
  }

  SECTION("- no compression, row/col-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_NONE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  }

  SECTION("- gzip compression, row/row-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_GZIP, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  }

  SECTION("- gzip compression, col/col-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_GZIP, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
  }

  SECTION("- gzip compression, row/col-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_GZIP, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  }

  SECTION("- bzip compression, row/col-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  }

  SECTION("- lz4 compression, row/col-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_LZ4, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  }

  SECTION("- rle compression, row/col-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_RLE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  }

  SECTION("- zstd compression, row/col-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_ZSTD, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  }

  SECTION("- double-delta compression, row/col-major") {
    check_sorted_reads(
        array_name,
        TILEDB_FILTER_DOUBLE_DELTA,
        TILEDB_ROW_MAJOR,
        TILEDB_COL_MAJOR);
  }

  SECTION("- delta compression, row/col-major") {
    check_sorted_reads(
        array_name, TILEDB_FILTER_DELTA, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  }

  remove_temp_dir(fs_vec_[0]->temp_dir());
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, duplicates",
    "[capi][sparse][dups][rest-fails][sc-42987]") {
  std::string array_name = prefix_ + "dups";
  create_sparse_array(array_name);

  SECTION("- unordered, error check") {
    check_sparse_array_unordered_with_duplicates_error(array_name);
  }

  SECTION("- unordered, no error check") {
    check_sparse_array_unordered_with_duplicates_no_check(array_name);
  }

  SECTION("- unordered, dedup") {
    check_sparse_array_unordered_with_duplicates_dedup(array_name);
  }

  SECTION("- unordered, all duplicates, dedup") {
    check_sparse_array_unordered_with_all_duplicates_dedup(array_name);
  }

  SECTION("- global, error check") {
    check_sparse_array_global_with_duplicates_error(array_name);
  }

  SECTION("- global, no error check") {
    check_sparse_array_global_with_duplicates_no_check(array_name);
  }

  SECTION("- global, dedup") {
    check_sparse_array_global_with_duplicates_dedup(array_name);
  }

  SECTION("- global, all duplicates, dedup") {
    check_sparse_array_global_with_all_duplicates_dedup(array_name);
  }
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, non-empty domain",
    "[capi][sparse][non-empty][rest]") {
  std::string array_name = prefix_ + "sparse_non_empty";
  check_non_empty_domain(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, invalid offsets on write",
    "[capi][sparse][invalid-offsets][rest]") {
  std::string array_name = prefix_ + "invalid_offs";
  create_sparse_array(array_name);
  check_invalid_offsets(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, no results",
    "[capi][sparse][no-results][rest-fails][sc-43108]") {
  std::string array_name = prefix_ + "no_results";
  create_sparse_array(array_name);
  write_partial_sparse_array(array_name);
  check_sparse_array_no_results(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, missing attributes in writes",
    "[capi][sparse][write-missing-attributes][rest-fails][sc-43108]") {
  std::string array_name = prefix_ + "sparse_write_missing_attributes";
  create_sparse_array(array_name);
  write_sparse_array_missing_attributes(array_name);
  check_sparse_array_no_results(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, error setting subarray on sparse write",
    "[capi][sparse][set-subarray][rest]") {
  std::string array_name = prefix_ + "sparse_set_subarray";
  create_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);

  // Set subarray
  uint64_t s0[] = {1, 1};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_ERR);

  // Submit
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_ERR);

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, check if coords exist",
    "[capi][sparse][coords-exist][rest-fails][sc-43108]") {
  std::string array_name = prefix_ + "sparse_coords_exist";
  create_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);

  // Set attribute buffers
  int a1[] = {1, 2};
  uint64_t a1_size = sizeof(a1);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  char a2[] = {'a', 'b'};
  uint64_t a2_size = sizeof(a2);
  uint64_t a2_off[] = {0, 1};
  uint64_t a2_off_size = sizeof(a2_off);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &a2_off_size);
  CHECK(rc == TILEDB_OK);
  float a3[] = {1.1f, 1.2f, 2.1f, 2.2f};
  uint64_t a3_size = sizeof(a3);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", a3, &a3_size);
  CHECK(rc == TILEDB_OK);

  // Submit query - should error
  CHECK(tiledb_query_submit(ctx_, query) == TILEDB_ERR);

  // Set coordinates
  uint64_t coords_dim1[] = {1, 1};
  uint64_t coords_dim2[] = {1, 2};
  uint64_t coords_size = sizeof(coords_dim1);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query - ok
  CHECK(tiledb_query_submit(ctx_, query) == TILEDB_OK);

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, global order check on write",
    "[capi][sparse][write-global-check][rest]") {
  std::string array_name = prefix_ + "sparse_write_global_check";
  create_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);

  // Set attribute buffers
  int a1[] = {1, 2};
  uint64_t a1_size = sizeof(a1);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  char a2[] = {'a', 'b'};
  uint64_t a2_size = sizeof(a2);
  uint64_t a2_off[] = {0, 1};
  uint64_t a2_off_size = sizeof(a2_off);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &a2_off_size);
  CHECK(rc == TILEDB_OK);
  float a3[] = {1.1f, 1.2f, 2.1f, 2.2f};
  uint64_t a3_size = sizeof(a3);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", a3, &a3_size);
  CHECK(rc == TILEDB_OK);

  // Set coordinates
  uint64_t coords_dim1[] = {1, 1};
  uint64_t coords_dim2[] = {2, 1};
  uint64_t coords_size = sizeof(coords_dim1);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query - ok
  CHECK(tiledb_query_submit(ctx_, query) == TILEDB_ERR);

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, invalidate cached max buffer sizes",
    "[capi][sparse][invalidate-max-sizes][rest]") {
  std::string array_name = prefix_ + "sparse_invalidate_max_sizes";
  create_sparse_array(array_name);
  write_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // ---- First READ query (empty)
  tiledb_query_t* empty_query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &empty_query);
  CHECK(rc == TILEDB_OK);

  // Get max buffer sizes for empty query
  uint64_t a1_size = 4;
  uint64_t a2_off_size = 16;
  uint64_t a2_size = 7;
  uint64_t a3_size = 8;
  uint64_t coords_size = 8;

  // Set attribute buffers
  auto a1 = (int*)malloc(a1_size);
  rc = tiledb_query_set_data_buffer(ctx_, empty_query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  auto a2_off = (uint64_t*)malloc(a2_off_size);
  auto a2 = (char*)malloc(a2_size);
  rc = tiledb_query_set_data_buffer(ctx_, empty_query, "a2", a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, empty_query, "a2", a2_off, &a2_off_size);
  CHECK(rc == TILEDB_OK);
  auto a3 = (float*)malloc(a3_size);
  rc = tiledb_query_set_data_buffer(ctx_, empty_query, "a3", a3, &a3_size);
  CHECK(rc == TILEDB_OK);
  auto coords_dim1 = (uint64_t*)malloc(coords_size);
  auto coords_dim2 = (uint64_t*)malloc(coords_size);
  rc = tiledb_query_set_data_buffer(
      ctx_, empty_query, "d1", coords_dim1, &coords_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, empty_query, "d2", coords_dim2, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Set subarray
  uint64_t s0[] = {1, 1};
  uint64_t s1[] = {3, 3};
  rc = tiledb_query_set_layout(ctx_, empty_query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, empty_query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  CHECK(tiledb_query_submit(ctx_, empty_query) == TILEDB_OK);

  // Check that there are no results
  CHECK(a1_size == 0);
  CHECK(a2_off_size == 0);
  CHECK(a2_size == 0);
  CHECK(a3_size == 0);

  // Clean up
  free(a1);
  free(a2_off);
  free(a2);
  free(a3);
  free(coords_dim1);
  free(coords_dim2);
  tiledb_query_free(&empty_query);
  tiledb_subarray_free(&subarray);

  // ---- Second READ query (non-empty)
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Set max buffer sizes for non-empty query
  a1_size = 8;
  a2_off_size = 16;
  a2_size = 3;
  a3_size = 16;
  coords_size = 16;

  // Set attribute buffers
  a1 = (int*)malloc(a1_size);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  a2_off = (uint64_t*)malloc(a2_off_size);
  a2 = (char*)malloc(a2_size);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &a2_off_size);
  CHECK(rc == TILEDB_OK);
  a3 = (float*)malloc(a3_size);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", a3, &a3_size);
  CHECK(rc == TILEDB_OK);
  coords_dim1 = (uint64_t*)malloc(coords_size);
  coords_dim2 = (uint64_t*)malloc(coords_size);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Set subarray
  s0[0] = 1;
  s0[1] = 1;
  s1[0] = 1;
  s1[1] = 2;
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  CHECK(tiledb_query_submit(ctx_, query) == TILEDB_OK);

  // Check that there are no results
  REQUIRE(a1_size == 2 * sizeof(int));
  REQUIRE(a2_off_size == 2 * sizeof(uint64_t));
  REQUIRE(a2_size == 3 * sizeof(char));
  REQUIRE(a3_size == 4 * sizeof(float));
  CHECK(a1[0] == 0);
  CHECK(a1[1] == 1);
  CHECK(a2_off[0] == 0);
  CHECK(a2_off[1] == 1);
  CHECK(a2[0] == 'a');
  CHECK(a2[1] == 'b');
  CHECK(a2[2] == 'b');
  CHECK(a3[0] == 0.1f);
  CHECK(a3[1] == 0.2f);
  CHECK(a3[2] == 1.1f);
  CHECK(a3[3] == 1.2f);

  // Clean up
  free(a1);
  free(a2_off);
  free(a2);
  free(a3);
  free(coords_dim1);
  free(coords_dim2);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);

  // Clean up
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, encrypted",
    "[capi][sparse][encryption][non-rest]") {
  std::string array_name;
  encryption_type = TILEDB_AES_256_GCM;
  encryption_key = "0123456789abcdeF0123456789abcdeF";

  // TODO: refactor for each supported FS.
  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  array_name = temp_dir + ARRAY;
  check_sorted_reads(
      array_name, TILEDB_FILTER_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, calibrate est size",
    "[capi][sparse][calibrate-est-size][rest]") {
  std::string array_name = prefix_ + "sparse_calibrate_est_size";

  create_sparse_array(array_name);

  // Write twice (2 fragments)
  write_sparse_array(array_name);
  write_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[2];
  uint64_t a1_size = sizeof(a1);
  char a2[6];
  uint64_t a2_size = sizeof(a2);
  uint64_t a2_off[2];
  uint64_t a2_off_size = sizeof(a2_off);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2, &a2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &a2_off_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s0[] = {1, 1};
  uint64_t s1[] = {1, 2};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  CHECK(a1_size == sizeof(a1));
  CHECK(a2_off_size == sizeof(a2_off));
  CHECK(a2_size == 3 * sizeof(char));
  CHECK(a1[0] == 0);
  CHECK(a1[1] == 1);
  CHECK(a2_off[0] == 0);
  CHECK(a2_off[1] == 1);
  CHECK(a2[0] == 'a');
  CHECK(a2[1] == 'b');
  CHECK(a2[2] == 'b');

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, calibrate est size, unary",
    "[capi][sparse][calibrate-est-size-unary][rest]") {
  std::string array_name = prefix_ + "sparse_calibrate_est_size_unary";

  create_sparse_array(array_name);

  // Write twice (2 fragments)
  write_sparse_array(array_name);
  write_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[1];
  uint64_t a1_size = sizeof(a1);
  char a2[2];
  uint64_t a2_size = sizeof(a2);
  uint64_t a2_off[1];
  uint64_t a2_off_size = sizeof(a2_off);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2, &a2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &a2_off_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s0[] = {1, 1};
  uint64_t s1[] = {1, 1};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  CHECK(a1[0] == 0);
  CHECK(a1_size == sizeof(a1));
  CHECK(a2_off[0] == 0);
  CHECK(a2_off_size == sizeof(a2_off));
  CHECK(a2[0] == 'a');
  CHECK(a2_size == sizeof(char));

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, calibrate est size, huge range",
    "[capi][sparse][calibrate-est-size-huge-range][rest]") {
  std::string array_name = prefix_ + "sparse_calibrate_est_size_huge_range";
  const uint64_t dim_domain[4] = {1, UINT64_MAX - 1, 1, UINT64_MAX - 1};

  create_sparse_array(array_name, TILEDB_ROW_MAJOR, dim_domain);

  // Write twice (2 fragments)
  write_sparse_array(array_name);
  write_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[2] = {-1, -1};
  uint64_t a1_size = sizeof(a1);
  char a2[6];
  uint64_t a2_size = sizeof(a2);
  uint64_t a2_off[2];
  uint64_t a2_off_size = sizeof(a2_off);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2, &a2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &a2_off_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s0[] = {1, UINT64_MAX - 1};
  uint64_t s1[] = {1, UINT64_MAX - 1};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  CHECK(a1[0] == 0);
  CHECK(a1[1] == 1);
  CHECK(a1_size == sizeof(a1));
  CHECK(a2_off[0] == 0);
  CHECK(a2_off[1] == 1);
  CHECK(a2_off_size == sizeof(a2_off));
  CHECK(a2[0] == 'a');
  CHECK(a2[1] == 'b');
  CHECK(a2[2] == 'b');
  CHECK(a2_size == 3 * sizeof(char));

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-subarray, 2D, complete",
    "[capi][sparse][MR][2D][complete][rest]") {
  std::string array_name = prefix_ + "sparse_multi_subarray_2d_complete";

  create_sparse_array(array_name);
  write_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[20];
  uint64_t a1_size = sizeof(a1);
  uint64_t coords_dim1[10];
  uint64_t coords_dim2[10];
  uint64_t coords_size = sizeof(coords_dim1);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s00[] = {1, 1};
  uint64_t s01[] = {3, 4};
  uint64_t s10[] = {2, 2};
  uint64_t s11[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s00[0], &s00[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s01[0], &s01[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s10[0], &s10[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s11[0], &s11[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  CHECK(a1[0] == 1);
  CHECK(a1[1] == 2);
  CHECK(a1[2] == 5);
  CHECK(a1[3] == 6);
  CHECK(a1[4] == 7);
  CHECK(a1_size == 5 * sizeof(int));
  CHECK(coords_size == 5 * sizeof(uint64_t));
  CHECK(coords_dim1[0] == 1);
  CHECK(coords_dim2[0] == 2);
  CHECK(coords_dim1[1] == 1);
  CHECK(coords_dim2[1] == 4);
  CHECK(coords_dim1[2] == 4);
  CHECK(coords_dim2[2] == 2);
  CHECK(coords_dim1[3] == 3);
  CHECK(coords_dim2[3] == 3);
  CHECK(coords_dim1[4] == 3);
  CHECK(coords_dim2[4] == 4);

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-subarray, 2D, multiplicities",
    "[capi][sparse][multi-subarray-2d-multiplicities][rest]") {
  std::string array_name = prefix_ + "sparse_multi_subarray_2d_multiplicities";

  create_sparse_array(array_name);
  write_sparse_array(array_name);

  // Disable merge overlapping sparse ranges.
  // Support for returning multiplicities for overlapping ranges will be
  // deprecated in a few releases. Turning off this setting allows to still
  // test that the feature functions properly until we do so. Once support is
  // fully removed for overlapping ranges, this section can be deleted.
  tiledb_error_t* error = nullptr;
  tiledb_config_t* config = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  int rc = tiledb_config_set(
      config, "sm.merge_overlapping_ranges_experimental", "false", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
  // reallocate with input config
  vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok();

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[20];
  uint64_t a1_size = sizeof(a1);
  uint64_t coords_dim1[10];
  uint64_t coords_dim2[10];
  uint64_t coords_size = sizeof(coords_dim1);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s00[] = {1, 1};
  uint64_t s01[] = {3, 4};
  uint64_t s10[] = {2, 2};
  uint64_t s11[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s00[0], &s00[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s01[0], &s01[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s00[0], &s00[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s10[0], &s10[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s11[0], &s11[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_config(ctx_, subarray, config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  check_counts(span(a1, 7), {0, 2, 2, 0, 0, 1, 1, 1});
  CHECK(a1_size == 7 * sizeof(int));
  check_counts(span(coords_dim1, 7), {0, 4, 0, 2, 1});
  check_counts(span(coords_dim2, 7), {0, 0, 3, 1, 3});
  CHECK(coords_size == 7 * sizeof(uint64_t));

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_subarray_free(&subarray);
  tiledb_config_free(&config);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-subarray, 2D, incomplete",
    "[capi][sparse][multi-subarray-2d-incomplete][rest]") {
  std::string array_name = prefix_ + "sparse_multi_subarray_2d_incomplete";

  create_sparse_array(array_name);
  write_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[3];
  uint64_t a1_size = sizeof(a1);
  uint64_t coords_dim1[3];
  uint64_t coords_dim2[3];
  uint64_t coords_size = sizeof(coords_dim1);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s00[] = {1, 1};
  uint64_t s01[] = {3, 4};
  uint64_t s10[] = {2, 2};
  uint64_t s11[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s00[0], &s00[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s01[0], &s01[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s10[0], &s10[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s11[0], &s11[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  if (use_refactored_sparse_global_order_reader()) {
    CHECK(a1_size == 3 * sizeof(int));
    check_counts(span(a1, 3), {0, 1, 1, 0, 0, 1});
    CHECK(coords_size == 3 * sizeof(uint64_t));
    check_counts(span(coords_dim1, 3), {0, 2, 0, 0, 1});
    check_counts(span(coords_dim2, 3), {0, 0, 2, 0, 1});
  } else {
    CHECK(a1_size == 2 * sizeof(int));
    CHECK(a1[0] == 1);
    CHECK(a1[1] == 2);
    CHECK(coords_size == 2 * sizeof(uint64_t));
    CHECK(coords_dim1[0] == 1);
    CHECK(coords_dim2[0] == 2);
    CHECK(coords_dim1[1] == 1);
    CHECK(coords_dim2[1] == 4);
  }

  TRY(ctx_, tiledb_query_submit(ctx_, query));
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  if (use_refactored_sparse_global_order_reader()) {
    CHECK(a1_size == 2 * sizeof(int));
    check_counts(span(a1, 2), {0, 0, 0, 0, 0, 0, 1, 1});
    CHECK(coords_size == 2 * sizeof(uint64_t));
    check_counts(span(coords_dim1, 2), {0, 0, 0, 2});
    check_counts(span(coords_dim2, 2), {0, 0, 0, 1, 1});
  } else {
    CHECK(a1_size == 3 * sizeof(int));
    CHECK(a1[0] == 5);
    CHECK(a1[1] == 6);
    CHECK(a1[2] == 7);
    CHECK(coords_size == 3 * sizeof(uint64_t));
    CHECK(coords_dim1[0] == 4);
    CHECK(coords_dim2[0] == 2);
    CHECK(coords_dim1[1] == 3);
    CHECK(coords_dim2[1] == 3);
    CHECK(coords_dim1[2] == 3);
    CHECK(coords_dim2[2] == 4);
  }

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-subarray, 2D, complete, col",
    "[capi][sparse][multi-subarray-2d-complete-col][rest]") {
  std::string array_name = prefix_ + "sparse_multi_subarray_2d_complete_col";

  create_sparse_array(array_name, TILEDB_COL_MAJOR);
  write_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[20];
  uint64_t a1_size = sizeof(a1);
  uint64_t coords_dim1[10];
  uint64_t coords_dim2[10];
  uint64_t coords_size = sizeof(coords_dim1);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray.
  uint64_t s00[] = {1, 1};
  uint64_t s01[] = {3, 4};
  uint64_t s10[] = {2, 2};
  uint64_t s11[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s00[0], &s00[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s01[0], &s01[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s10[0], &s10[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s11[0], &s11[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  CHECK(a1_size == 5 * sizeof(int));
  check_counts(span(a1, 5), {0, 1, 1, 0, 0, 1, 1, 1});
  CHECK(coords_size == 5 * sizeof(uint64_t));
  check_counts(span(coords_dim1, 5), {0, 2, 0, 2, 1});
  check_counts(span(coords_dim2, 5), {0, 0, 2, 1, 2});

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array 2, multi-range subarray, row-major",
    "[capi][sparse][multi-range-row][rest]") {
  std::string array_name = prefix_ + "sparse_multi_range_row";

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[20];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(b_a1_size == 8 * sizeof(int));
  CHECK(b_a2_off_size == 8 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 20 * sizeof(char));
  CHECK(b_a3_size == 16 * sizeof(float));
  CHECK(b_coords_size == 8 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 1);
  CHECK(b_a1[2] == 2);
  CHECK(b_a1[3] == 3);
  CHECK(b_a1[4] == 4);
  CHECK(b_a1[5] == 6);
  CHECK(b_a1[6] == 7);
  CHECK(b_a1[7] == 5);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 3);
  CHECK(b_a2_off[3] == 6);
  CHECK(b_a2_off[4] == 10);
  CHECK(b_a2_off[5] == 11);
  CHECK(b_a2_off[6] == 14);
  CHECK(b_a2_off[7] == 18);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'b');
  CHECK(b_a2_val[2] == 'b');
  CHECK(b_a2_val[3] == 'c');
  CHECK(b_a2_val[4] == 'c');
  CHECK(b_a2_val[5] == 'c');
  CHECK(b_a2_val[6] == 'd');
  CHECK(b_a2_val[7] == 'd');
  CHECK(b_a2_val[8] == 'd');
  CHECK(b_a2_val[9] == 'd');
  CHECK(b_a2_val[10] == 'e');
  CHECK(b_a2_val[11] == 'g');
  CHECK(b_a2_val[12] == 'g');
  CHECK(b_a2_val[13] == 'g');
  CHECK(b_a2_val[14] == 'h');
  CHECK(b_a2_val[15] == 'h');
  CHECK(b_a2_val[16] == 'h');
  CHECK(b_a2_val[17] == 'h');
  CHECK(b_a2_val[18] == 'f');
  CHECK(b_a2_val[19] == 'f');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 1.1f);
  CHECK(b_a3[3] == 1.2f);
  CHECK(b_a3[4] == 2.1f);
  CHECK(b_a3[5] == 2.2f);
  CHECK(b_a3[6] == 3.1f);
  CHECK(b_a3[7] == 3.2f);
  CHECK(b_a3[8] == 4.1f);
  CHECK(b_a3[9] == 4.2f);
  CHECK(b_a3[10] == 6.1f);
  CHECK(b_a3[11] == 6.2f);
  CHECK(b_a3[12] == 7.1f);
  CHECK(b_a3[13] == 7.2f);
  CHECK(b_a3[14] == 5.1f);
  CHECK(b_a3[15] == 5.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);
  CHECK(b_coords_dim1[1] == 1);
  CHECK(b_coords_dim2[1] == 2);
  CHECK(b_coords_dim1[2] == 1);
  CHECK(b_coords_dim2[2] == 4);
  CHECK(b_coords_dim1[3] == 2);
  CHECK(b_coords_dim2[3] == 3);
  CHECK(b_coords_dim1[4] == 3);
  CHECK(b_coords_dim2[4] == 1);
  CHECK(b_coords_dim1[5] == 3);
  CHECK(b_coords_dim2[5] == 3);
  CHECK(b_coords_dim1[6] == 3);
  CHECK(b_coords_dim2[6] == 4);
  CHECK(b_coords_dim1[7] == 4);
  CHECK(b_coords_dim2[7] == 2);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, col-major",
    "[capi][sparse][multi-range-col][rest]") {
  std::string array_name = prefix_ + "sparse_multi_range_col";

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[20];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(b_a1_size == 8 * sizeof(int));
  CHECK(b_a2_off_size == 8 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 20 * sizeof(char));
  CHECK(b_a3_size == 16 * sizeof(float));
  CHECK(b_coords_size == 8 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 4);
  CHECK(b_a1[2] == 1);
  CHECK(b_a1[3] == 5);
  CHECK(b_a1[4] == 3);
  CHECK(b_a1[5] == 6);
  CHECK(b_a1[6] == 2);
  CHECK(b_a1[7] == 7);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 2);
  CHECK(b_a2_off[3] == 4);
  CHECK(b_a2_off[4] == 6);
  CHECK(b_a2_off[5] == 10);
  CHECK(b_a2_off[6] == 13);
  CHECK(b_a2_off[7] == 16);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'e');
  CHECK(b_a2_val[2] == 'b');
  CHECK(b_a2_val[3] == 'b');
  CHECK(b_a2_val[4] == 'f');
  CHECK(b_a2_val[5] == 'f');
  CHECK(b_a2_val[6] == 'd');
  CHECK(b_a2_val[7] == 'd');
  CHECK(b_a2_val[8] == 'd');
  CHECK(b_a2_val[9] == 'd');
  CHECK(b_a2_val[10] == 'g');
  CHECK(b_a2_val[11] == 'g');
  CHECK(b_a2_val[12] == 'g');
  CHECK(b_a2_val[13] == 'c');
  CHECK(b_a2_val[14] == 'c');
  CHECK(b_a2_val[15] == 'c');
  CHECK(b_a2_val[16] == 'h');
  CHECK(b_a2_val[17] == 'h');
  CHECK(b_a2_val[18] == 'h');
  CHECK(b_a2_val[19] == 'h');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 4.1f);
  CHECK(b_a3[3] == 4.2f);
  CHECK(b_a3[4] == 1.1f);
  CHECK(b_a3[5] == 1.2f);
  CHECK(b_a3[6] == 5.1f);
  CHECK(b_a3[7] == 5.2f);
  CHECK(b_a3[8] == 3.1f);
  CHECK(b_a3[9] == 3.2f);
  CHECK(b_a3[10] == 6.1f);
  CHECK(b_a3[11] == 6.2f);
  CHECK(b_a3[12] == 2.1f);
  CHECK(b_a3[13] == 2.2f);
  CHECK(b_a3[14] == 7.1f);
  CHECK(b_a3[15] == 7.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);
  CHECK(b_coords_dim1[1] == 3);
  CHECK(b_coords_dim2[1] == 1);
  CHECK(b_coords_dim1[2] == 1);
  CHECK(b_coords_dim2[2] == 2);
  CHECK(b_coords_dim1[3] == 4);
  CHECK(b_coords_dim2[3] == 2);
  CHECK(b_coords_dim1[4] == 2);
  CHECK(b_coords_dim2[4] == 3);
  CHECK(b_coords_dim1[5] == 3);
  CHECK(b_coords_dim2[5] == 3);
  CHECK(b_coords_dim1[6] == 1);
  CHECK(b_coords_dim2[6] == 4);
  CHECK(b_coords_dim1[7] == 3);
  CHECK(b_coords_dim2[7] == 4);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, row-major, incomplete 1",
    "[capi][sparse][multi-range-row-incomplete-1][rest]") {
  std::string array_name = prefix_ + "sparse_multi_range_row_incomplete_1";

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[6];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 4 * sizeof(int));
  CHECK(b_a2_off_size == 4 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 10 * sizeof(char));
  CHECK(b_a3_size == 8 * sizeof(float));
  CHECK(b_coords_size == 4 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 1);
  CHECK(b_a1[2] == 2);
  CHECK(b_a1[3] == 3);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 3);
  CHECK(b_a2_off[3] == 6);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'b');
  CHECK(b_a2_val[2] == 'b');
  CHECK(b_a2_val[3] == 'c');
  CHECK(b_a2_val[4] == 'c');
  CHECK(b_a2_val[5] == 'c');
  CHECK(b_a2_val[6] == 'd');
  CHECK(b_a2_val[7] == 'd');
  CHECK(b_a2_val[8] == 'd');
  CHECK(b_a2_val[9] == 'd');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 1.1f);
  CHECK(b_a3[3] == 1.2f);
  CHECK(b_a3[4] == 2.1f);
  CHECK(b_a3[5] == 2.2f);
  CHECK(b_a3[6] == 3.1f);
  CHECK(b_a3[7] == 3.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);
  CHECK(b_coords_dim1[1] == 1);
  CHECK(b_coords_dim2[1] == 2);
  CHECK(b_coords_dim1[2] == 1);
  CHECK(b_coords_dim2[2] == 4);
  CHECK(b_coords_dim1[3] == 2);
  CHECK(b_coords_dim2[3] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(b_a1_size == 4 * sizeof(int));
  CHECK(b_a2_off_size == 4 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 10 * sizeof(char));
  CHECK(b_a3_size == 8 * sizeof(float));
  CHECK(b_coords_size == 4 * sizeof(uint64_t));

  CHECK(b_a1[0] == 4);
  CHECK(b_a1[1] == 6);
  CHECK(b_a1[2] == 7);
  CHECK(b_a1[3] == 5);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 4);
  CHECK(b_a2_off[3] == 8);

  CHECK(b_a2_val[0] == 'e');
  CHECK(b_a2_val[1] == 'g');
  CHECK(b_a2_val[2] == 'g');
  CHECK(b_a2_val[3] == 'g');
  CHECK(b_a2_val[4] == 'h');
  CHECK(b_a2_val[5] == 'h');
  CHECK(b_a2_val[6] == 'h');
  CHECK(b_a2_val[7] == 'h');
  CHECK(b_a2_val[8] == 'f');
  CHECK(b_a2_val[9] == 'f');

  CHECK(b_a3[0] == 4.1f);
  CHECK(b_a3[1] == 4.2f);
  CHECK(b_a3[2] == 6.1f);
  CHECK(b_a3[3] == 6.2f);
  CHECK(b_a3[4] == 7.1f);
  CHECK(b_a3[5] == 7.2f);
  CHECK(b_a3[6] == 5.1f);
  CHECK(b_a3[7] == 5.2f);

  CHECK(b_coords_dim1[0] == 3);
  CHECK(b_coords_dim2[0] == 1);
  CHECK(b_coords_dim1[1] == 3);
  CHECK(b_coords_dim2[1] == 3);
  CHECK(b_coords_dim1[2] == 3);
  CHECK(b_coords_dim2[2] == 4);
  CHECK(b_coords_dim1[3] == 4);
  CHECK(b_coords_dim2[3] == 2);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, col-major, incomplete 1",
    "[capi][sparse][multi-range-col-incomplete-1][rest]") {
  std::string array_name = prefix_ + "sparse_multi_range_col_incomplete_1";

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[6];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 4 * sizeof(int));
  CHECK(b_a2_off_size == 4 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 6 * sizeof(char));
  CHECK(b_a3_size == 8 * sizeof(float));
  CHECK(b_coords_size == 4 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 4);
  CHECK(b_a1[2] == 1);
  CHECK(b_a1[3] == 5);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 2);
  CHECK(b_a2_off[3] == 4);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'e');
  CHECK(b_a2_val[2] == 'b');
  CHECK(b_a2_val[3] == 'b');
  CHECK(b_a2_val[4] == 'f');
  CHECK(b_a2_val[5] == 'f');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 4.1f);
  CHECK(b_a3[3] == 4.2f);
  CHECK(b_a3[4] == 1.1f);
  CHECK(b_a3[5] == 1.2f);
  CHECK(b_a3[6] == 5.1f);
  CHECK(b_a3[7] == 5.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);
  CHECK(b_coords_dim1[1] == 3);
  CHECK(b_coords_dim2[1] == 1);
  CHECK(b_coords_dim1[2] == 1);
  CHECK(b_coords_dim2[2] == 2);
  CHECK(b_coords_dim1[3] == 4);
  CHECK(b_coords_dim2[3] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(b_a1_size == 4 * sizeof(int));
  CHECK(b_a2_off_size == 4 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 14 * sizeof(char));
  CHECK(b_a3_size == 8 * sizeof(float));
  CHECK(b_coords_size == 4 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);
  CHECK(b_a1[1] == 6);
  CHECK(b_a1[2] == 2);
  CHECK(b_a1[3] == 7);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 4);
  CHECK(b_a2_off[2] == 7);
  CHECK(b_a2_off[3] == 10);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');
  CHECK(b_a2_val[4] == 'g');
  CHECK(b_a2_val[5] == 'g');
  CHECK(b_a2_val[6] == 'g');
  CHECK(b_a2_val[7] == 'c');
  CHECK(b_a2_val[8] == 'c');
  CHECK(b_a2_val[9] == 'c');
  CHECK(b_a2_val[10] == 'h');
  CHECK(b_a2_val[11] == 'h');
  CHECK(b_a2_val[12] == 'h');
  CHECK(b_a2_val[13] == 'h');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);
  CHECK(b_a3[2] == 6.1f);
  CHECK(b_a3[3] == 6.2f);
  CHECK(b_a3[4] == 2.1f);
  CHECK(b_a3[5] == 2.2f);
  CHECK(b_a3[6] == 7.1f);
  CHECK(b_a3[7] == 7.2f);

  CHECK(b_coords_dim1[0] == 2);
  CHECK(b_coords_dim2[0] == 3);
  CHECK(b_coords_dim1[1] == 3);
  CHECK(b_coords_dim2[1] == 3);
  CHECK(b_coords_dim1[2] == 1);
  CHECK(b_coords_dim2[2] == 4);
  CHECK(b_coords_dim1[3] == 3);
  CHECK(b_coords_dim2[3] == 4);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, row-major, incomplete 2",
    "[capi][sparse][multi-range-row-incomplete-2][rest]") {
  std::string array_name = prefix_ + "sparse_multi_range_row_incomplete_2";

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[3];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 3 * sizeof(int));
  CHECK(b_a2_off_size == 3 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 6 * sizeof(char));
  CHECK(b_a3_size == 6 * sizeof(float));
  CHECK(b_coords_size == 3 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 1);
  CHECK(b_a1[2] == 2);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 3);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'b');
  CHECK(b_a2_val[2] == 'b');
  CHECK(b_a2_val[3] == 'c');
  CHECK(b_a2_val[4] == 'c');
  CHECK(b_a2_val[5] == 'c');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 1.1f);
  CHECK(b_a3[3] == 1.2f);
  CHECK(b_a3[4] == 2.1f);
  CHECK(b_a3[5] == 2.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);
  CHECK(b_coords_dim1[1] == 1);
  CHECK(b_coords_dim2[1] == 2);
  CHECK(b_coords_dim1[2] == 1);
  CHECK(b_coords_dim2[2] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);

  CHECK(b_coords_dim1[0] == 2);
  CHECK(b_coords_dim2[0] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 3 * sizeof(int));
  CHECK(b_a2_off_size == 3 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 8 * sizeof(char));
  CHECK(b_a3_size == 6 * sizeof(float));
  CHECK(b_coords_size == 3 * sizeof(uint64_t));

  CHECK(b_a1[0] == 4);
  CHECK(b_a1[1] == 6);
  CHECK(b_a1[2] == 7);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 4);

  CHECK(b_a2_val[0] == 'e');
  CHECK(b_a2_val[1] == 'g');
  CHECK(b_a2_val[2] == 'g');
  CHECK(b_a2_val[3] == 'g');
  CHECK(b_a2_val[4] == 'h');
  CHECK(b_a2_val[5] == 'h');
  CHECK(b_a2_val[6] == 'h');
  CHECK(b_a2_val[7] == 'h');

  CHECK(b_a3[0] == 4.1f);
  CHECK(b_a3[1] == 4.2f);
  CHECK(b_a3[2] == 6.1f);
  CHECK(b_a3[3] == 6.2f);
  CHECK(b_a3[4] == 7.1f);
  CHECK(b_a3[5] == 7.2f);

  CHECK(b_coords_dim1[0] == 3);
  CHECK(b_coords_dim2[0] == 1);
  CHECK(b_coords_dim1[1] == 3);
  CHECK(b_coords_dim2[1] == 3);
  CHECK(b_coords_dim1[2] == 3);
  CHECK(b_coords_dim2[2] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 5);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'f');
  CHECK(b_a2_val[1] == 'f');

  CHECK(b_a3[0] == 5.1f);
  CHECK(b_a3[1] == 5.2f);

  CHECK(b_coords_dim1[0] == 4);
  CHECK(b_coords_dim2[0] == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, col-major, incomplete 2",
    "[capi][sparse][multi-range-col-incomplete-2][rest]") {
  std::string array_name = prefix_ + "sparse_multi_range_col_incomplete_2";

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[3];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 4);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'e');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 4.1f);
  CHECK(b_a3[3] == 4.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);
  CHECK(b_coords_dim1[1] == 3);
  CHECK(b_coords_dim2[1] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 1);
  CHECK(b_a1[1] == 5);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 2);

  CHECK(b_a2_val[0] == 'b');
  CHECK(b_a2_val[1] == 'b');
  CHECK(b_a2_val[2] == 'f');
  CHECK(b_a2_val[3] == 'f');

  CHECK(b_a3[0] == 1.1f);
  CHECK(b_a3[1] == 1.2f);
  CHECK(b_a3[2] == 5.1f);
  CHECK(b_a3[3] == 5.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 2);
  CHECK(b_coords_dim1[1] == 4);
  CHECK(b_coords_dim2[1] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 7 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);
  CHECK(b_a1[1] == 6);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 4);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');
  CHECK(b_a2_val[4] == 'g');
  CHECK(b_a2_val[5] == 'g');
  CHECK(b_a2_val[6] == 'g');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);
  CHECK(b_a3[2] == 6.1f);
  CHECK(b_a3[3] == 6.2f);

  CHECK(b_coords_dim1[0] == 2);
  CHECK(b_coords_dim2[0] == 3);
  CHECK(b_coords_dim1[1] == 3);
  CHECK(b_coords_dim2[1] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 7 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 2);
  CHECK(b_a1[1] == 7);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 3);

  CHECK(b_a2_val[0] == 'c');
  CHECK(b_a2_val[1] == 'c');
  CHECK(b_a2_val[2] == 'c');
  CHECK(b_a2_val[3] == 'h');
  CHECK(b_a2_val[4] == 'h');
  CHECK(b_a2_val[5] == 'h');
  CHECK(b_a2_val[6] == 'h');

  CHECK(b_a3[0] == 2.1f);
  CHECK(b_a3[1] == 2.2f);
  CHECK(b_a3[2] == 7.1f);
  CHECK(b_a3[3] == 7.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 4);
  CHECK(b_coords_dim1[1] == 3);
  CHECK(b_coords_dim2[1] == 4);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, row-major, incomplete 3",
    "[capi][sparse][multi-range-row-incomplete-3][rest-fails][sc-43108]") {
  std::string array_name = prefix_ + "sparse_multi_range_row_incomplete_3";

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[2];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 1);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'b');
  CHECK(b_a2_val[2] == 'b');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 1.1f);
  CHECK(b_a3[3] == 1.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);
  CHECK(b_coords_dim1[1] == 1);
  CHECK(b_coords_dim2[1] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 2);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'c');
  CHECK(b_a2_val[1] == 'c');
  CHECK(b_a2_val[2] == 'c');

  CHECK(b_a3[0] == 2.1f);
  CHECK(b_a3[1] == 2.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);

  CHECK(b_coords_dim1[0] == 2);
  CHECK(b_coords_dim2[0] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 4);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'e');

  CHECK(b_a3[0] == 4.1f);
  CHECK(b_a3[1] == 4.2f);

  CHECK(b_coords_dim1[0] == 3);
  CHECK(b_coords_dim2[0] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 7 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 6);
  CHECK(b_a1[1] == 7);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 3);

  CHECK(b_a2_val[0] == 'g');
  CHECK(b_a2_val[1] == 'g');
  CHECK(b_a2_val[2] == 'g');
  CHECK(b_a2_val[3] == 'h');
  CHECK(b_a2_val[4] == 'h');
  CHECK(b_a2_val[5] == 'h');
  CHECK(b_a2_val[6] == 'h');

  CHECK(b_a3[0] == 6.1f);
  CHECK(b_a3[1] == 6.2f);
  CHECK(b_a3[2] == 7.1f);
  CHECK(b_a3[3] == 7.2f);

  CHECK(b_coords_dim1[0] == 3);
  CHECK(b_coords_dim2[0] == 3);
  CHECK(b_coords_dim1[1] == 3);
  CHECK(b_coords_dim2[1] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 5);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'f');
  CHECK(b_a2_val[1] == 'f');

  CHECK(b_a3[0] == 5.1f);
  CHECK(b_a3[1] == 5.2f);

  CHECK(b_coords_dim1[0] == 4);
  CHECK(b_coords_dim2[0] == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, row-major, incomplete 4",
    "[capi][sparse][multi-range-row-incomplete-4][rest]") {
  std::string array_name = prefix_ + "sparse_multi_range_row_incomplete_4";

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[1];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'a');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 1);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'b');
  CHECK(b_a2_val[1] == 'b');

  CHECK(b_a3[0] == 1.1f);
  CHECK(b_a3[1] == 1.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 2);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'c');
  CHECK(b_a2_val[1] == 'c');
  CHECK(b_a2_val[2] == 'c');

  CHECK(b_a3[0] == 2.1f);
  CHECK(b_a3[1] == 2.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);

  CHECK(b_coords_dim1[0] == 2);
  CHECK(b_coords_dim2[0] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 4);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'e');

  CHECK(b_a3[0] == 4.1f);
  CHECK(b_a3[1] == 4.2f);

  CHECK(b_coords_dim1[0] == 3);
  CHECK(b_coords_dim2[0] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 6);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'g');
  CHECK(b_a2_val[1] == 'g');
  CHECK(b_a2_val[1] == 'g');

  CHECK(b_a3[0] == 6.1f);
  CHECK(b_a3[1] == 6.2f);

  CHECK(b_coords_dim1[0] == 3);
  CHECK(b_coords_dim2[0] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 7);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'h');
  CHECK(b_a2_val[1] == 'h');
  CHECK(b_a2_val[1] == 'h');
  CHECK(b_a2_val[2] == 'h');

  CHECK(b_a3[0] == 7.1f);
  CHECK(b_a3[1] == 7.2f);

  CHECK(b_coords_dim1[0] == 3);
  CHECK(b_coords_dim2[0] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 5);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'f');
  CHECK(b_a2_val[1] == 'f');

  CHECK(b_a3[0] == 5.1f);
  CHECK(b_a3[1] == 5.2f);

  CHECK(b_coords_dim1[0] == 4);
  CHECK(b_coords_dim2[0] == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, col-major, incomplete 4",
    "[capi][sparse][multi-range-col-incomplete-4][rest]") {
  std::string array_name = prefix_ + "sparse_multi_range_col_incomplete_4";

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[1];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'a');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 4);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'e');

  CHECK(b_a3[0] == 4.1f);
  CHECK(b_a3[1] == 4.2f);

  CHECK(b_coords_dim1[0] == 3);
  CHECK(b_coords_dim2[0] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 1);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'b');
  CHECK(b_a2_val[1] == 'b');

  CHECK(b_a3[0] == 1.1f);
  CHECK(b_a3[1] == 1.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 5);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'f');
  CHECK(b_a2_val[1] == 'f');

  CHECK(b_a3[0] == 5.1f);
  CHECK(b_a3[1] == 5.2f);

  CHECK(b_coords_dim1[0] == 4);
  CHECK(b_coords_dim2[0] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);

  CHECK(b_coords_dim1[0] == 2);
  CHECK(b_coords_dim2[0] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 6);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'g');
  CHECK(b_a2_val[1] == 'g');
  CHECK(b_a2_val[1] == 'g');

  CHECK(b_a3[0] == 6.1f);
  CHECK(b_a3[1] == 6.2f);

  CHECK(b_coords_dim1[0] == 3);
  CHECK(b_coords_dim2[0] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 2);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'c');
  CHECK(b_a2_val[1] == 'c');
  CHECK(b_a2_val[1] == 'c');

  CHECK(b_a3[0] == 2.1f);
  CHECK(b_a3[1] == 2.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 7);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'h');
  CHECK(b_a2_val[1] == 'h');
  CHECK(b_a2_val[2] == 'h');
  CHECK(b_a2_val[3] == 'h');

  CHECK(b_a3[0] == 7.1f);
  CHECK(b_a3[1] == 7.2f);

  CHECK(b_coords_dim1[0] == 3);
  CHECK(b_coords_dim2[0] == 4);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, row-major, incomplete 5",
    "[capi][sparse][multi-range-row-incomplete-5][rest-fails][sc-43108]") {
  std::string array_name = prefix_ + "sparse_multi_range_row_incomplete_5";
  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[1];
  uint64_t b_a1_size = 1;
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 0);
  CHECK(b_a2_off_size == 0);
  CHECK(b_a2_val_size == 0);
  CHECK(b_a3_size == 0);
  CHECK(b_coords_size == 0);

  // Reset buffer
  b_a1_size = 1 * sizeof(int);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'a');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, col-major, incomplete 5",
    "[capi][sparse][multi-range-col-incomplete-5][rest-fails][sc-43108]") {
  std::string array_name = prefix_ + "sparse_multi_range_col_incomplete_5";

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[1];
  uint64_t b_a1_size = 1;
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[10];
  uint64_t b_coords_dim2[10];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 0);
  CHECK(b_a2_off_size == 0);
  CHECK(b_a2_val_size == 0);
  CHECK(b_a3_size == 0);
  CHECK(b_coords_size == 0);

  // Reset buffer
  b_a1_size = 1 * sizeof(int);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 1 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'a');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);

  CHECK(b_coords_dim1[0] == 1);
  CHECK(b_coords_dim2[0] == 1);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, global order with 0-sized buffers",
    "[capi][sparse][global-check][zero-buffers][rest]") {
  std::string array_name = prefix_ + "sparse_write_global_check";
  create_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);

  // Prepare attribute buffers
  int a1[1];
  char a2[1];
  uint64_t a2_off[1];
  float a3[1];
  uint64_t coords_dim1[1];
  uint64_t coords_dim2[1];
  uint64_t zero_size = 0;

  // Set buffers with zero size
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &zero_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2, &zero_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &zero_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", a3, &zero_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d1", coords_dim1, &zero_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d2", coords_dim2, &zero_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  CHECK(tiledb_array_close(ctx_, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, split coordinate buffers",
    "[capi][sparse][split-coords]") {
  std::string array_name = prefix_ + "sparse_split_coords";
  create_sparse_array(array_name);

  // ---- WRITE ----

  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a2_size = sizeof(buffer_a2);
  // No need to store the last '\0' character
  uint64_t buffer_var_a2_size = sizeof(buffer_var_a2) - 1;
  uint64_t buffer_a3_size = sizeof(buffer_a3);
  uint64_t buffer_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t buffer_d1_size = sizeof(buffer_d1);
  uint64_t buffer_d2[] = {1, 2, 4, 3, 1, 3, 4, 2};
  uint64_t buffer_d2_size = sizeof(buffer_d2);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_var_a2, &buffer_var_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", (uint64_t*)buffer_a2, &buffer_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", buffer_d2, &buffer_d2_size);
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

  // ---- READ ----

  // Create array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[30];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[30];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[30];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[30];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[15];
  uint64_t b_coords_dim2[15];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set a subarray
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffer sizes
  CHECK(b_a1_size == buffer_a1_size);
  CHECK(b_a2_off_size == buffer_a2_size);
  CHECK(b_a2_val_size == buffer_var_a2_size);
  CHECK(b_a3_size == buffer_a3_size);
  CHECK(b_coords_size == (buffer_d1_size + buffer_d2_size) / 2);

  // Check buffer data
  int c_b_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t c_b_a2_off[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char c_b_a2_val[] = "abbcccddddeffggghhhh";
  float c_b_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t c_b_coords_dim1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t c_b_coords_dim2[] = {1, 2, 4, 3, 1, 3, 4, 2};
  CHECK(!memcmp(c_b_a1, b_a1, b_a1_size));
  CHECK(!memcmp(c_b_a2_off, b_a2_off, b_a2_off_size));
  CHECK(!memcmp(c_b_a2_val, b_a2_val, b_a2_val_size));
  CHECK(!memcmp(c_b_a3, b_a3, b_a3_size));
  CHECK(!memcmp(c_b_coords_dim1, b_coords_dim1, b_coords_size));
  CHECK(!memcmp(c_b_coords_dim2, b_coords_dim2, b_coords_size));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, split coordinate buffers, global write",
    "[capi][sparse][split-coords][global][rest]") {
  std::string array_name = prefix_ + "sparse_split_coords_global";
  create_sparse_array(array_name);

  // ---- WRITE ----

  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a2_size = sizeof(buffer_a2);
  // No need to store the last '\0' character
  uint64_t buffer_var_a2_size = sizeof(buffer_var_a2) - 1;
  uint64_t buffer_a3_size = sizeof(buffer_a3);
  uint64_t buffer_d1[] = {1, 1, 1, 2, 3, 4, 3, 3};
  uint64_t buffer_d1_size = sizeof(buffer_d1);
  uint64_t buffer_d2[] = {1, 2, 4, 3, 1, 2, 3, 4};
  uint64_t buffer_d2_size = sizeof(buffer_d2);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_var_a2, &buffer_var_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", (uint64_t*)buffer_a2, &buffer_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", buffer_d2, &buffer_d2_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit_and_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // ---- READ ----

  // Create array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[30];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[30];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[30];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[30];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords_dim1[15];
  uint64_t b_coords_dim2[15];
  uint64_t b_coords_size = sizeof(b_coords_dim1);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", b_coords_dim1, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", b_coords_dim2, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set a subarray
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffer sizes
  CHECK(b_a1_size == buffer_a1_size);
  CHECK(b_a2_off_size == buffer_a2_size);
  CHECK(b_a2_val_size == buffer_var_a2_size);
  CHECK(b_a3_size == buffer_a3_size);
  CHECK(b_coords_size == (buffer_d1_size + buffer_d2_size) / 2);

  // Check buffer data
  int c_b_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t c_b_a2_off[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char c_b_a2_val[] = "abbcccddddeffggghhhh";
  float c_b_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t c_b_coords_dim1[] = {1, 1, 1, 2, 3, 4, 3, 3};
  uint64_t c_b_coords_dim2[] = {1, 2, 4, 3, 1, 2, 3, 4};
  CHECK(!memcmp(c_b_a1, b_a1, b_a1_size));
  CHECK(!memcmp(c_b_a2_off, b_a2_off, b_a2_off_size));
  CHECK(!memcmp(c_b_a2_val, b_a2_val, b_a2_val_size));
  CHECK(!memcmp(c_b_a3, b_a3, b_a3_size));
  CHECK(!memcmp(c_b_coords_dim1, b_coords_dim1, b_coords_size));
  CHECK(!memcmp(c_b_coords_dim2, b_coords_dim2, b_coords_size));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, split coordinate buffers, errors",
    "[capi][sparse][split-coords][errors][rest]") {
  std::string array_name = prefix_ + "sparse_split_coords_errors";
  create_sparse_array(array_name);

  // Prepare cell buffers
  uint64_t buffer_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t buffer_d1_size = sizeof(buffer_d1);
  uint64_t buffer_d2[] = {1, 2, 4, 3, 1, 3};
  uint64_t buffer_d2_size = sizeof(buffer_d2);

  // Open array for writing
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);

  // Set buffers with different coord number
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", buffer_d2, &buffer_d2_size);
  CHECK(rc == TILEDB_OK);

  // Set zipped coordinates buffer after separate coordinate buffers
  // have been set
  uint64_t buffer_coords_dim1[] = {1, 2};
  uint64_t buffer_coords_dim2[] = {1, 3};
  uint64_t buffer_coords[] = {1, 2, 3, 4};
  uint64_t buffer_coords_dim_size = sizeof(buffer_coords_dim1);
  uint64_t buffer_coords_size = sizeof(buffer_coords);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_ERR);

  // Set coordinates buffer for dimension that does not exist
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "foo", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_ERR);

  // Set rest of the attribute buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a2_size = sizeof(buffer_a2);
  // No need to store the last '\0' character
  uint64_t buffer_var_a2_size = sizeof(buffer_var_a2) - 1;
  uint64_t buffer_a3_size = sizeof(buffer_a3);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_var_a2, &buffer_var_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", (uint64_t*)buffer_a2, &buffer_a2_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_free(&query);

  // Set zipped coordinates first and the separate coordinate buffers
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);

  // Set separate coordinate buffers and then zipped coordinates
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim_size);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array for reading
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Set zipped coordinates first and the separate coordinate buffers
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_ERR);
  tiledb_query_free(&query);

  // Set separate coordinate buffers and then zipped coordinates
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, split coordinate buffers for reads",
    "[capi][sparse][split-coords][read][rest]") {
  std::string array_name = prefix_ + "sparse_split_coords_read";
  create_sparse_array(array_name);

  // ---- WRITE ----

  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a2_size = sizeof(buffer_a2);
  // No need to store the last '\0' character
  uint64_t buffer_var_a2_size = sizeof(buffer_var_a2) - 1;
  uint64_t buffer_a3_size = sizeof(buffer_a3);
  uint64_t buffer_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t buffer_d1_size = sizeof(buffer_d1);
  uint64_t buffer_d2[] = {1, 2, 4, 3, 1, 3, 4, 2};
  uint64_t buffer_d2_size = sizeof(buffer_d2);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_var_a2, &buffer_var_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", (uint64_t*)buffer_a2, &buffer_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", buffer_d2, &buffer_d2_size);
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

  // ---- READ ----

  // Create array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[30];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[30];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[30];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[30];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_d1[30];
  uint64_t b_d1_size = sizeof(b_d1);
  uint64_t b_d2[30];
  uint64_t b_d2_size = sizeof(b_d2);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d1", b_d1, &b_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d2", b_d2, &b_d2_size);
  CHECK(rc == TILEDB_OK);

  // Set a subarray
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffer sizes
  CHECK(b_a1_size == buffer_a1_size);
  CHECK(b_a2_off_size == buffer_a2_size);
  CHECK(b_a2_val_size == buffer_var_a2_size);
  CHECK(b_a3_size == buffer_a3_size);
  CHECK(b_d1_size == buffer_d1_size);
  CHECK(b_d2_size == buffer_d2_size);

  // Check buffer data
  int c_b_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t c_b_a2_off[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char c_b_a2_val[] = "abbcccddddeffggghhhh";
  float c_b_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t c_b_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t c_b_d2[] = {1, 2, 4, 3, 1, 3, 4, 2};
  CHECK(!memcmp(c_b_a1, b_a1, b_a1_size));
  CHECK(!memcmp(c_b_a2_off, b_a2_off, b_a2_off_size));
  CHECK(!memcmp(c_b_a2_val, b_a2_val, b_a2_val_size));
  CHECK(!memcmp(c_b_a3, b_a3, b_a3_size));
  CHECK(!memcmp(c_b_d1, b_d1, b_d1_size));
  CHECK(!memcmp(c_b_d2, b_d2, b_d2_size));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, split coordinate buffers for reads, subset of "
    "dimensions",
    "[capi][sparse][split-coords][read][subset][rest]") {
  std::string array_name = prefix_ + "sparse_split_coords_read_subset";
  create_sparse_array(array_name);

  // ---- WRITE ----

  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a2_size = sizeof(buffer_a2);
  // No need to store the last '\0' character
  uint64_t buffer_var_a2_size = sizeof(buffer_var_a2) - 1;
  uint64_t buffer_a3_size = sizeof(buffer_a3);
  uint64_t buffer_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t buffer_d1_size = sizeof(buffer_d1);
  uint64_t buffer_d2[] = {1, 2, 4, 3, 1, 3, 4, 2};
  uint64_t buffer_d2_size = sizeof(buffer_d2);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_var_a2, &buffer_var_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", (uint64_t*)buffer_a2, &buffer_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", buffer_d2, &buffer_d2_size);
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

  // ---- READ ----

  // Create array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[30];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[30];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[30];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[30];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_d1[30];
  uint64_t b_d1_size = sizeof(b_d1);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a2", b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "d1", b_d1, &b_d1_size);
  CHECK(rc == TILEDB_OK);

  // Set a subarray
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffer sizes
  CHECK(b_a1_size == buffer_a1_size);
  CHECK(b_a2_off_size == buffer_a2_size);
  CHECK(b_a2_val_size == buffer_var_a2_size);
  CHECK(b_a3_size == buffer_a3_size);
  CHECK(b_d1_size == buffer_d1_size);

  // Check buffer data
  int c_b_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t c_b_a2_off[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char c_b_a2_val[] = "abbcccddddeffggghhhh";
  float c_b_a3[] = {
      0.1f,
      0.2f,
      1.1f,
      1.2f,
      2.1f,
      2.2f,
      3.1f,
      3.2f,
      4.1f,
      4.2f,
      5.1f,
      5.2f,
      6.1f,
      6.2f,
      7.1f,
      7.2f};
  uint64_t c_b_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  CHECK(!memcmp(c_b_a1, b_a1, b_a1_size));
  CHECK(!memcmp(c_b_a2_off, b_a2_off, b_a2_off_size));
  CHECK(!memcmp(c_b_a2_val, b_a2_val, b_a2_val_size));
  CHECK(!memcmp(c_b_a3, b_a3, b_a3_size));
  CHECK(!memcmp(c_b_d1, b_d1, b_d1_size));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "Sparse array: 2D, multi write global order",
    "[capi][sparse][2D][multi-write][rest]") {
  std::string array_name = prefix_ + "sparse_split_coords_read_subset";
  create_sparse_array(array_name);

  std::vector<uint64_t> d1 = {1, 1, 2, 2};
  uint64_t d1_size = d1.size() * sizeof(uint64_t);
  std::vector<uint64_t> d2 = {1, 2, 1, 2};
  uint64_t d2_size = d2.size() * sizeof(uint64_t);
  std::vector<int> a1 = {1, 2, 3, 4};
  uint64_t a1_size = a1.size() * sizeof(int);
  std::vector<uint64_t> a2_off = {
      0, sizeof(char), 3 * sizeof(char), 6 * sizeof(char)};

  uint64_t a2_off_size = a2_off.size() * sizeof(uint64_t);
  std::vector<char> a2_val = {'a', 'b', 'b', 'c', 'c', 'c', 'd', 'd', 'd'};
  uint64_t a2_val_size = a2_val.size() * sizeof(char);
  std::vector<float> a3 = {1.1f, 1.2f, 2.1f, 2.2f, 3.1f, 3.2f, 4.1f, 4.2f};
  uint64_t a3_size = a3.size() * sizeof(float);
  tiledb::test::QueryBuffers buffers;
  buffers["d1"] = tiledb::test::QueryBuffer({&d1[0], d1_size, nullptr, 0});
  buffers["d2"] = tiledb::test::QueryBuffer({&d2[0], d2_size, nullptr, 0});
  buffers["a1"] = tiledb::test::QueryBuffer({&a1[0], a1_size, nullptr, 0});
  buffers["a2"] = tiledb::test::QueryBuffer(
      {&a2_off[0], a2_off_size, &a2_val[0], a2_val_size});
  buffers["a3"] = tiledb::test::QueryBuffer({&a3[0], a3_size, nullptr, 0});

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);

  // Set buffers
  for (const auto& b : buffers) {
    if (b.second.var_ == nullptr) {  // Fixed-sized
      rc = tiledb_query_set_data_buffer(
          ctx_,
          query,
          b.first.c_str(),
          b.second.fixed_,
          (uint64_t*)&(b.second.fixed_size_));
      CHECK(rc == TILEDB_OK);
    } else {  // Var-sized
      rc = tiledb_query_set_data_buffer(
          ctx_,
          query,
          b.first.c_str(),
          b.second.var_,
          (uint64_t*)&(b.second.var_size_));
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_offsets_buffer(
          ctx_,
          query,
          b.first.c_str(),
          (uint64_t*)b.second.fixed_,
          (uint64_t*)&(b.second.fixed_size_));
      CHECK(rc == TILEDB_OK);
    }
  }

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Create new buffers of smaller size to test being able to write multiple
  // buffer sizes
  std::vector<uint64_t> d1_2 = {3, 3};
  uint64_t d1_size_2 = d1_2.size() * sizeof(uint64_t);
  std::vector<uint64_t> d2_2 = {1, 2};
  uint64_t d2_size_2 = d2_2.size() * sizeof(uint64_t);

  std::vector<int> a1_2 = {5, 6};
  uint64_t a1_size_2 = a1_2.size() * sizeof(int);
  std::vector<uint64_t> a2_off_2 = {0, 2 * sizeof(char)};

  uint64_t a2_off_size_2 = a2_off_2.size() * sizeof(uint64_t);
  std::vector<char> a2_val_2 = {'e', 'e', 'f', 'f', 'f', 'f'};
  uint64_t a2_val_size_2 = a2_val_2.size() * sizeof(char);
  std::vector<float> a3_2 = {5.1f, 5.2f, 6.1f, 6.2f};
  uint64_t a3_size_2 = a3_2.size() * sizeof(float);
  tiledb::test::QueryBuffers buffers2;
  buffers2["d1"] = tiledb::test::QueryBuffer({&d1_2[0], d1_size_2, nullptr, 0});
  buffers2["d2"] = tiledb::test::QueryBuffer({&d2_2[0], d2_size_2, nullptr, 0});
  buffers2["a1"] = tiledb::test::QueryBuffer({&a1_2[0], a1_size_2, nullptr, 0});
  buffers2["a2"] = tiledb::test::QueryBuffer(
      {&a2_off_2[0], a2_off_size_2, &a2_val_2[0], a2_val_size_2});
  buffers2["a3"] = tiledb::test::QueryBuffer({&a3_2[0], a3_size_2, nullptr, 0});

  // Set buffers
  for (const auto& b : buffers2) {
    if (b.second.var_ == nullptr) {  // Fixed-sized
      rc = tiledb_query_set_data_buffer(
          ctx_,
          query,
          b.first.c_str(),
          b.second.fixed_,
          (uint64_t*)&(b.second.fixed_size_));
      CHECK(rc == TILEDB_OK);
    } else {  // Var-sized
      rc = tiledb_query_set_data_buffer(
          ctx_,
          query,
          b.first.c_str(),
          b.second.var_,
          (uint64_t*)&(b.second.var_size_));
      CHECK(rc == TILEDB_OK);
      rc = tiledb_query_set_offsets_buffer(
          ctx_,
          query,
          b.first.c_str(),
          (uint64_t*)b.second.fixed_,
          (uint64_t*)&(b.second.fixed_size_));
      CHECK(rc == TILEDB_OK);
    }
  }

  // Submit query
  rc = tiledb_query_submit_and_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Write sparse array without setting layout",
    "[capi][sparse][query]") {
  // Create the array.
  uint64_t domain[2]{0, 3};
  uint64_t x_tile_extent{4};
  auto array_schema = create_array_schema(
      ctx,
      TILEDB_SPARSE,
      {"x"},
      {TILEDB_UINT64},
      {&domain[0]},
      {&x_tile_extent},
      {"a"},
      {TILEDB_FLOAT64},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      4096,
      false);
  auto array_name = create_temporary_array("sparse_array1", array_schema);
  tiledb_array_schema_free(&array_schema);

  // Open array for writing.
  tiledb_array_t* array;
  require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_WRITE));

  // Define input data and write.
  std::vector<uint64_t> input_dim_data{0, 1, 2, 3};
  std::vector<double> input_attr_data{0.5, 1.0, 1.5, 2.0};
  uint64_t dim_data_size{input_dim_data.size() * sizeof(uint64_t)};
  uint64_t attr_data_size{input_attr_data.size() * sizeof(double)};

  // Create write query.
  tiledb_query_t* query;
  require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
  require_tiledb_ok(tiledb_query_set_data_buffer(
      ctx, query, "x", input_dim_data.data(), &dim_data_size));
  require_tiledb_ok(tiledb_query_set_data_buffer(
      ctx, query, "a", input_attr_data.data(), &attr_data_size));

  // Submit write query.
  require_tiledb_ok(tiledb_query_submit(ctx, query));

  // Clean-up.
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "Test array directory serialization",
    "[capi][sparse][array-directory][serialization]") {
#ifdef TILEDB_SERIALIZATION
  SupportedFsLocal local_fs;
  std::string array_name = local_fs.file_prefix() + local_fs.temp_dir() +
                           "serialize_array_directory";

  create_sparse_array(array_name);

  // Write twice (2 fragments)
  write_sparse_array(array_name);
  write_sparse_array(array_name);

  // Consolidate fragments
  tiledb_error_t* error = nullptr;
  tiledb_config_t* cfg = nullptr;
  REQUIRE(tiledb_config_alloc(&cfg, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  int rc = tiledb_array_consolidate(ctx_, array_name.c_str(), cfg);
  CHECK(rc == TILEDB_OK);

  // Consolidate array metadata
  rc = tiledb_config_set(cfg, "sm.consolidation.mode", "array_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_consolidate(ctx_, array_name.c_str(), cfg);
  CHECK(rc == TILEDB_OK);

  // Consolidate fragment metadata
  rc = tiledb_config_set(cfg, "sm.consolidation.mode", "fragment_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_consolidate(ctx_, array_name.c_str(), cfg);
  CHECK(rc == TILEDB_OK);

  // Consolidate commits
  rc = tiledb_config_set(cfg, "sm.consolidation.mode", "commits", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_consolidate(ctx_, array_name.c_str(), cfg);
  CHECK(rc == TILEDB_OK);

  // Get the array directory
  tiledb::Context& ctx = vanilla_context_cpp();
  tiledb::sm::ContextResources& resources = ctx.ptr()->context().resources();
  tiledb::sm::URI array_uri(array_name);
  tiledb::sm::ArrayDirectory array_dir =
      tiledb::sm::ArrayDirectory(resources, array_uri, 0, 5);

  // Serialize and deserialize it
  ::capnp::MallocMessageBuilder message;
  tiledb::sm::serialization::capnp::ArrayDirectory::Builder array_dir_builder =
      message.initRoot<tiledb::sm::serialization::capnp::ArrayDirectory>();
  tiledb::sm::serialization::array_directory_to_capnp(
      array_dir, &array_dir_builder);
  auto deserialized_array_dir =
      tiledb::sm::serialization::array_directory_from_capnp(
          array_dir_builder, resources, array_uri);

  // Compare original array_directory to the deserialized one
  REQUIRE(
      deserialized_array_dir->uri().to_string() == array_dir.uri().to_string());
  REQUIRE(
      deserialized_array_dir->unfiltered_fragment_uris() ==
      array_dir.unfiltered_fragment_uris());
  REQUIRE(
      deserialized_array_dir->consolidated_commit_uris_set() ==
      array_dir.consolidated_commit_uris_set());
  REQUIRE(
      deserialized_array_dir->array_schema_uris() ==
      array_dir.array_schema_uris());
  REQUIRE(
      deserialized_array_dir->latest_array_schema_uri() ==
      array_dir.latest_array_schema_uri());
  REQUIRE(
      deserialized_array_dir->array_meta_uris_to_vacuum() ==
      array_dir.array_meta_uris_to_vacuum());
  REQUIRE(
      deserialized_array_dir->array_meta_vac_uris_to_vacuum() ==
      array_dir.array_meta_vac_uris_to_vacuum());
  REQUIRE(
      deserialized_array_dir->commit_uris_to_consolidate() ==
      array_dir.commit_uris_to_consolidate());
  REQUIRE(
      deserialized_array_dir->commit_uris_to_vacuum() ==
      array_dir.commit_uris_to_vacuum());
  REQUIRE(
      deserialized_array_dir->consolidated_commits_uris_to_vacuum() ==
      array_dir.consolidated_commits_uris_to_vacuum());
  REQUIRE(
      deserialized_array_dir->array_meta_uris() == array_dir.array_meta_uris());
  REQUIRE(
      deserialized_array_dir->fragment_meta_uris() ==
      array_dir.fragment_meta_uris());
  REQUIRE(
      deserialized_array_dir->delete_and_update_tiles_location() ==
      array_dir.delete_and_update_tiles_location());
  REQUIRE(
      deserialized_array_dir->timestamp_start() == array_dir.timestamp_start());
  REQUIRE(deserialized_array_dir->timestamp_end() == array_dir.timestamp_end());

  REQUIRE(resources.vfs().remove_dir(tiledb::sm::URI(array_name)).ok());
#endif
}
