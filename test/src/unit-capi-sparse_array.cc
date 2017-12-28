/**
 * @file   unit-capi-sparse_array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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
 * Tests of C API for sparse array operations.
 */

#include "catch.hpp"
#include "posix_filesystem.h"
#include "tiledb.h"

#include <sys/time.h>
#include <cassert>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <sstream>

#ifdef HAVE_S3
#include "s3.h"
#endif

struct SparseArrayFx {
  // Constant parameters
  const char* ATTR_NAME = "a";
  const char* DIM1_NAME = "x";
  const char* DIM2_NAME = "y";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const tiledb_array_type_t ARRAY_TYPE = TILEDB_SPARSE;
  int COMPRESSION_LEVEL = -1;
#ifdef HAVE_HDFS
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
#endif
#ifdef HAVE_S3
  tiledb::S3 s3_;
  const char* S3_BUCKET = "tiledb";
  const std::string S3_TEMP_DIR = "s3://tiledb/tiledb_test/";
#endif
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::posix::current_dir() + "/tiledb_test/";
  int ITER_NUM = 5;
  const std::string ARRAY = "sparse_array";

  // TileDB context
  tiledb_ctx_t* ctx_;

  // Functions
  SparseArrayFx();
  ~SparseArrayFx();
  void create_temp_dir();
  void remove_temp_dir();
  void check_sorted_reads(
      const std::string& array_name,
      tiledb_compressor_t compressor,
      tiledb_layout_t tile_order,
      tiledb_layout_t cell_order);

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
      const tiledb_compressor_t compressor,
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

SparseArrayFx::SparseArrayFx() {
  // Create TileDB context
  tiledb_config_t* config = nullptr;
  REQUIRE(tiledb_config_create(&config) == TILEDB_OK);
#ifdef HAVE_S3
  REQUIRE(
      tiledb_config_set(
          config, "tiledb.s3.endpoint_override", "localhost:9999") ==
      TILEDB_OK);
#endif
  REQUIRE(tiledb_ctx_create(&ctx_, config) == TILEDB_OK);
  REQUIRE(tiledb_config_free(config) == TILEDB_OK);

  // Connect to S3
#ifdef HAVE_S3
  // TODO: use tiledb_vfs_t instead of S3::*
  tiledb::S3::S3Config s3_config;
  s3_config.endpoint_override_ = "localhost:9999";
  REQUIRE(s3_.connect(s3_config).ok());

  // Create bucket if it does not exist
  if (!s3_.bucket_exists(S3_BUCKET))
    REQUIRE(s3_.create_bucket(S3_BUCKET).ok());
#endif

  std::srand(0);

  create_temp_dir();
}

SparseArrayFx::~SparseArrayFx() {
  CHECK(tiledb_ctx_free(ctx_) == TILEDB_OK);
  remove_temp_dir();
}

void SparseArrayFx::create_temp_dir() {
  remove_temp_dir();

#ifdef HAVE_S3
  REQUIRE(s3_.create_dir(tiledb::URI(S3_TEMP_DIR)).ok());
#endif
#ifdef HAVE_HDFS
  auto cmd_hdfs = std::string("hadoop fs -mkdir -p ") + HDFS_TEMP_DIR;
  REQUIRE(system(cmd_hdfs.c_str()) == 0);
#endif
  auto cmd_posix = std::string("mkdir -p ") + FILE_TEMP_DIR;
  REQUIRE(system(cmd_posix.c_str()) == 0);
}

void SparseArrayFx::remove_temp_dir() {
// Delete temporary directory
#ifdef HAVE_S3
  REQUIRE(s3_.remove_path(tiledb::URI(S3_TEMP_DIR)).ok());
#endif
#ifdef HAVE_HDFS
  auto cmd_hdfs = std::string("hadoop fs -rm -r -f ") + HDFS_TEMP_DIR;
  REQUIRE(system(cmd_hdfs.c_str()) == 0);
#endif
  auto cmd_posix = std::string("rm -rf ") + FILE_TEMP_DIR;
  REQUIRE(system(cmd_posix.c_str()) == 0);
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
    const tiledb_compressor_t compressor,
    const tiledb_layout_t cell_order,
    const tiledb_layout_t tile_order) {
  // Prepare and set the array_metadata metadata object and data structures
  int64_t dim_domain[] = {domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};

  // Create attribute
  tiledb_attribute_t* a;
  int rc = tiledb_attribute_create(ctx_, &a, ATTR_NAME, ATTR_TYPE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a, compressor, COMPRESSION_LEVEL);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_create(
      ctx_, &d1, DIM1_NAME, TILEDB_INT64, &dim_domain[0], &tile_extent_0);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_create(
      ctx_, &d2, DIM2_NAME, TILEDB_INT64, &dim_domain[2], &tile_extent_1);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_create(ctx_, &domain, DIM_TYPE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);

  // Create array_metadata metadata
  tiledb_array_metadata_t* array_metadata;
  rc = tiledb_array_metadata_create(ctx_, &array_metadata, array_name.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_capacity(ctx_, array_metadata, capacity);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_cell_order(ctx_, array_metadata, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_tile_order(ctx_, array_metadata, tile_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_array_type(ctx_, array_metadata, ARRAY_TYPE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata, a);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_set_domain(ctx_, array_metadata, domain);
  REQUIRE(rc == TILEDB_OK);

  // Create the array_metadata
  rc = tiledb_array_create(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_attribute_free(ctx_, a);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_dimension_free(ctx_, d2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_free(ctx_, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_metadata_free(ctx_, array_metadata);
  REQUIRE(rc == TILEDB_OK);
}

int* SparseArrayFx::read_sparse_array_2D(
    const std::string& array_name,
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const tiledb_query_type_t query_type,
    const tiledb_layout_t query_layout) {
  // Initialize a subarray
  const int64_t subarray[] = {
      domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};

  // Subset over a specific attribute
  const char* attributes[] = {ATTR_NAME};

  // Prepare the buffers that will store the result
  int64_t domain_size_0 = domain_0_hi - domain_0_lo + 1;
  int64_t domain_size_1 = domain_1_hi - domain_1_lo + 1;
  int64_t cell_num = domain_size_0 * domain_size_1;
  auto buffer_a1 = new int[cell_num];
  REQUIRE(buffer_a1 != nullptr);
  void* buffers[] = {buffer_a1};
  uint64_t buffer_size_a1 = cell_num * sizeof(int);
  uint64_t buffer_sizes[] = {buffer_size_a1};

  // Create query
  tiledb_query_t* query;
  int rc = tiledb_query_create(ctx_, &query, array_name.c_str(), query_type);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 1, buffers, buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray, TILEDB_INT64);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Free/finalize query
  rc = tiledb_query_free(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Success - return the created buffer
  return buffer_a1;
}

void SparseArrayFx::write_sparse_array_unsorted_2D(
    const std::string& array_name,
    const int64_t domain_size_0,
    const int64_t domain_size_1) {
  // Generate random attribute values and coordinates for sparse write
  int64_t cell_num = domain_size_0 * domain_size_1;
  auto buffer_a1 = new int[cell_num];
  auto buffer_coords = new int64_t[2 * cell_num];
  int64_t coords_index = 0L;
  for (int64_t i = 0; i < domain_size_0; ++i) {
    for (int64_t j = 0; j < domain_size_1; ++j) {
      buffer_a1[i * domain_size_1 + j] = i * domain_size_1 + j;
      buffer_coords[2 * coords_index] = i;
      buffer_coords[2 * coords_index + 1] = j;
      coords_index++;
    }
  }

  // Prepare buffers
  void* buffers[] = {buffer_a1, buffer_coords};
  uint64_t buffer_sizes[2];
  buffer_sizes[0] = cell_num * sizeof(int);
  buffer_sizes[1] = 2 * cell_num * sizeof(int64_t);

  // Set attributes
  const char* attributes[] = {ATTR_NAME, TILEDB_COORDS};

  // Create query
  tiledb_query_t* query;
  int rc = tiledb_query_create(ctx_, &query, array_name.c_str(), TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 2, buffers, buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Free/finalize query
  rc = tiledb_query_free(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  delete[] buffer_a1;
  delete[] buffer_coords;
}

void SparseArrayFx::test_random_subarrays(
    const std::string& array_name,
    int64_t domain_size_0,
    int64_t domain_size_1,
    int iter_num) {
  // write array_metadata cells with value = row id * columns + col id to disk
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

    // read subarray
    int* buffer = read_sparse_array_2D(
        array_name, d0_lo, d0_hi, d1_lo, d1_hi, TILEDB_READ, TILEDB_ROW_MAJOR);
    CHECK(buffer != NULL);

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
    tiledb_compressor_t compressor,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order) {
  // Parameters used in this test
  int64_t domain_size_0 = 5000;
  int64_t domain_size_1 = 1000;
  int64_t tile_extent_0 = 100;
  int64_t tile_extent_1 = 100;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  int64_t capacity = 100000;
  int iter_num = (compressor != TILEDB_BZIP2) ? ITER_NUM : 1;

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

TEST_CASE_METHOD(
    SparseArrayFx, "C API: Test sorted reads", "[capi], [sparse]") {
  std::string array_name;

  SECTION("- no compression, row/row-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_NO_COMPRESSION, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_NO_COMPRESSION, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_NO_COMPRESSION, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
#endif
  }

  SECTION("- no compression, col/col-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_NO_COMPRESSION, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_NO_COMPRESSION, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_NO_COMPRESSION, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
#endif
  }

  SECTION("- no compression, row/col-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_NO_COMPRESSION, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_NO_COMPRESSION, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_NO_COMPRESSION, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif
  }

  SECTION("- gzip compression, row/row-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_GZIP, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_GZIP, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_GZIP, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
#endif
  }

  SECTION("- gzip compression, col/col-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_GZIP, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_GZIP, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_GZIP, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
#endif
  }

  SECTION("- gzip compression, row/col-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_GZIP, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_GZIP, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_GZIP, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif
  }

  SECTION("- blosc compression, row/col-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_BLOSC, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_BLOSC, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_BLOSC, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif
  }

  SECTION("- bzip compression, row/col-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif
  }

  SECTION("- lz4 compression, row/col-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_LZ4, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_LZ4, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_LZ4, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif
  }

  SECTION("- rle compression, row/col-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_RLE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_RLE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_RLE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif
  }

  SECTION("- zstd compression, row/col-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_ZSTD, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_ZSTD, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_ZSTD, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif
  }

  SECTION("- double-delta compression, row/col-major") {
    // posix
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_DOUBLE_DELTA, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

#ifdef HAVE_S3
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_DOUBLE_DELTA, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif

#ifdef HAVE_HDFS
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_DOUBLE_DELTA, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
#endif
  }
}
