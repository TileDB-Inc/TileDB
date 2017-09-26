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
#include "tiledb.h"

#include <sys/time.h>
#include <cassert>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <sstream>

struct SparseArrayFx {
  // Constant parameters
  const char* ATTR_NAME = "a";
  const char* DIM1_NAME = "x";
  const char* DIM2_NAME = "y";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const tiledb_array_type_t ARRAY_TYPE = TILEDB_SPARSE;
  int COMPRESSION_LEVEL = -1;

  // Workspace folder name
  const std::string GROUP = "my_group/";

  // Array name
  std::string array_name_;

  // Array metadata object under test
  tiledb_array_metadata_t* array_metadata_;

  // TileDB context
  tiledb_ctx_t* ctx_;

  SparseArrayFx() {
    // Initialize context
    int rc = tiledb_ctx_create(&ctx_);
    assert(rc == TILEDB_OK);

    // Create group, delete it if it already exists
    std::string cmd = "test -d " + GROUP;
    rc = system(cmd.c_str());
    if (rc == 0) {
      cmd = "rm -rf " + GROUP;
      rc = system(cmd.c_str());
      assert(rc == 0);
    }
    rc = tiledb_group_create(ctx_, GROUP.c_str());
    assert(rc == TILEDB_OK);
  }

  ~SparseArrayFx() {
    // Finalize TileDB context
    tiledb_ctx_free(ctx_);

    // Remove the temporary group
    std::string cmd = "rm -rf " + GROUP;
    int rc = system(cmd.c_str());
    assert(rc == 0);
  }

  /**
   * Creates a 2D sparse array.
   *
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
    // Error code
    int rc;

    // Prepare and set the array_metadata metadata object and data structures
    int64_t domain[] = {domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};

    // Create attribute
    tiledb_attribute_t* a;
    rc = tiledb_attribute_create(ctx_, &a, ATTR_NAME, ATTR_TYPE);
    REQUIRE(rc == TILEDB_OK);
    rc =
        tiledb_attribute_set_compressor(ctx_, a, compressor, COMPRESSION_LEVEL);
    REQUIRE(rc == TILEDB_OK);

    // Create hyperspace
    tiledb_hyperspace_t* hyperspace;
    rc = tiledb_hyperspace_create(ctx_, &hyperspace, DIM_TYPE);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_hyperspace_add_dimension(
        ctx_, hyperspace, DIM1_NAME, &domain[0], &tile_extent_0);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_hyperspace_add_dimension(
        ctx_, hyperspace, DIM2_NAME, &domain[2], &tile_extent_1);
    REQUIRE(rc == TILEDB_OK);

    // Create array_metadata metadata
    rc = tiledb_array_metadata_create(
        ctx_, &array_metadata_, array_name_.c_str());
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_metadata_set_capacity(ctx_, array_metadata_, capacity);
    REQUIRE(rc == TILEDB_OK);
    rc =
        tiledb_array_metadata_set_cell_order(ctx_, array_metadata_, cell_order);
    REQUIRE(rc == TILEDB_OK);
    rc =
        tiledb_array_metadata_set_tile_order(ctx_, array_metadata_, tile_order);
    REQUIRE(rc == TILEDB_OK);
    rc =
        tiledb_array_metadata_set_array_type(ctx_, array_metadata_, ARRAY_TYPE);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata_, a);
    REQUIRE(rc == TILEDB_OK);
    rc =
        tiledb_array_metadata_set_hyperspace(ctx_, array_metadata_, hyperspace);
    REQUIRE(rc == TILEDB_OK);

    // Create the array_metadata
    rc = tiledb_array_create(ctx_, array_metadata_);
    REQUIRE(rc == TILEDB_OK);

    // Clean up
    rc = tiledb_attribute_free(ctx_, a);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_hyperspace_free(ctx_, hyperspace);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_metadata_free(ctx_, array_metadata_);
    REQUIRE(rc == TILEDB_OK);
  }

  /**
   * Reads a subarray oriented by the input boundaries and outputs the buffer
   * containing the attribute values of the corresponding cells.
   *
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
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const tiledb_query_type_t query_type,
      const tiledb_layout_t query_layout) {
    // Error code
    int rc;

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
    assert(buffer_a1);
    void* buffers[] = {buffer_a1};
    uint64_t buffer_size_a1 = cell_num * sizeof(int);
    uint64_t buffer_sizes[] = {buffer_size_a1};

    // Create query
    tiledb_query_t* query;
    rc = tiledb_query_create(
        ctx_,
        &query,
        array_name_.c_str(),
        query_type,
        query_layout,
        subarray,
        attributes,
        1,
        buffers,
        buffer_sizes);
    if (rc != TILEDB_OK)
      return nullptr;

    // Submit query
    rc = tiledb_query_submit(ctx_, query);
    if (rc != TILEDB_OK) {
      tiledb_query_free(ctx_, query);
      return nullptr;
    }

    // Free/finalize query
    rc = tiledb_query_free(ctx_, query);
    if (rc != TILEDB_OK)
      return nullptr;

    // Success - return the created buffer
    return buffer_a1;
  }

  /** Sets the array name for the current test. */
  void set_array_name(const char* name) {
    array_name_ = GROUP + name;
  }

  /**
   * Write random values in unsorted mode. The buffer is initialized with each
   * cell being equalt to row_id*domain_size_1+col_id.
   *
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @return TILEDB_OK on success and TILEDB_ERR on error.
   */
  int write_sparse_array_unsorted_2D(
      const int64_t domain_size_0, const int64_t domain_size_1) {
    // Error code
    int rc;

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

    // Create query
    tiledb_query_t* query;
    rc = tiledb_query_create(
        ctx_,
        &query,
        array_name_.c_str(),
        TILEDB_WRITE,
        TILEDB_UNORDERED,
        nullptr,
        nullptr,
        0,
        buffers,
        buffer_sizes);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Submit query
    rc = tiledb_query_submit(ctx_, query);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Free/finalize query
    rc = tiledb_query_free(ctx_, query);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Clean up
    delete[] buffer_a1;
    delete[] buffer_coords;

    // Success
    return TILEDB_OK;
  }

  bool test_random_subarrays(
      int64_t domain_size_0, int64_t domain_size_1, int ntests) {
    // write array_metadata cells with value = row id * columns + col id to disk
    int rc = write_sparse_array_unsorted_2D(domain_size_0, domain_size_1);
    REQUIRE(rc == TILEDB_OK);

    // test random subarrays and check with corresponding value set by
    // row_id*dim1+col_id. top left corner is always 4,4.
    int64_t d0_lo = 4;
    int64_t d0_hi = 0;
    int64_t d1_lo = 4;
    int64_t d1_hi = 0;
    int64_t height = 0, width = 0;

    for (int iter = 0; iter < ntests; ++iter) {
      height = rand() % (domain_size_0 - d0_lo);
      width = rand() % (domain_size_1 - d1_lo);
      d0_hi = d0_lo + height;
      d1_hi = d1_lo + width;
      int64_t index = 0;

      // read subarray
      int* buffer = read_sparse_array_2D(
          d0_lo, d0_hi, d1_lo, d1_hi, TILEDB_READ, TILEDB_ROW_MAJOR);
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
      if (!allok) {
        return false;
      }
    }

    return true;
  }
};

/**
 * test is to randomly read subregions of the array and
 * check with corresponding value set by row_id*dim1+col_id
 * top left corner is always 4,4
 * test runs through 10 iterations to choose random
 * width and height of the sub-regions
 */
TEST_CASE_METHOD(SparseArrayFx, "C API: Test random sparse sorted reads") {
  // error code
  int rc;

  // Parameters used in this test
  int64_t domain_size_0 = 5000;
  int64_t domain_size_1 = 1000;
  int64_t tile_extent_0 = 100;
  int64_t tile_extent_1 = 100;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  int64_t capacity = 1000;
  int ntests = 5;

  // set array_metadata name
  set_array_name("sparse_test_5000x1000_100x100");

  SECTION("- no compression row-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_NO_COMPRESSION,
        TILEDB_ROW_MAJOR,
        TILEDB_COL_MAJOR);
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }

  SECTION("- no compression col-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_NO_COMPRESSION,
        TILEDB_COL_MAJOR,
        TILEDB_COL_MAJOR);
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }

  SECTION("- no compression row/col-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_NO_COMPRESSION,
        TILEDB_ROW_MAJOR,
        TILEDB_COL_MAJOR);
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }

  SECTION("- gzip compression row/col-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_GZIP,
        TILEDB_ROW_MAJOR,
        TILEDB_COL_MAJOR);
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }

  SECTION("- gzip compression col-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_GZIP,
        TILEDB_COL_MAJOR,
        TILEDB_COL_MAJOR);
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }

  SECTION("- gzip compression row/col-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_GZIP,
        TILEDB_ROW_MAJOR,
        TILEDB_COL_MAJOR);
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }

  SECTION("- blosc compression row/col-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_BLOSC,
        TILEDB_ROW_MAJOR,
        TILEDB_COL_MAJOR);
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }

  SECTION("- bzip compression row/col-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_BZIP2,
        TILEDB_ROW_MAJOR,
        TILEDB_COL_MAJOR);
    // Only run 1 randomized trial here as Bzip is ~10x slower than other
    // compressors
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }

  SECTION("- lz4 compression row/col-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_LZ4,
        TILEDB_ROW_MAJOR,
        TILEDB_COL_MAJOR);
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }

  SECTION("- rle compression row/col-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_RLE,
        TILEDB_ROW_MAJOR,
        TILEDB_COL_MAJOR);
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }

  SECTION("- zstd compression row/col-major") {
    create_sparse_array_2D(
        tile_extent_0,
        tile_extent_1,
        domain_0_lo,
        domain_0_hi,
        domain_1_lo,
        domain_1_hi,
        capacity,
        TILEDB_ZSTD,
        TILEDB_ROW_MAJOR,
        TILEDB_COL_MAJOR);
    CHECK(test_random_subarrays(domain_size_0, domain_size_1, ntests));
  }
}
