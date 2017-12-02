/**
 * @file   unit-capi-dense_array.cc
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
 * Tests of C API for dense array operations.
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

struct DenseArrayFx {
  // Constant parameters
  const char* ATTR_NAME = "a";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const char* DIM1_NAME = "x";
  const char* DIM2_NAME = "y";
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;

  // Group folder name
#ifdef HAVE_HDFS
  const std::string URI_PREFIX = "hdfs://";
  const std::string TEMP_DIR = "/tiledb_test/";
#else
  const std::string URI_PREFIX = "file://";
  const std::string TEMP_DIR = tiledb::posix::current_dir() + "/";
#endif
  const std::string GROUP = "my_group/";

  // Array name
  std::string array_name_;

  // Array metadata object under test
  tiledb_array_metadata_t* array_metadata_;

  // TileDB context
  tiledb_ctx_t* ctx_;

  DenseArrayFx() {
    // Reset the random number generator
    std::srand(0);

    // Initialize context
    int rc = tiledb_ctx_create(&ctx_);
    if (rc != TILEDB_OK) {
      std::cerr << "DenseArrayFx() Error creating tiledb_ctx_t" << std::endl;
      std::exit(1);
    }

    // Create group, delete it if it already exists
    if (dir_exists(TEMP_DIR + GROUP)) {
      bool success = remove_dir(TEMP_DIR + GROUP);
      if (!success) {
        std::cerr << "DenseArrayFx() Error deleting existing test group"
                  << std::endl;
        std::exit(1);
      }
    }
    rc = tiledb_group_create(ctx_, (URI_PREFIX + TEMP_DIR + GROUP).c_str());
    if (rc != TILEDB_OK) {
      std::cerr << "DenseArrayFx() Error creating test group" << std::endl;
      std::exit(1);
    }
  }

  ~DenseArrayFx() {
    // Finalize TileDB context
    tiledb_ctx_free(ctx_);

    // Remove the temporary group
    bool success = remove_dir(TEMP_DIR + GROUP);
    if (!success) {
      std::cerr << "DenseArrayFx() Error deleting test group" << std::endl;
      std::exit(1);
    }
  }

  bool dir_exists(std::string path) {
#ifdef HAVE_HDFS
    std::string cmd = std::string("hadoop fs -test -d ") + path;
#else
    std::string cmd = std::string("test -d ") + path;
#endif
    return (system(cmd.c_str()) == 0);
  }

  bool remove_dir(std::string path) {
#ifdef HAVE_HDFS
    std::string cmd = std::string("hadoop fs -rm -r -f ") + path;
#else
    std::string cmd = std::string("rm -r -f ") + path;
#endif
    return (system(cmd.c_str()) == 0);
  }

  /**
   * Checks two buffers, one before and one after the updates. The updates
   * are given as function inputs and facilitate the check.
   *
   * @param buffer_before The buffer before the updates.
   * @param buffer_after The buffer after the updates.
   * @param buffer_updates_a1 The updated attribute values.
   * @param buffer_updates_coords The coordinates where the updates occurred.
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @param update_num The number of updates.
   * @return True on success and false on error.
   */
  static bool check_buffer_after_updates(
      const int* buffer_before,
      const int* buffer_after,
      const int* buffer_updates_a1,
      const int64_t* buffer_updates_coords,
      const int64_t domain_size_0,
      const int64_t domain_size_1,
      const uint64_t update_num) {
    // Initializations
    int l, r;
    uint64_t cell_num = (uint64_t)domain_size_0 * domain_size_1;

    // Check the contents of the buffers cell by cell
    for (uint64_t i = 0; i < cell_num; ++i) {
      l = buffer_before[i];
      r = buffer_after[i];

      // If they are not the same, check if it is due to an update
      if (l != r) {
        bool found = false;
        for (uint64_t k = 0; k < update_num; ++k) {
          // The difference is due to an update
          if (r == buffer_updates_a1[k] &&
              (l / domain_size_1) == buffer_updates_coords[2 * k] &&
              (l % domain_size_1) == buffer_updates_coords[2 * k + 1]) {
            found = true;
            break;
          }
        }

        // The difference is not due to an update
        if (!found)
          return false;
      }
    }

    // Success
    return true;
  }

  /**
   * Creates a 2D dense array.
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
  void create_dense_array_2D(
      const int64_t tile_extent_0,
      const int64_t tile_extent_1,
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const uint64_t capacity,
      const tiledb_layout_t cell_order,
      const tiledb_layout_t tile_order) {
    // Error code
    int rc;

    // Prepare and set the array metadata object and data structures
    int64_t dim_domain[] = {domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};

    // Create attribute
    tiledb_attribute_t* a;
    rc = tiledb_attribute_create(ctx_, &a, ATTR_NAME, ATTR_TYPE);
    REQUIRE(rc == TILEDB_OK);
    //    rc = tiledb_attribute_set_compressor(ctx_, a, TILEDB_GZIP, -1);
    //    REQUIRE(rc == TILEDB_OK);

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

    // Create array metadata
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
    rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata_, a);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_metadata_set_domain(ctx_, array_metadata_, domain);
    REQUIRE(rc == TILEDB_OK);

    // Create the array
    rc = tiledb_array_create(ctx_, array_metadata_);
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
    rc = tiledb_array_metadata_free(ctx_, array_metadata_);
    REQUIRE(rc == TILEDB_OK);
  }

  /**
   * Generates a 1D buffer containing the cell values of a 2D array.
   * Each cell value equals (row index * total number of columns + col index).
   *
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @return The created buffer of size domain_size_0*domain_size_1 integers.
   *     Note that the function creates the buffer with 'new'. Make sure
   *     to delete the returned buffer in the caller function.
   */
  int* generate_1D_int_buffer(
      const int64_t domain_size_0, const int64_t domain_size_1) {
    // Create buffer
    auto buffer = new int[domain_size_0 * domain_size_1];

    // Populate buffer
    for (int64_t i = 0; i < domain_size_0; ++i)
      for (int64_t j = 0; j < domain_size_1; ++j)
        buffer[i * domain_size_1 + j] = i * domain_size_1 + j;

    // Return
    return buffer;
  }

  /**
   * Generates a 2D buffer containing the cell values of a 2D array.
   * Each cell value equals (row index * total number of columns + col index).
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @return The created 2D buffer. Note that the function creates the buffer
   *     with 'new'. Make sure to delete the returned buffer in the caller
   *     function.
   */
  int** generate_2D_buffer(
      const int64_t domain_size_0, const int64_t domain_size_1) {
    // Create buffer
    auto buffer = new int*[domain_size_0];

    // Populate buffer
    for (int64_t i = 0; i < domain_size_0; ++i) {
      buffer[i] = new int[domain_size_1];
      for (int64_t j = 0; j < domain_size_1; ++j) {
        buffer[i][j] = (int)(i * domain_size_1 + j);
      }
    }

    // Return
    return buffer;
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
  int* read_dense_array_2D(
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
    rc = tiledb_query_create(ctx_, &query, array_name_.c_str(), query_type);
    if (rc != TILEDB_OK)
      return nullptr;
    rc = tiledb_query_set_buffers(
        ctx_, query, attributes, 1, buffers, buffer_sizes);
    if (rc != TILEDB_OK)
      return nullptr;
    rc = tiledb_query_by_subarray(ctx_, query, subarray, TILEDB_INT64);
    if (rc != TILEDB_OK)
      return nullptr;
    rc = tiledb_query_set_layout(ctx_, query, query_layout);
    if (rc != TILEDB_OK)
      return nullptr;

    // Read from array
    rc = tiledb_query_submit(ctx_, query);
    REQUIRE(rc == TILEDB_OK);
    if (rc != TILEDB_OK) {
      delete[] buffer_a1;
      return nullptr;
    }

    // Free/finalize query
    rc = tiledb_query_free(ctx_, query);
    REQUIRE(rc == TILEDB_OK);
    if (rc != TILEDB_OK) {
      delete[] buffer_a1;
      return nullptr;
    }

    // Success - return the created buffer
    return buffer_a1;
  }

  /** Sets the array name for the current test. */
  void set_array_name(const char* name) {
    array_name_ = URI_PREFIX + TEMP_DIR + GROUP + name;
  }

  /**
   * Updates random locations in a dense array with the input domain sizes.
   *
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @param udpate_num The number of updates to be performed.
   * @param seed The seed for the random generator.
   * @param buffers The buffers to be dispatched to the write command.
   * @param buffer_sizes The buffer sizes to be dispatched to the write command.
   * @return TILEDB_OK on success and TILEDB_ERR on error.
   */
  int update_dense_array_2D(
      const int64_t domain_size_0,
      const int64_t domain_size_1,
      int64_t update_num,
      int seed,
      void** buffers,
      uint64_t* buffer_sizes) {
    // Error code
    int rc;

    // For easy reference
    auto buffer_a1 = (int*)buffers[0];
    auto buffer_coords = (int64_t*)buffers[1];

    // Specify attributes to be written
    const char* attributes[] = {ATTR_NAME, TILEDB_COORDS};

    // Populate buffers with random updates
    std::srand(seed);
    int64_t x, y, v;
    int64_t coords_index = 0L;
    std::map<std::string, int> my_map;
    std::map<std::string, int>::iterator it;
    my_map.clear();
    for (int64_t i = 0; i < update_num; ++i) {
      std::ostringstream rand_stream;
      do {
        std::ostringstream rand_stream;
        x = std::rand() % domain_size_0;
        y = std::rand() % domain_size_1;
        v = std::rand();
        rand_stream << x << "," << y;
        it = my_map.find(rand_stream.str());
      } while (it != my_map.end());
      rand_stream << x << "," << y;
      my_map[rand_stream.str()] = v;
      buffer_coords[coords_index++] = x;
      buffer_coords[coords_index++] = y;
      buffer_a1[i] = v;
    }

    // Create query
    tiledb_query_t* query;
    rc = tiledb_query_create(ctx_, &query, array_name_.c_str(), TILEDB_WRITE);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;
    rc = tiledb_query_set_buffers(
        ctx_, query, attributes, 2, buffers, buffer_sizes);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;
    rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
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

    // Success
    return TILEDB_OK;
  }

  /**
   * Write to a 2D dense array tile by tile. The buffer is initialized
   * with row_id*domain_size_1+col_id values.
   *
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @param tile_extent_0 The tile extent along the first dimension.
   * @param tile_extent_1 The tile extent along the second dimension.
   * @return TILEDB_OK on success and TILEDB_ERR on error.
   */
  int write_dense_array_by_tiles(
      const int64_t domain_size_0,
      const int64_t domain_size_1,
      const int64_t tile_extent_0,
      const int64_t tile_extent_1) {
    // Error code
    int rc;

    // Other initializations
    auto buffer = generate_2D_buffer(domain_size_0, domain_size_1);
    int64_t cell_num_in_tile = tile_extent_0 * tile_extent_1;
    auto buffer_a1 = new int[cell_num_in_tile];
    for (int64_t i = 0; i < cell_num_in_tile; ++i)
      buffer_a1[i] = 0;
    void* buffers[2];
    buffers[0] = buffer_a1;
    uint64_t buffer_sizes[2];
    int64_t index = 0L;
    uint64_t buffer_size = 0L;

    const char* attributes[] = {ATTR_NAME};

    // Create query
    tiledb_query_t* query;
    rc = tiledb_query_create(ctx_, &query, array_name_.c_str(), TILEDB_WRITE);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;
    rc = tiledb_query_set_buffers(
        ctx_, query, attributes, 1, buffers, buffer_sizes);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;
    rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Populate and write tile by tile
    for (int64_t i = 0; i < domain_size_0; i += tile_extent_0) {
      for (int64_t j = 0; j < domain_size_1; j += tile_extent_1) {
        int64_t tile_rows = ((i + tile_extent_0) < domain_size_0) ?
                                tile_extent_0 :
                                (domain_size_0 - i);
        int64_t tile_cols = ((j + tile_extent_1) < domain_size_1) ?
                                tile_extent_1 :
                                (domain_size_1 - j);
        int64_t k, l;
        for (k = 0; k < tile_rows; ++k) {
          for (l = 0; l < tile_cols; ++l) {
            index = uint64_t(k * tile_cols + l);
            buffer_a1[index] = buffer[uint64_t(i + k)][uint64_t(j + l)];
          }
        }
        buffer_size = k * l * sizeof(int);
        buffer_sizes[0] = {buffer_size};

        //    tiledb_query_reset_buffers(ctx_, query, buffers, buffer_sizes);
        rc = tiledb_query_submit(ctx_, query);

        if (rc != TILEDB_OK) {
          tiledb_query_free(ctx_, query);
          for (int64_t i = 0; i < domain_size_0; ++i)
            delete[] buffer[i];
          delete[] buffer;
          delete[] buffer_a1;
          return TILEDB_ERR;
        }
      }
    }

    // Finalize the query
    rc = tiledb_query_free(ctx_, query);

    // Clean up
    for (int64_t i = 0; i < domain_size_0; ++i)
      delete[] buffer[i];
    delete[] buffer;
    delete[] buffer_a1;

    // Return
    return rc;
  }

  /**
   * Writes a 2D dense subarray.
   *
   * @param subarray The subarray to focus on, given as a vector of low, high
   *     values.
   * @param write_mode The write mode.
   * @param buffer The attribute buffer to be populated and written.
   * @param buffer_sizes The buffer sizes to be dispatched to the write command.
   * @return TILEDB_OK on success and TILEDB_ERR on error.
   */
  int write_dense_subarray_2D(
      int64_t* subarray,
      tiledb_query_type_t query_type,
      tiledb_layout_t query_layout,
      int* buffer,
      uint64_t* buffer_sizes) {
    // Error code
    int rc;

    // Attribute to focus on and buffers
    const char* attributes[] = {ATTR_NAME};
    void* buffers[] = {buffer};

    // Create query
    tiledb_query_t* query;
    rc = tiledb_query_create(ctx_, &query, array_name_.c_str(), query_type);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;
    rc = tiledb_query_set_buffers(
        ctx_, query, attributes, 1, buffers, buffer_sizes);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;
    rc = tiledb_query_by_subarray(ctx_, query, subarray, TILEDB_INT64);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;
    rc = tiledb_query_set_layout(ctx_, query, query_layout);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Submit query
    rc = tiledb_query_submit(ctx_, query);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Free/finalize query
    rc = tiledb_query_free(ctx_, query);

    // Return
    return rc;
  }
};

/**
 * Tests 10 random 2D subarrays and checks if the value of each cell is equal
 * to row_id*dim1+col_id. Top left corner is always 4,4.
 */
TEST_CASE_METHOD(
    DenseArrayFx, "C API: Test random dense sorted reads", "[dense]") {
  // Error code
  int rc;

  // Parameters used in this test
  int64_t domain_size_0 = 5000;
  int64_t domain_size_1 = 10000;
  int64_t tile_extent_0 = 1000;
  int64_t tile_extent_1 = 1000;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  int iter_num = 10;

  // Set array name
  set_array_name("dense_test_5000x10000_100x100");

  // Create a dense integer array
  create_dense_array_2D(
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk tile by tile
  rc = write_dense_array_by_tiles(
      domain_size_0, domain_size_1, tile_extent_0, tile_extent_1);
  REQUIRE(rc == TILEDB_OK);

  // Test random subarrays and check with corresponding value set by
  // row_id*dim1+col_id. Top left corner is always 4,4.
  int64_t d0_lo = 4;
  int64_t d0_hi = 0;
  int64_t d1_lo = 4;
  int64_t d1_hi = 0;
  int64_t height = 0, width = 0;

  for (int iter = 0; iter < iter_num; ++iter) {
    height = std::rand() % (domain_size_0 - d0_lo);
    width = std::rand() % (domain_size_1 - d1_lo);
    d0_hi = d0_lo + height;
    d1_hi = d1_lo + width;
    int64_t index = 0;

    // Read subarray
    int* buffer = read_dense_array_2D(
        d0_lo, d0_hi, d1_lo, d1_hi, TILEDB_READ, TILEDB_ROW_MAJOR);
    REQUIRE(buffer != NULL);

    bool allok = true;
    // Check
    for (int64_t i = d0_lo; i <= d0_hi; ++i) {
      for (int64_t j = d1_lo; j <= d1_hi; ++j) {
        bool match = (buffer[index] == i * domain_size_1 + j);
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
    REQUIRE(allok);

    // Clean up
    delete[] buffer;
  }
}

/**
 * Tests random 2D subarray writes.
 */

TEST_CASE_METHOD(
    DenseArrayFx, "C API: Test random dense sorted writes", "[dense]") {
  // Error code
  int rc;

  // Parameters used in this test
  int64_t domain_size_0 = 100;
  int64_t domain_size_1 = 100;
  int64_t tile_extent_0 = 10;
  int64_t tile_extent_1 = 10;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  int iter_num = 10;

  // Set array name
  set_array_name("dense_test_100x100_10x10");

  // Create a dense integer array
  create_dense_array_2D(
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  // Write random subarray, then read it back and check
  int64_t d0[2], d1[2];
  for (int i = 0; i < iter_num; ++i) {
    // Create subarray
    d0[0] = std::rand() % domain_size_0;
    d1[0] = std::rand() % domain_size_1;
    d0[1] = d0[0] + std::rand() % (domain_size_0 - d0[0]);
    d1[1] = d1[0] + std::rand() % (domain_size_1 - d1[0]);
    int64_t subarray[] = {d0[0], d0[1], d1[0], d1[1]};

    // Prepare buffers
    int64_t subarray_length[2] = {d0[1] - d0[0] + 1, d1[1] - d1[0] + 1};
    int64_t cell_num_in_subarray = subarray_length[0] * subarray_length[1];
    auto buffer = new int[cell_num_in_subarray];
    int64_t index = 0;
    uint64_t buffer_size = cell_num_in_subarray * sizeof(int);
    uint64_t buffer_sizes[] = {buffer_size};
    for (int64_t r = 0; r < subarray_length[0]; ++r)
      for (int64_t c = 0; c < subarray_length[1]; ++c)
        buffer[index++] = -(std::rand() % 999999);

    // Write 2D subarray
    rc = write_dense_subarray_2D(
        subarray, TILEDB_WRITE, TILEDB_ROW_MAJOR, buffer, buffer_sizes);
    REQUIRE(rc == TILEDB_OK);

    // Read back the same subarray
    int* read_buffer = read_dense_array_2D(
        subarray[0],
        subarray[1],
        subarray[2],
        subarray[3],
        TILEDB_READ,
        TILEDB_ROW_MAJOR);
    REQUIRE(read_buffer != NULL);

    // Check the two buffers
    bool allok = true;
    for (index = 0; index < cell_num_in_subarray; ++index) {
      if (buffer[index] != read_buffer[index]) {
        allok = false;
        break;
      }
    }
    REQUIRE(allok);

    // Clean up
    delete[] buffer;
    delete[] read_buffer;
  }
}

/**
 * Test random updates in a 2D dense array.
 */
TEST_CASE_METHOD(DenseArrayFx, "C API: Test random dense updates", "[dense]") {
  // Error code
  int rc;

  // Parameters used in this test
  int64_t domain_size_0 = 100;
  int64_t domain_size_1 = 100;
  int64_t tile_extent_0 = 10;
  int64_t tile_extent_1 = 10;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  int64_t update_num = 100;
  int seed = 7;

  // Set array name
  set_array_name("dense_test_100x100_10x10");

  // Create a dense integer array
  create_dense_array_2D(
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk tile by tile
  rc = write_dense_array_by_tiles(
      domain_size_0, domain_size_1, tile_extent_0, tile_extent_1);
  REQUIRE(rc == TILEDB_OK);

  // Read the entire array back to memory
  int* before_update = read_dense_array_2D(
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      TILEDB_READ,
      TILEDB_GLOBAL_ORDER);
  REQUIRE(before_update != NULL);

  // Prepare random updates
  auto buffer_a1 = new int[update_num];
  auto buffer_coords = new int64_t[2 * update_num];
  void* buffers[] = {buffer_a1, buffer_coords};
  uint64_t buffer_sizes[2];
  buffer_sizes[0] = update_num * sizeof(int);
  buffer_sizes[1] = 2 * update_num * sizeof(int64_t);

  rc = update_dense_array_2D(
      domain_size_0, domain_size_1, update_num, seed, buffers, buffer_sizes);
  REQUIRE(rc == TILEDB_OK);

  // Read the entire array back to memory after update
  int* after_update = read_dense_array_2D(
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      TILEDB_READ,
      TILEDB_GLOBAL_ORDER);
  REQUIRE(after_update != NULL);

  // Compare array before and after
  bool success = check_buffer_after_updates(
      before_update,
      after_update,
      buffer_a1,
      buffer_coords,
      domain_size_0,
      domain_size_1,
      update_num);
  REQUIRE(success);

  // Clean up
  delete[] before_update;
  delete[] after_update;
  delete[] buffer_a1;
  delete[] buffer_coords;
}
