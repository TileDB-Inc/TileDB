/**
 * @file   unit-capi-densearray.cc
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

// #include "progress_bar.h"
#include <sys/time.h>
#include <cassert>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <sstream>
#include "tiledb.h"

#include "catch.hpp"

struct DenseArrayFx {
  // Workspace folder name
  const std::string WORKSPACE = ".__workspace/";

  // Array name
  std::string array_name_;

  // Array schema object under test
  TileDB_ArraySchema array_schema_;

  // TileDB context
  TileDB_CTX* tiledb_ctx_;

  DenseArrayFx() {
    // Reset the random number generator
    srand(0);

    // Error code
    int rc;

    // Initialize context
    rc = tiledb_ctx_init(&tiledb_ctx_, nullptr);
    assert(rc == TILEDB_OK);

    // Create workspace
    rc = tiledb_workspace_create(tiledb_ctx_, WORKSPACE.c_str());
    assert(rc == TILEDB_OK);
  }

  ~DenseArrayFx() {
    // Error code
    int rc;

    // Finalize TileDB context
    rc = tiledb_ctx_finalize(tiledb_ctx_);
    assert(rc == TILEDB_OK);

    // Remove the temporary workspace
    std::string command = "rm -rf ";
    command.append(WORKSPACE);
    rc = system(command.c_str());
    assert(rc == 0);
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
      const int64_t update_num) {
    // Initializations
    int l, r;
    int64_t cell_num = domain_size_0 * domain_size_1;

    // Check the contents of the buffers cell by cell
    for (int64_t i = 0; i < cell_num; ++i) {
      l = buffer_before[i];
      r = buffer_after[i];

      // If they are not the same, check if it is due to an update
      if (l != r) {
        bool found = false;
        for (int64_t k = 0; k < update_num; ++k) {
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
   * @param enable_compression If true, then GZIP compression is used.
   * @param cell_order The cell order.
   * @param tile_order The tile order.
   * @return TILEDB_OK on success and TILEDB_ERR on error.
   */
  int create_dense_array_2D(
      const int64_t tile_extent_0,
      const int64_t tile_extent_1,
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const int64_t capacity,
      const bool enable_compression,
      const int cell_order,
      const int tile_order) {
    // Error code
    int rc;

    // Prepare and set the array schema object and data structures
    const int attribute_num = 1;
    const char* attributes[] = {"ATTR_INT32"};
    const char* dimensions[] = {"X", "Y"};
    int64_t domain[] = {domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};
    int64_t tile_extents[] = {tile_extent_0, tile_extent_1};
    const int types[] = {TILEDB_INT32, TILEDB_INT64};
    int compression[2];
    const int dense = 1;

    if (!enable_compression) {
      compression[0] = TILEDB_NO_COMPRESSION;
      compression[1] = TILEDB_NO_COMPRESSION;
    } else {
      compression[0] = TILEDB_GZIP;
      compression[1] = TILEDB_GZIP;
    }

    // Set the array schema
    rc = tiledb_array_set_schema(
        &array_schema_,
        array_name_.c_str(),
        attributes,
        attribute_num,
        capacity,
        cell_order,
        nullptr,
        compression,
        dense,
        dimensions,
        2,
        domain,
        4 * sizeof(int64_t),
        tile_extents,
        2 * sizeof(int64_t),
        tile_order,
        types);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Create the array
    rc = tiledb_array_create(tiledb_ctx_, &array_schema_);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Free array schema
    rc = tiledb_array_free_schema(&array_schema_);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Success
    return TILEDB_OK;
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
    int* buffer = new int[domain_size_0 * domain_size_1];

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
    int** buffer = new int*[domain_size_0];

    // Populate buffer
    for (int64_t i = 0; i < domain_size_0; ++i) {
      buffer[i] = new int[domain_size_1];
      for (int64_t j = 0; j < domain_size_1; ++j) {
        buffer[i][j] = i * domain_size_1 + j;
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
      const int read_mode) {
    // Error code
    int rc;

    // Initialize a subarray
    const int64_t subarray[] = {
        domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};

    // Subset over a specific attribute
    const char* attributes[] = {"ATTR_INT32"};

    // Initialize the array in the input mode
    TileDB_Array* tiledb_array;
    rc = tiledb_array_init(
        tiledb_ctx_,
        &tiledb_array,
        array_name_.c_str(),
        read_mode,
        subarray,
        attributes,
        1);
    if (rc != TILEDB_OK)
      return nullptr;

    // Prepare the buffers that will store the result
    int64_t domain_size_0 = domain_0_hi - domain_0_lo + 1;
    int64_t domain_size_1 = domain_1_hi - domain_1_lo + 1;
    int64_t cell_num = domain_size_0 * domain_size_1;
    int* buffer_a1 = new int[cell_num];
    void* buffers[] = {buffer_a1};
    size_t buffer_size_a1 = cell_num * sizeof(int);
    size_t buffer_sizes[] = {buffer_size_a1};

    // Read from array
    rc = tiledb_array_read(tiledb_array, buffers, buffer_sizes);
    if (rc != TILEDB_OK) {
      delete[] buffer_a1;
      return nullptr;
    }

    // Finalize the array
    rc = tiledb_array_finalize(tiledb_array);
    if (rc != TILEDB_OK) {
      delete[] buffer_a1;
      return nullptr;
    }

    // Success - return the created buffer
    return buffer_a1;
  }

  /** Sets the array name for the current test. */
  void set_array_name(const char* name) {
    array_name_ = WORKSPACE + name;
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
      const size_t* buffer_sizes) {
    // Error code
    int rc;

    // For easy reference
    int* buffer_a1 = (int*)buffers[0];
    int64_t* buffer_coords = (int64_t*)buffers[1];

    // Specify attributes to be written
    const char* attributes[] = {"ATTR_INT32", TILEDB_COORDS};

    // Initialize the array
    TileDB_Array* tiledb_array;
    rc = tiledb_array_init(
        tiledb_ctx_,
        &tiledb_array,
        array_name_.c_str(),
        TILEDB_ARRAY_WRITE_UNSORTED,
        nullptr,
        attributes,
        2);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Populate buffers with random updates
    srand(seed);
    int64_t x, y, v;
    int64_t coords_index = 0L;
    std::map<std::string, int> my_map;
    std::map<std::string, int>::iterator it;
    my_map.clear();
    for (int64_t i = 0; i < update_num; ++i) {
      std::ostringstream rand_stream;
      do {
        std::ostringstream rand_stream;
        x = rand() % domain_size_0;
        y = rand() % domain_size_1;
        v = rand();
        rand_stream << x << "," << y;
        it = my_map.find(rand_stream.str());
      } while (it != my_map.end());
      rand_stream << x << "," << y;
      my_map[rand_stream.str()] = v;
      buffer_coords[coords_index++] = x;
      buffer_coords[coords_index++] = y;
      buffer_a1[i] = v;
    }

    // Write to array
    rc = tiledb_array_write(tiledb_array, (const void**)buffers, buffer_sizes);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Finalize the array
    rc = tiledb_array_finalize(tiledb_array);
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

    // Initialize the array
    TileDB_Array* tiledb_array;
    rc = tiledb_array_init(
        tiledb_ctx_,
        &tiledb_array,
        array_name_.c_str(),
        TILEDB_ARRAY_WRITE,
        nullptr,
        nullptr,
        0);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Other initializations
    int** buffer = generate_2D_buffer(domain_size_0, domain_size_1);
    int64_t cell_num_in_tile = tile_extent_0 * tile_extent_1;
    int* buffer_a1 = new int[cell_num_in_tile];
    for (int64_t i = 0; i < cell_num_in_tile; ++i)
      buffer_a1[i] = 0;
    const void* buffers[2];
    buffers[0] = buffer_a1;
    size_t buffer_sizes[2];
    int64_t index = 0L;
    size_t buffer_size = 0L;

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
        rc = tiledb_array_write(tiledb_array, buffers, buffer_sizes);
        if (rc != TILEDB_OK) {
          tiledb_array_finalize(tiledb_array);
          for (int64_t i = 0; i < domain_size_0; ++i)
            delete[] buffer[i];
          delete[] buffer;
          delete[] buffer_a1;
          return TILEDB_ERR;
        }
      }
    }

    // Finalize the array
    rc = tiledb_array_finalize(tiledb_array);

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
      int64_t* subarray, int write_mode, int* buffer, size_t* buffer_sizes) {
    // Error code
    int rc;

    // Attribute to focus on
    const char* attributes[] = {"ATTR_INT32"};

    // Initialize the array
    TileDB_Array* tiledb_array;
    rc = tiledb_array_init(
        tiledb_ctx_,
        &tiledb_array,
        array_name_.c_str(),
        write_mode,
        subarray,
        attributes,
        1);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Write to array
    const void* buffers[] = {buffer};
    rc = tiledb_array_write(tiledb_array, buffers, buffer_sizes);
    if (rc != TILEDB_OK)
      return TILEDB_ERR;

    // Finalize the array
    rc = tiledb_array_finalize(tiledb_array);

    // Return
    return rc;
  }
};

/**
 * Tests 10 random 2D subarrays and checks if the value of each cell is equal
 * to row_id*dim1+col_id. Top left corner is always 4,4.
 */
TEST_CASE_METHOD(DenseArrayFx, "test random dense sorted reads") {
  // Error code
  int rc;

  // Parameters used in this test
  int64_t domain_size_0 = 5000;
  int64_t domain_size_1 = 10000;
  int64_t tile_extent_0 = 100;
  int64_t tile_extent_1 = 100;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  int64_t capacity = 0;  // 0 means use default capacity
  int cell_order = TILEDB_ROW_MAJOR;
  int tile_order = TILEDB_ROW_MAJOR;
  int iter_num = 10;

  // Set array name
  set_array_name("dense_test_5000x10000_100x100");

  // Create a progress bar
  // ProgressBar* progress_bar = new ProgressBar();

  // Create a dense integer array
  rc = create_dense_array_2D(
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      false,
      cell_order,
      tile_order);
  REQUIRE(rc == TILEDB_OK);

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
    height = rand() % (domain_size_0 - d0_lo);
    width = rand() % (domain_size_1 - d1_lo);
    d0_hi = d0_lo + height;
    d1_hi = d1_lo + width;
    int64_t index = 0;

    // Read subarray
    int* buffer = read_dense_array_2D(
        d0_lo, d0_hi, d1_lo, d1_hi, TILEDB_ARRAY_READ_SORTED_ROW);
    REQUIRE(buffer != NULL);

    bool allok = true;
    // Check
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
    CHECK(allok);

    // Clean up
    delete[] buffer;

    // Update progress bar
    // progress_bar->load(1.0/iter_num);
  }

  // Delete progress bar
  // delete progress_bar;
}

/**
 * Tests random 2D subarray writes.
 */
TEST_CASE_METHOD(DenseArrayFx, "test random dense sorted writes") {
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
  int64_t capacity = 0;  // 0 means use default capacity
  int cell_order = TILEDB_ROW_MAJOR;
  int tile_order = TILEDB_ROW_MAJOR;
  int iter_num = 10;

  // Set array name
  set_array_name("dense_test_100x100_10x10");

  // Create a progress bar
  // ProgressBar* progress_bar = new ProgressBar();

  // Create a dense integer array
  rc = create_dense_array_2D(
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      false,
      cell_order,
      tile_order);
  REQUIRE(rc == TILEDB_OK);

  // Write random subarray, then read it back and check
  int64_t d0[2], d1[2];
  for (int i = 0; i < iter_num; ++i) {
    // Create subarray
    d0[0] = rand() % domain_size_0;
    d1[0] = rand() % domain_size_1;
    d0[1] = d0[0] + rand() % (domain_size_0 - d0[0]);
    d1[1] = d1[0] + rand() % (domain_size_1 - d1[0]);
    int64_t subarray[] = {d0[0], d0[1], d1[0], d1[1]};

    // Prepare buffers
    int64_t subarray_length[2] = {d0[1] - d0[0] + 1, d1[1] - d1[0] + 1};
    int64_t cell_num_in_subarray = subarray_length[0] * subarray_length[1];
    int* buffer = new int[cell_num_in_subarray];
    int64_t index = 0;
    size_t buffer_size = cell_num_in_subarray * sizeof(int);
    size_t buffer_sizes[] = {buffer_size};
    for (int64_t r = 0; r < subarray_length[0]; ++r)
      for (int64_t c = 0; c < subarray_length[1]; ++c)
        buffer[index++] = -(rand() % 999999);

    // Write 2D subarray
    rc = write_dense_subarray_2D(
        subarray, TILEDB_ARRAY_WRITE_SORTED_ROW, buffer, buffer_sizes);
    REQUIRE(rc == TILEDB_OK);

    // Read back the same subarray
    int* read_buffer = read_dense_array_2D(
        subarray[0],
        subarray[1],
        subarray[2],
        subarray[3],
        TILEDB_ARRAY_READ_SORTED_ROW);
    REQUIRE(read_buffer != NULL);

    // Check the two buffers
    bool allok = true;
    for (index = 0; index < cell_num_in_subarray; ++index) {
      if (buffer[index] != read_buffer[index]) {
        allok = false;
        break;
      }
    }
    CHECK(allok);

    // Clean up
    delete[] buffer;
    delete[] read_buffer;

    // Update progress bar
    // progress_bar->load(1.0/iter_num);
  }

  // Delete progress bar
  // delete progress_bar;
}

/**
 * Test random updates in a 2D dense array.
 */
TEST_CASE_METHOD(DenseArrayFx, "test random dense updates") {
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
  int64_t capacity = 0;  // 0 means use default capacity
  int cell_order = TILEDB_ROW_MAJOR;
  int tile_order = TILEDB_ROW_MAJOR;
  int64_t update_num = 100;
  int seed = 7;

  // Set array name
  set_array_name("dense_test_100x100_10x10");

  // Create a dense integer array
  rc = create_dense_array_2D(
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      false,
      cell_order,
      tile_order);
  REQUIRE(rc == TILEDB_OK);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk tile by tile
  rc = write_dense_array_by_tiles(
      domain_size_0, domain_size_1, tile_extent_0, tile_extent_1);
  REQUIRE(rc == TILEDB_OK);

  // Read the entire array back to memory
  int* before_update = read_dense_array_2D(
      domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi, TILEDB_ARRAY_READ);
  REQUIRE(before_update != NULL);

  // Prepare random updates
  int* buffer_a1 = new int[update_num];
  int64_t* buffer_coords = new int64_t[2 * update_num];
  void* buffers[] = {buffer_a1, buffer_coords};
  size_t buffer_sizes[2];
  buffer_sizes[0] = update_num * sizeof(int);
  buffer_sizes[1] = 2 * update_num * sizeof(int64_t);

  rc = update_dense_array_2D(
      domain_size_0, domain_size_1, update_num, seed, buffers, buffer_sizes);
  REQUIRE(rc == TILEDB_OK);

  // Read the entire array back to memory after update
  int* after_update = read_dense_array_2D(
      domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi, TILEDB_ARRAY_READ);
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
  CHECK(success);

  // Clean up
  delete[] before_update;
  delete[] after_update;
  delete[] buffer_a1;
  delete[] buffer_coords;
}
