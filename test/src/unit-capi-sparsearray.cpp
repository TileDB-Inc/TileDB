/**
 * @file   unit-capi-sparsearray.cpp
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
#include "tiledb.h"
#include "progress_bar.h"

#include <cstring>
#include <iostream>
#include <map>
#include <cassert>
#include <time.h>
#include <sys/time.h>
#include <sstream>

#include "catch.hpp"

struct SparseArrayFx {
  
  // Workspace folder name
  const std::string WORKSPACE = ".__workspace/";

  // Array name 
  std::string array_name_;

  // Array schema object under test
  TileDB_ArraySchema array_schema_;
  
  // TileDB context
  TileDB_CTX* tiledb_ctx_;
  
  SparseArrayFx() {
    // Error code
    int rc;
 
    // Initialize context
    rc = tiledb_ctx_init(&tiledb_ctx_, NULL);
    assert(rc == TILEDB_OK);

    // Create workspace
    rc = tiledb_workspace_create(tiledb_ctx_, WORKSPACE.c_str());
    assert(rc == TILEDB_OK);
  }

  ~SparseArrayFx() {
    // Error code
    int rc;

    // Finalize TileDB context
    rc = tiledb_ctx_finalize(tiledb_ctx_);
    assert(rc ==TILEDB_OK);

    // Remove the temporary workspace
    std::string command = "rm -rf ";
    command.append(WORKSPACE);
    rc = system(command.c_str());
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
   * @param enable_compression If true, then GZIP compression is used.
   * @param cell_order The cell order.
   * @param tile_order The tile order.
   * @return TILEDB_OK on success and TILEDB_ERR on error.
   */
  int create_sparse_array_2D(
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
    const char* attributes[] = { "ATTR_INT32" };
    const char* dimensions[] = { "X", "Y" };
    int64_t domain[] = { domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi };
    int64_t tile_extents[] = { tile_extent_0, tile_extent_1 };
    const int types[] = { TILEDB_INT32, TILEDB_INT64 };
    int compression[2];
    const int dense = 0;

    if(!enable_compression) {
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
           NULL,
           compression,
           dense,
           dimensions,
           2,
           domain,
           4*sizeof(int64_t),
           tile_extents,
           2*sizeof(int64_t),
           tile_order,
           types);
    if(rc != TILEDB_OK)
      return TILEDB_ERR;

    // Create the array
    rc = tiledb_array_create(tiledb_ctx_, &array_schema_);
    if(rc != TILEDB_OK)
      return TILEDB_ERR;

    // Free array schema
    rc = tiledb_array_free_schema(&array_schema_);
    if(rc != TILEDB_OK)
      return TILEDB_ERR;
  
    // Success
    return TILEDB_OK;
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
      const int read_mode) {
    // Error code
    int rc;

    // Initialize a subarray
    const int64_t subarray[] = {
        domain_0_lo, 
        domain_0_hi, 
        domain_1_lo, 
        domain_1_hi
    };

    // Subset over a specific attribute
    const char* attributes[] = { "ATTR_INT32" };

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
    if(rc != TILEDB_OK) 
      return NULL;

    // Prepare the buffers that will store the result
    int64_t domain_size_0 = domain_0_hi - domain_0_lo + 1;
    int64_t domain_size_1 = domain_1_hi - domain_1_lo + 1;
    int64_t cell_num = domain_size_0 * domain_size_1;
    int* buffer_a1 = new int[cell_num];
    assert(buffer_a1);
    void* buffers[] = { buffer_a1 };
    size_t buffer_size_a1 = cell_num * sizeof(int);
    size_t buffer_sizes[] = { buffer_size_a1 };

    // Read from array
    rc = tiledb_array_read(tiledb_array, buffers, buffer_sizes);
    if(rc != TILEDB_OK) {
      tiledb_array_finalize(tiledb_array);
      return NULL;
    }

    // Finalize the array
    rc = tiledb_array_finalize(tiledb_array);
    if(rc != TILEDB_OK) 
      return NULL;

    // Success - return the created buffer
    return buffer_a1;
  }

  /** Sets the array name for the current test. */
  void set_array_name(const char *name) {
    array_name_ = WORKSPACE + name;
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
      const int64_t domain_size_0,
      const int64_t domain_size_1) {
    // Error code
    int rc;

    // Generate random attribute values and coordinates for sparse write
    int64_t cell_num = domain_size_0*domain_size_1;
    int* buffer_a1 = new int[cell_num];
    int64_t* buffer_coords = new int64_t[2*cell_num];
    int64_t coords_index = 0L;
    for (int64_t i = 0; i < domain_size_0; ++i) {
      for (int64_t j = 0; j < domain_size_1; ++j) {
        buffer_a1[i*domain_size_1+j] = i*domain_size_1+j;
        buffer_coords[2*coords_index] = i;
        buffer_coords[2*coords_index+1] = j;
        coords_index++;
      }
    }

    // Initialize the array 
    TileDB_Array* tiledb_array;
    rc = tiledb_array_init(
             tiledb_ctx_,
             &tiledb_array,
             array_name_.c_str(),
             TILEDB_ARRAY_WRITE_UNSORTED,
             NULL,
             NULL,
             0);  
    if(rc != TILEDB_OK)
      return TILEDB_ERR;

    // Write to array
    const void* buffers[] = { buffer_a1, buffer_coords };
    size_t buffer_sizes[2];
    buffer_sizes[0] = cell_num*sizeof(int);
    buffer_sizes[1] = 2*cell_num*sizeof(int64_t);
    rc = tiledb_array_write(tiledb_array, buffers, buffer_sizes);
    if(rc != TILEDB_OK)
      return TILEDB_ERR;

    // Finalize the array
    rc = tiledb_array_finalize(tiledb_array);
    if(rc != TILEDB_OK)
      return TILEDB_ERR;

    // Clean up
    delete [] buffer_a1;
    delete [] buffer_coords;

    // Success
    return TILEDB_OK;
  } 
};  


/**
 * test is to randomly read subregions of the array and
 * check with corresponding value set by row_id*dim1+col_id
 * top left corner is always 4,4
 * test runs through 10 iterations to choose random
 * width and height of the subregions
 */
TEST_CASE_METHOD(SparseArrayFx, "Test random sparse sorted reads") {
  // error code
  int rc;

  // parameters used in this test
  int64_t domain_size_0 = 5000;
  int64_t domain_size_1 = 1000;
  int64_t tile_extent_0 = 100;
  int64_t tile_extent_1 = 100;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0-1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1-1;
  int64_t capacity = 0; // 0 means use default capacity
  int cell_order = TILEDB_ROW_MAJOR; 
  int tile_order = TILEDB_ROW_MAJOR;
  int iter_num = 10;

  // set array name
  set_array_name("sparse_test_5000x1000_100x100");

  // create a progress bar
  ProgressBar* progress_bar = new ProgressBar();

  // create a dense integer array
  rc = create_sparse_array_2D(
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

  // write array cells with value = row id * columns + col id
  // to disk
  rc = write_sparse_array_unsorted_2D(domain_size_0, domain_size_1);
  REQUIRE(rc == TILEDB_OK);

  // test random subarrays and check with corresponding value set by
  // row_id*dim1+col_id. top left corner is always 4,4.
  int64_t d0_lo = 4;
  int64_t d0_hi = 0;
  int64_t d1_lo = 4;
  int64_t d1_hi = 0;
  int64_t height = 0, width = 0;

  for(int iter = 0; iter < iter_num; ++iter) {
    height = rand() % (domain_size_0 - d0_lo);
    width = rand() % (domain_size_1 - d1_lo);
    d0_hi = d0_lo + height;
    d1_hi = d1_lo + width;
    int64_t index = 0;

    // read subarray
    int *buffer = read_sparse_array_2D(
                      d0_lo,
                      d0_hi,
                      d1_lo,
                      d1_hi,
                      TILEDB_ARRAY_READ_SORTED_ROW);
    REQUIRE(buffer != NULL);

    // check
    bool allok = true;
    for(int64_t i = d0_lo; i <= d0_hi; ++i) {
      for(int64_t j = d1_lo; j <= d1_hi; ++j) {
        bool match = buffer[index] == i*domain_size_1+j;
        if (!match) {
          allok = false;
          std::cout << "mismatch: " << i
              << "," << j << "=" << buffer[index] << "!="
              << ((i*domain_size_1+j)) << "\n";
          break;
        }
        ++index;
      }
      if (!allok)
        break;
    }
    CHECK(allok);

    // clean up
    delete [] buffer;
    
    // update progress bar
    progress_bar->load(1.0/iter_num);
  } 

  delete progress_bar;
} 
