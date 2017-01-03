/**
 * @file   c_api_sparse_array_spec.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
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

#include "c_api_sparse_array_spec.h"
#include "progress_bar.h"
#include <cstring>
#include <iostream>
#include <map>
#include <time.h>
#include <sys/time.h>
#include <sstream>




/* ****************************** */
/*        GTEST FUNCTIONS         */
/* ****************************** */

void SparseArrayTestFixture::SetUp() {
  // Error code
  int rc;
 
  // Initialize context
  rc = tiledb_ctx_init(&tiledb_ctx_, NULL);
  ASSERT_EQ(rc, TILEDB_OK);

  // Create workspace
  rc = tiledb_workspace_create(tiledb_ctx_, WORKSPACE.c_str());
  ASSERT_EQ(rc, TILEDB_OK);
}

void SparseArrayTestFixture::TearDown() {
  // Error code
  int rc;

  // Finalize TileDB context
  rc = tiledb_ctx_finalize(tiledb_ctx_);
  ASSERT_EQ(rc, TILEDB_OK);

  // Remove the temporary workspace
  std::string command = "rm -rf ";
  command.append(WORKSPACE);
  rc = system(command.c_str());
  ASSERT_EQ(rc, 0);
}




/* ****************************** */
/*          PUBLIC METHODS        */
/* ****************************** */

int SparseArrayTestFixture::create_sparse_array_2D(
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

int* SparseArrayTestFixture::read_sparse_array_2D(
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

void SparseArrayTestFixture::set_array_name(const char *name) {
  array_name_ = WORKSPACE + name;
}

int SparseArrayTestFixture::write_sparse_array_unsorted_2D(
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



/**
 * Test is to randomly read subregions of the array and
 * check with corresponding value set by row_id*dim1+col_id
 * Top left corner is always 4,4
 * Test runs through 100 iterations to choose random
 * width and height of the subregions
 */
TEST_F(SparseArrayTestFixture, test_random_sparse_sorted_reads) {
  // Error code
  int rc;

  // Parameters used in this test
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
  int iter_num = 100;

  // Set array name
  set_array_name("sparse_test_5000x1000_100x100");

  // Create a progress bar
  ProgressBar* progress_bar = new ProgressBar();

  // Create a dense integer array
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
  ASSERT_EQ(rc, TILEDB_OK);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk
  rc = write_sparse_array_unsorted_2D(domain_size_0, domain_size_1);
  ASSERT_EQ(rc, TILEDB_OK);

  // Test random subarrays and check with corresponding value set by
  // row_id*dim1+col_id. Top left corner is always 4,4.
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

    // Read subarray
    int *buffer = read_sparse_array_2D(
                      d0_lo,
                      d0_hi,
                      d1_lo,
                      d1_hi,
                      TILEDB_ARRAY_READ_SORTED_ROW);
    ASSERT_TRUE(buffer != NULL);

    // Check
    for(int64_t i = d0_lo; i <= d0_hi; ++i) {
      for(int64_t j = d1_lo; j <= d1_hi; ++j) {
        ASSERT_EQ(buffer[index], i*domain_size_1+j);
        if (buffer[index] !=  (i*domain_size_1+j)) {
          std::cout << "mismatch: " << i
              << "," << j << "=" << buffer[index] << "!="
              << ((i*domain_size_1+j)) << "\n";
          return;
        }
        ++index;
      }
    }

    // Clean up
    delete [] buffer;

    // Update progress bar
    progress_bar->load(1.0/iter_num);
  } 

  // Delete progress bar
  delete progress_bar;
} 



