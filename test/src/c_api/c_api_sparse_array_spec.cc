/**
 * Copyright (c) 2016  Massachusetts Institute of Technology and Intel Corp.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
 * THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * Tests of C API for read/write/update operations for
 * sparse arrays
 */

#include <gtest/gtest.h>
#include "c_api.h"
#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <cstring>
#include <sstream>
#include <map>

class SparseArrayTestFixture: public testing::Test {
  const std::string WORKSPACE = ".__workspace/";
  const std::string ARRAY_100x100 = "sparse_test_100x100_10x10";
  const int ARRAY_RANK_2D = 2;

public:
  // Array schema object under test
  TileDB_ArraySchema schema;
  // TileDB context
  TileDB_CTX* tiledb_ctx;
  // Array name is initialized with the workspace folder
  std::string arrayName;

  /**
   * Create the test dense array with given tile extents
   */
  int create_sparse_array_2D(
          const long dim0_tile_extent,
          const long dim1_tile_extent,
          const long dim0_lo,
          const long dim0_hi,
          const long dim1_lo,
          const long dim1_hi,
          const int capacity,
          int cell_order,
          int tile_order,
          const bool enable_compression);

  /**
   * Load the array in a sorted row-major manner using
   * a buffer which is ordered in the global cell order.
   * The buffer is initialized cell values as
   * row_id*DIM1+col_id values. Tile extents or chunk
   * sizes are defined in the create_sparse_array_2D
   */
  int write_sparse_array_unsorted_2D(
          const int64_t dim0,
          const int64_t dim1);

  /**
   * Read cell values of a sparse array for a given
   * range and test whether it matches the value
   * row_id*DIM1+col_id
   */
  int * read_sparse_array_2D(
            const int64_t dim0_lo,
            const int64_t dim0_hi,
            const int64_t dim1_lo,
            const int64_t dim1_hi,
            const int read_mode);

  /**
   * Default constructor used to create a temporary
   * TileDB workspace in the current working directory
   * before all tests are called. User must have write
   * permissions to this directory
   */
  SparseArrayTestFixture();

  /**
   * Default destructor removes the temporary
   * TileDB workspace and destroys the TileDB
   * context
   */
  ~SparseArrayTestFixture();

  /**
   * Code here will be called immediately after the constructor (right
   * before each test).
   */
  virtual void SetUp() {
    // do Nothing
  }

  /**
   * Code here will be called immediately after each test
   * (right before the destructor).
   */
  virtual void TearDown();

  /**
   * Sets the object member array name
   * For now each test creates its own
   * array. Later, we can share this
   * across multiple tests
   */
  void setArrayName(const char *name);

};  // end of SparseArrayTestFixture

SparseArrayTestFixture::SparseArrayTestFixture() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_init(&tiledb_ctx, NULL);
  if (tiledb_workspace_create(tiledb_ctx, WORKSPACE.c_str())
      != TILEDB_OK) {
    exit(EXIT_FAILURE);
  }
}

SparseArrayTestFixture::~SparseArrayTestFixture() {
  // Finalize TileDB context
  tiledb_ctx_finalize(tiledb_ctx);

  // Remove the temporary workspace
  std::string command = "rm -rf ";
  command.append(WORKSPACE);
  int rc = system(command.c_str());
  assert(rc == 0);
}

void SparseArrayTestFixture::TearDown() {
  // delete currently tested array
  if (arrayName.empty()) return;

  tiledb_delete(
      tiledb_ctx,
      arrayName.c_str());
  arrayName.clear();
}

int SparseArrayTestFixture::create_sparse_array_2D(
  const long dim0_tile_extent,
  const long dim1_tile_extent,
  const long dim0_lo,
  const long dim0_hi,
  const long dim1_lo,
  const long dim1_hi,
  const int capacity,
  const int cell_order,
  const int tile_order,
  const bool enable_compression) {

  // Prepare and set the array schema object and data structures
  const int attribute_num = 1;
  const char* attributes[] = { "ATTR_INT32" };
  const char* dimensions[] = { "X", "Y" };
  int64_t domain[] = { dim0_lo, dim0_hi, dim1_lo, dim1_hi };
  int64_t tile_extents[] = { dim0_tile_extent, dim1_tile_extent };
  const int types[] = { TILEDB_INT32, TILEDB_INT64 };
  int compression[sizeof(dimensions)];

  if (!enable_compression) {
    compression[0] = TILEDB_NO_COMPRESSION;
    compression[1] = TILEDB_NO_COMPRESSION;
  } else {
    compression[0] = TILEDB_GZIP;
    compression[1] = TILEDB_GZIP;
  }
  const int dense = 0;

  tiledb_array_set_schema(
      &schema,
      arrayName.c_str(),
      attributes,
      attribute_num,
      capacity,
      cell_order,
      NULL,
      compression,
      dense,
      dimensions,
      ARRAY_RANK_2D,
      domain,
      4*sizeof(int64_t),
      tile_extents,
      2*sizeof(int64_t),
      tile_order,
      types);

  // Create the array
  int ret = tiledb_array_create(tiledb_ctx, &schema);

  return ret;
} // end of create_sparse_array_2D

void SparseArrayTestFixture::setArrayName(const char *name) {
  this->arrayName.append(WORKSPACE);
  this->arrayName.append(name);
}

int SparseArrayTestFixture::write_sparse_array_unsorted_2D(
  const int64_t dim0,
  const int64_t dim1) {

  int ret = 0;

  // Generate the data and coordinates for sparse write
  int64_t size = dim0*dim1;
  int * buffer_attr = new int [size];
  int64_t * buffer_coords = new int64_t [2*size];
  int64_t coords_index = 0L;
  for (int64_t i = 0; i < dim0; ++i) {
    for (int64_t j = 0; j < dim1; ++j) {
      buffer_attr[i*dim1+j] = i*dim1+j;
      buffer_coords[2*coords_index] = i;
      buffer_coords[2*coords_index+1] = j;
      coords_index++;
    }
  }

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  ret = tiledb_array_init(
            tiledb_ctx,
            &tiledb_array,
            arrayName.c_str(),
            TILEDB_ARRAY_WRITE_UNSORTED,
            NULL,            // No range - entire domain
            NULL,            // No projection - all attributes
            0);              // Meaningless when "attributes" is NULL

  assert(ret == TILEDB_OK);

  const void* buffers[ARRAY_RANK_2D];
  buffers[0] = buffer_attr;
  buffers[1] = buffer_coords;

  size_t bufferSizes[ARRAY_RANK_2D];
  bufferSizes[0] = { (size_t)size*sizeof(int) };
  bufferSizes[1] = { (size_t)2*size*sizeof(int64_t) };

  ret = tiledb_array_write(tiledb_array, buffers, bufferSizes);

  if (ret != TILEDB_OK) {
    return ret;
  }

  /* Finalize the array. */
  ret = tiledb_array_finalize(tiledb_array);
  return ret;
} // end of write_dense_array_by_chunks

int * SparseArrayTestFixture::read_sparse_array_2D(
    const int64_t dim0_lo,
    const int64_t dim0_hi,
    const int64_t dim1_lo,
    const int64_t dim1_hi,
    const int read_mode) {

  /* Initialize a range. */
  const int64_t range[] = { dim0_lo, dim0_hi, dim1_lo, dim1_hi };

  /* Subset over attribute "a1". */
  const char* attributes[] = { "ATTR_INT32" };

  /* Initialize the array in READ mode. */
  TileDB_Array* tiledb_array;
  int ret = tiledb_array_init(
      tiledb_ctx,
      &tiledb_array,
      arrayName.c_str(),
      read_mode,
      range,
      attributes,
      1);
  assert(ret == TILEDB_OK);

  /* Prepare cell buffers for attributes "a1" and "a2". */
  int64_t d0 = dim0_hi - dim0_lo + 1;
  int64_t d1 = dim1_hi - dim1_lo + 1;
  int64_t size = d0*d1;
  int *buffer_a1 = new int [size];
  void* buffers[] = { buffer_a1 };
  size_t buffer_sizes[1] = { size*sizeof(int) };

  /* Read from array. */
  ret = tiledb_array_read(tiledb_array, buffers, buffer_sizes);

  if (ret != TILEDB_OK) {
    return NULL;
  }

  /* Finalize the array. */
  tiledb_array_finalize(tiledb_array);

  return buffer_a1;
} // end of read_sparse_array_2D


/**
 * Test is to randomly read subregions of the array and
 * check with corresponding value set by row_id*dim1+col_id
 * Top left corner is always 4,4
 * Test runs through 100 iterations to choose random
 * width and height of the subregions
 */
TEST_F(SparseArrayTestFixture, test_random_sorted_reads) {
  int64_t dim0 = 5000;
  int64_t dim1 = 1000;
  int64_t chunkDim0 = 100;
  int64_t chunkDim1 = 100;
  int64_t dim0_lo = 0;
  int64_t dim0_hi = dim0-1;
  int64_t dim1_lo = 0;
  int64_t dim1_hi = dim1-1;
  int capacity = 0; // 0 means use default capacity
  int cell_order = TILEDB_ROW_MAJOR;
  int tile_order = TILEDB_ROW_MAJOR;
  bool enable_compression = false;

  setArrayName("sparse_test_5000x1000_100x100");

  // Create a dense integer array
  create_sparse_array_2D(
      chunkDim0,
      chunkDim1,
      dim0_lo,
      dim0_hi,
      dim1_lo,
      dim1_hi,
      capacity,
      cell_order,
      tile_order,
      enable_compression);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk chunk by chunk
  write_sparse_array_unsorted_2D(dim0, dim1);

  // Test is to randomly read sub-regions of the array
  // and check with corresponding value set by
  // row_id*dim1+col_id
  // Top left corner is always 4,4
  int64_t d0_lo = 4;
  int64_t d0_hi = 0;
  int64_t d1_lo = 4;
  int64_t d1_hi = 0;
  int64_t height = 0, width = 0;

  for (int iter = 0; iter < 20; ++iter) {
    height = rand() % (dim0 - d0_lo);
    width = rand() % (dim1 - d1_lo);
    d0_hi = d0_lo + height;
    d1_hi = d1_lo + width;
    int index = 0;

    int *buffer = read_sparse_array_2D(
                      d0_lo,
                      d0_hi,
                      d1_lo,
                      d1_hi,
                      TILEDB_ARRAY_READ_SORTED_ROW);

    if (!buffer) {
      std::cerr << "ERROR: NULL buffer returned. "
          << "Check TileDB array path "
          << arrayName << "\n";
      FAIL();
    }

    for (int i = d0_lo; i <= d0_hi; ++i) {
      for (int j = d1_lo; j <= d1_hi; ++j) {
        EXPECT_EQ(buffer[index], i*dim1+j);
        if (buffer[index] !=  (i*dim1+j)) {
          std::cerr << "mismatch: " << i
              << "," << j << "=" << buffer[index] << "!="
              << ((i*dim1+j)) << "\n";
          return;
        }
        index++;
      }
    }
  } // end of random for-loop
} // end of test_random_sorted_reads



