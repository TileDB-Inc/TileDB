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
 * Tests to check read/write/update operations for
 * dense and sparse TileDB arrays via the C API
 */

#include <gtest/gtest.h>
#include "c_api.h"
#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <cstring>
#include <sstream>
#include <map>

class TileDBAPITest: public testing::Test {
  const std::string WORKSPACE = ".__workspace/";
  const std::string ARRAY_100x100 = "dense_test_100x100_10x10";
  const int ARRAY_RANK = 2;

public:
  // Array schema object under test
  TileDB_ArraySchema schema;
  // TileDB context
  TileDB_CTX* tiledb_ctx;
  // Array name is initialized with the workspace folder
  std::string arrayName;

  int **generate_buffer(
      const int, const int);
  int create_dense_array(
      const long dim0_tile_extent,
      const long dim1_tile_extent,
      const long dim0_lo,
      const long dim0_hi,
      const long dim1_lo,
      const long dim1_hi,
      const int capacity);
  int write_dense_array(
      const int64_t dim0,
      const int64_t dim1,
      const int64_t chunkDim0,
      const int64_t chunkDim1);
  int update_dense_array(
      const int dim0,
      const int dim1,
      int length,
      int srand_key,
      int *buffer_a1,
      int64_t *buffer_coords,
      const void* buffers[],
      size_t buffer_sizes[2]);
  int * read_dense_array(
      const int64_t dim0_lo,
      const int64_t dim0_hi,
      const int64_t dim1_lo,
      const int64_t dim1_hi);

  virtual void SetUp() {
    // Initialize context with the default configuration parameters
    tiledb_ctx_init(&tiledb_ctx, NULL);
    if (tiledb_workspace_create(
        tiledb_ctx,
        WORKSPACE.c_str()) != TILEDB_OK) {
      exit(EXIT_FAILURE);
    }

    arrayName.append(WORKSPACE);
    arrayName.append(ARRAY_100x100);
  }

  virtual void TearDown() {
    // Finalize TileDB context
    tiledb_ctx_finalize(tiledb_ctx);

    // Remove the temporary workspace
    std::string command = "rm -rf ";
    command.append(WORKSPACE);
    int rc = system(command.c_str());
    ASSERT_EQ(rc, 0);
  }
};
/**
 * Generate a test buffer to full up the dense array where
 * each cell value = row index * total number of columns + col index
 */
int **TileDBAPITest::generate_buffer(const int dim0, const int dim1) {
  int **buffer = new int * [dim0];
  for (int i = 0; i < dim0; ++i) {
    buffer[i] = new int [dim1];
    for (int j = 0; j < dim1; ++j) {
      buffer[i][j] = i * dim1 + j;
    }
  }
  return buffer;
}

/**
 * Create the test 100x100 dense array with tile sizes = 10x10
**/
int TileDBAPITest::create_dense_array(
  const long dim0_tile_extent,
  const long dim1_tile_extent,
  const long dim0_lo,
  const long dim0_hi,
  const long dim1_lo,
  const long dim1_hi,
  const int capacity) {

  // Prepare and set the array schema object and data structures
  const int attribute_num = 1;
  const char* attributes[] = { "ATTR_INT32" };
  const char* dimensions[] = { "X", "Y" };
  int64_t domain[] = { dim0_lo, dim0_hi, dim1_lo, dim1_hi };
  int64_t tile_extents[] = { dim0_tile_extent, dim1_tile_extent };
  const int types[] = { TILEDB_INT32, TILEDB_INT64 };
  const int compression[] =
  { TILEDB_NO_COMPRESSION, TILEDB_NO_COMPRESSION };
  const int dense = 1;

  tiledb_array_set_schema(
    &schema,
    arrayName.c_str(),
    attributes,
    attribute_num,
    capacity,
    TILEDB_ROW_MAJOR,
    NULL,
    compression,
    dense,
    dimensions,
    ARRAY_RANK,
    domain,
    4*sizeof(int64_t),
    tile_extents,
    2*sizeof(int64_t),
    0,
    types);

  // Create the array
  int ret = tiledb_array_create(tiledb_ctx, &schema);
  return ret;
} // end of create_dense_array

/**
 * Load the array with the buffer initialized with generatedBuffer logic
 * to the database
 */
int TileDBAPITest::write_dense_array(
  const int64_t dim0,
  const int64_t dim1,
  const int64_t chunkDim0,
  const int64_t chunkDim1) {

  int ret = 0;
  int **buffer = generate_buffer(dim0, dim1);
  int64_t segmentSize = chunkDim0 * chunkDim1;
  int * buffer_a1 = new int [segmentSize];
  for (int i = 0; i < segmentSize; ++i)
    buffer_a1[i] = 0;

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,
      &tiledb_array,
      arrayName.c_str(),
      TILEDB_ARRAY_WRITE,
      NULL,            // No range - entire domain
      NULL,            // No projection - all attributes
      0);              // Meaningless when "attributes" is NULL

  const void* buffers[ARRAY_RANK];
  buffers[0] = buffer_a1;
  size_t bufferSizes[ARRAY_RANK];
  int64_t index = 0L;
  size_t writeSize = 0L;
  for (int i = 0; i < dim0; i += chunkDim0) {
    for (int j = 0; j < dim1; j += chunkDim1) {
      long tile_rows = ((i + chunkDim0) < dim0) ? chunkDim0 : (dim0 - i);
      long tile_cols = ((j + chunkDim1) < dim1) ? chunkDim1 : (dim1 - j);
      int k,l;
      for (k = 0; k < tile_rows; ++k) {
        for (l = 0; l < tile_cols; ++l) {
          index = uint64_t(k * tile_cols + l);
          buffer_a1[index] = buffer[uint64_t(i + k)][uint64_t(j + l)];
        }
      }
      writeSize = k*l* sizeof(int);

      bufferSizes[0] = { writeSize };
      ret = tiledb_array_write(tiledb_array, buffers, bufferSizes);
      if (ret != TILEDB_OK) {
        return ret;
      }
    }
  }

  /* Finalize the array. */
  ret = tiledb_array_finalize(tiledb_array);
  return ret;
} // end of write_dense_array

int TileDBAPITest::update_dense_array(
  const int dim0,
  const int dim1,
  int length,
  int srand_key,
  int *buffer_a1,
  int64_t *buffer_coords,
  const void* buffers[],
  size_t buffer_sizes[2]) {

  /* Subset over attribute "a1" and the coordinates. */
  const char* attributes[] = { "ATTR_INT32", TILEDB_COORDS };

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,
      &tiledb_array,
      arrayName.c_str(),
      TILEDB_ARRAY_WRITE_UNSORTED,
      NULL,            // No range - entire domain
      attributes,            // No projection - all attributes
      2);              // Meaningless when "attributes" is NULL

  /* Populate attribute buffers with some arbitrary values. */
  // Random updates
  srand(srand_key);
  int64_t d0, d1, x;
  int64_t coords_index = 0L;
  std::map<std::string, int> my_map;
  std::map<std::string, int>::iterator it;
  my_map.clear();
  for (int i = 0; i < length; ++i) {
    std::ostringstream rand_stream;
    do {
      std::ostringstream rand_stream;
      d0 = rand() % dim0;
      d1 = rand() % dim1;
      x = rand();
      rand_stream << d0 << "," << d1;
      it = my_map.find(rand_stream.str());
    } while (it != my_map.end());
    rand_stream << d0 << "," << d1;
    my_map[rand_stream.str()] = x;
    buffer_coords[coords_index++] = d0;
    buffer_coords[coords_index++] = d1;
    buffer_a1[i] = x;
  }

  /* Write to array. */
  int ret = tiledb_array_write(
		  tiledb_array,
		  buffers,
		  buffer_sizes);

  if (ret != TILEDB_OK) {
    return ret;
  }

  /* Finalize the array. */
  ret = tiledb_array_finalize(tiledb_array);
  return ret;
} // end of update_array

/**
 * Read the elements of the array into buffers for a
 * given range
 */
int * TileDBAPITest::read_dense_array(
  const int64_t dim0_lo,
  const int64_t dim0_hi,
  const int64_t dim1_lo,
  const int64_t dim1_hi) {

    /* Initialize a range. */
    const int64_t range[] = { dim0_lo, dim0_hi, dim1_lo, dim1_hi };

    /* Subset over attribute "a1". */
    const char* attributes[] = { "ATTR_INT32" };

    /* Initialize the array in READ mode. */
    TileDB_Array* tiledb_array;
    tiledb_array_init(
        tiledb_ctx,
        &tiledb_array,
        arrayName.c_str(),
        TILEDB_ARRAY_READ,
        range,
        attributes,
        1);

    /* Prepare cell buffers for attributes "a1" and "a2". */
    size_t dim0 = dim0_hi - dim0_lo + 1;
    size_t dim1 = dim1_hi - dim1_lo + 1;
    size_t size = dim0*dim1;
    int *buffer_a1 = new int [size];
    void* buffers[] = { buffer_a1 };
    size_t buffer_sizes[1] = { size*sizeof(int) };

    /* Read from array. */
    int ret = tiledb_array_read(tiledb_array, buffers, buffer_sizes);

    if (ret != TILEDB_OK) {
      return NULL;
    }

    /* Finalize the array. */
    tiledb_array_finalize(tiledb_array);
    return buffer_a1;
}	// end of read_dense_array


/**
 * Stand-alone checker to compare two buffers
 */
bool check_buffer(
  const int *before,
  const int *after,
  const int *buffer_a1,
  const int64_t *buffer_coords,
  const int64_t dim0,
  const int64_t dim1,
  const int64_t chunkDim0,
  const int64_t chunkDim1,
  const int length) {

  int l,r;
  bool fail = false;
  int count = 0;
  for (int64_t i = 0; i < dim0*dim1; ++i) {
    l = before[i];
    r = after[i];

    if (l!=r) {
      bool found = false;
      for (int k = 0; k < length; ++k) {
        if (r==buffer_a1[k] && (l/dim1)==buffer_coords[2*k] &&
            (l%dim1)==buffer_coords[2*k+1]) {
          found = true;
          count++;
        }
      }
      if (!found) {
        fail = true;
      }
    }
  }

  if (count != length) {
    fail = true;
  }

  return fail;
}
TEST_F(TileDBAPITest, DenseArrayRandomUpdates) {
  int64_t dim0 = 100;
  int64_t dim1 = 100;
  int64_t chunkDim0 = 10;
  int64_t chunkDim1 = 10;
  int64_t dim0_lo = 0;
  int64_t dim0_hi = 99;
  int64_t dim1_lo = 0;
  int64_t dim1_hi = 99;
  int capacity = 0; // 0 means use default capacity

  // Create a dense integer array 100x100 with 10x10 tiles/chunks
  create_dense_array(
    chunkDim0,
    chunkDim1,
    0,
    dim0-1,
    0,
    dim1-1,
    capacity);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk via TileDB Storage Manager
  write_dense_array(
    dim0,
    dim1,
    chunkDim0,
    chunkDim1);

  // Read the entire array back to memory
  int *before_update = read_dense_array(
      dim0_lo,
      dim0_hi,
      dim1_lo,
      dim1_hi);

  // Update random 100 elements with random seed = 7
  int length = 100;
  int srand_key = 7;
  // Prepare cell buffers for attributes "ATTR_INT32"
  int *buffer_a1 = new int [length];
  int64_t *buffer_coords = new int64_t [2*length];
  const void* buffers[] = { buffer_a1, buffer_coords};
  size_t buffer_sizes[2] = { length*sizeof(int), 2*length*sizeof(int64_t) };

  update_dense_array(
    dim0,
    dim1,
    length,
    srand_key,
    buffer_a1,
    buffer_coords,
    buffers,
    buffer_sizes);

  // Read the entire array back to memory after update
  int *after_update = read_dense_array(
      dim0_lo,
      dim0_hi,
      dim1_lo,
      dim1_hi);

  // Compare array before and after array to check whether the randomly
  // generated elements are written to the correct positions
  bool fail = check_buffer(
      before_update,
      after_update,
      buffer_a1,
      buffer_coords,
      dim0,
      dim1,
      chunkDim0,
      chunkDim1,
      length);

  ASSERT_EQ(fail, false);
}
