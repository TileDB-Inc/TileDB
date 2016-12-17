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
 * dense arrays
 */

#include <gtest/gtest.h>
#include "c_api.h"
#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <cstring>
#include <sstream>
#include <map>

/**
 * Test fixture for dense array operations.
 * The Setup() and TearDown() methods are called
 * after each test is called and destroyed respectively.
 * In these methods, we create and delete the array
 */
class DenseArrayTestFixture: public testing::Test {
  const std::string WORKSPACE = ".__workspace/";
  const std::string ARRAY_100x100_10x10 = "dense_test_100x100_10x10";
  const int ARRAY_RANK_2D = 2;

public:
  // Array schema object under test
  TileDB_ArraySchema schema;
  // TileDB context
  TileDB_CTX* tiledb_ctx;
  // Array name is initialized with the workspace folder
  std::string arrayName;

  /**
   * Default constructor used to create a temporary
   * TileDB workspace in the current working directory
   * before all tests are called. User must have write
   * permissions to this directory
   */
  DenseArrayTestFixture();

  /**
   * Default destructor removes the temporary
   * TileDB workspace and destroys the TileDB
   * context
   */
  ~DenseArrayTestFixture();

  /**
   * Set the array name for the current test
   */
  void setArrayName(const char *);

  /**
   * Generate a test buffer to full up the dense array where
   * each cell value = row index * total number of columns + col index
   */
  int **generate_2Dbuffer(
      const int, const int);

  /**
   * Generate a 1D buffer containing the cell values
   * of a 2D array
   */
  int *generate_1Dbuffer(
        const int, const int);

  /**
   * Create the test dense array with given tile extents
   */
  int create_dense_array_2D(
      const long dim0_tile_extent,
      const long dim1_tile_extent,
      const long dim0_lo,
      const long dim0_hi,
      const long dim1_lo,
      const long dim1_hi,
      const int capacity,
      const bool enable_compression,
      const int cell_order,
      const int tile_order);

  /**
   * Load the array chunk by chunk. The buffer is initialized
   * with row_id*DIM1+col_id values. Tile extents or chunk
   * sizes are defined in the create_dense_array_2D
   */
  int write_dense_array_by_chunks(
      const int64_t dim0,
      const int64_t dim1,
      const int64_t chunkDim0,
      const int64_t chunkDim1);

  /**
   * Load the array in a sorted row-major manner using
   * a buffer which is ordered in the global cell order.
   * The buffer is initialized cell values as
   * row_id*DIM1+col_id values. Tile extents or chunk
   * sizes are defined in the create_dense_array_2D
   */
  int write_dense_array_sorted_2D(
      const int64_t dim0,
      const int64_t dim1,
      const int64_t chunkDim0,
      const int64_t chunkDim1,
      const int write_mode);

  int write_dense_array_sorted_range_2D(
      int64_t *subarray,
      int write_mode,
      size_t buffer_sizes[],
      int* buffer);

  /**
   * Update random locations in the dense array
   * These locations are recorded and later
   * used to validate reads
   */
  int update_dense_array_2D(
      const int dim0,
      const int dim1,
      int length,
      int srand_key,
      int *buffer_a1,
      int64_t *buffer_coords,
      const void* buffers[],
      size_t buffer_sizes[2]);

  /**
   * Read cell values of a dense array for a given
   * range and test whether it matches the value
   * row_id*DIM1+col_id
   */
  int * read_dense_array(
        const int64_t dim0_lo,
        const int64_t dim0_hi,
        const int64_t dim1_lo,
        const int64_t dim1_hi,
        const int read_mode);

  /**
   * Not a member-function. A global
   * stand-alone checker to compare two buffers
   */
  static bool check_buffer(
              const int *before,
              const int *after,
              const int *buffer_a1,
              const int64_t *buffer_coords,
              const int64_t dim0,
              const int64_t dim1,
              const int64_t chunkDim0,
              const int64_t chunkDim1,
              const int length);

  /**
   * Code here will be called immediately after the constructor (right
   * before each test).
   */
  virtual void SetUp();

  /**
   * Code here will be called immediately after each test
   * (right before the destructor).
   */
  virtual void TearDown();
};

DenseArrayTestFixture::DenseArrayTestFixture() {
  // Initialize context with the default configuration parameters
  tiledb_ctx_init(&tiledb_ctx, NULL);
  if (tiledb_workspace_create(tiledb_ctx, WORKSPACE.c_str())
        != TILEDB_OK) {
    exit(EXIT_FAILURE);
  }
}

DenseArrayTestFixture::~DenseArrayTestFixture() {
  // Finalize TileDB context
  tiledb_ctx_finalize(tiledb_ctx);

  // Remove the temporary workspace
  std::string command = "rm -rf ";
  command.append(WORKSPACE);
  int rc = system(command.c_str());
  assert(rc == 0);
}

void DenseArrayTestFixture::SetUp() {
  // Reset the random number generator
  srand(0);
}

void DenseArrayTestFixture::TearDown() {
  // delete currently tested array
  if (arrayName.empty()) return;

  tiledb_delete(
      (const TileDB_CTX*)this->tiledb_ctx,
      this->arrayName.c_str());
  arrayName.clear();
}

void DenseArrayTestFixture::setArrayName(const char *name) {
  this->arrayName.append(WORKSPACE);
  this->arrayName.append(name);
}

int **DenseArrayTestFixture::generate_2Dbuffer(
	const int dim0,
	const int dim1) {
  int **buffer = new int * [dim0];
  for (int i = 0; i < dim0; ++i) {
    buffer[i] = new int [dim1];
    for (int j = 0; j < dim1; ++j) {
      buffer[i][j] = i * dim1 + j;
    }
  }
  return buffer;
}

int *DenseArrayTestFixture::generate_1Dbuffer(
	const int dim0,
	const int dim1) {
	int *buffer = new int [dim0*dim1];
	for (int i = 0;i < dim0; ++i)
		for (int j = 0;j < dim1; ++j)
			buffer[i*dim1+j] = i*dim1+j;
	return buffer;
}

int DenseArrayTestFixture::create_dense_array_2D(
  const long dim0_tile_extent,
  const long dim1_tile_extent,
  const long dim0_lo,
  const long dim0_hi,
  const long dim1_lo,
  const long dim1_hi,
  const int capacity,
  const bool enable_compression,
  const int cell_order,
  const int tile_order) {

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
  const int dense = 1;

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
} // end of create_dense_array

int DenseArrayTestFixture::write_dense_array_by_chunks(
  const int64_t dim0,
  const int64_t dim1,
  const int64_t chunkDim0,
  const int64_t chunkDim1) {

  int ret = 0;
  int **buffer = generate_2Dbuffer(dim0, dim1);
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

  const void* buffers[ARRAY_RANK_2D];
  buffers[0] = buffer_a1;
  size_t bufferSizes[ARRAY_RANK_2D];
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
} // end of write_dense_array_by_chunks

int DenseArrayTestFixture::write_dense_array_sorted_2D(
  const int64_t dim0,
  const int64_t dim1,
  const int64_t chunkDim0,
  const int64_t chunkDim1,
  const int write_mode) {

  int ret = 0;
  int *buffer = generate_1Dbuffer(dim0, dim1);

  // Set the subarray for sorted writes
  int64_t subarray[] = { 0, dim0-1, 0, dim1-1 };

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  tiledb_array_init(
      tiledb_ctx,
      &tiledb_array,
      arrayName.c_str(),
      write_mode,
      subarray,
      NULL,            // No projection - all attributes
      1);              // Meaningless when "attributes" is NULL

  const void *buffers[] = { buffer };
  const size_t buffer_sizes[] = { dim0*dim1*sizeof(int) };

  ret = tiledb_array_write(
            tiledb_array,
            buffers,
            buffer_sizes);

  /* Finalize the array. */
  ret = tiledb_array_finalize(
          tiledb_array);

  delete(buffer);
  return ret;
} // end of write_dense_array_sorted_2D

int DenseArrayTestFixture::write_dense_array_sorted_range_2D(
  int64_t *subarray,
  int write_mode,
  size_t buffer_sizes[],
  int* buffer) {

  int ret = 0;
  const char *attributes[] = { "ATTR_INT32" };

  /* Initialize the array in WRITE mode. */
  TileDB_Array* tiledb_array;
  ret = tiledb_array_init(
      tiledb_ctx,
      &tiledb_array,
      arrayName.c_str(),
      write_mode,
      subarray,
      attributes,
      1);
  assert(ret == TILEDB_OK);

  const void * buffers[] = { buffer };
  ret = tiledb_array_write(tiledb_array, buffers, buffer_sizes);
  assert(ret == TILEDB_OK);

  /* Finalize the array. */
  ret = tiledb_array_finalize(tiledb_array);
  assert(ret == TILEDB_OK);

  return ret;
}

int DenseArrayTestFixture::update_dense_array_2D(
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
  int ret = tiledb_array_write(tiledb_array, buffers, buffer_sizes);

  if (ret != TILEDB_OK) {
    return ret;
  }

  /* Finalize the array. */
  ret = tiledb_array_finalize(tiledb_array);
  return ret;
} // end of update_array

int * DenseArrayTestFixture::read_dense_array(
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
    tiledb_array_init(
        tiledb_ctx,
        &tiledb_array,
        arrayName.c_str(),
        read_mode,
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

bool DenseArrayTestFixture::check_buffer(
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


/////////////////////////////
// Test definitions follow //
/////////////////////////////

/**
 * Test random updates in a dense array
 */
TEST_F(DenseArrayTestFixture, test_random_updates) {
  int64_t dim0 = 100;
  int64_t dim1 = 100;
  int64_t chunkDim0 = 10;
  int64_t chunkDim1 = 10;
  int64_t dim0_lo = 0;
  int64_t dim0_hi = 99;
  int64_t dim1_lo = 0;
  int64_t dim1_hi = 99;
  int capacity = 0; // 0 means use default capacity
  int cell_order = TILEDB_ROW_MAJOR;
  int tile_order = TILEDB_ROW_MAJOR;

  setArrayName("dense_test_100x100_10x10");

  // Create a dense integer array 100x100 with 10x10 tiles/chunks
  create_dense_array_2D(
      chunkDim0,
      chunkDim1,
      dim0_lo,
      dim0_hi,
      dim1_lo,
      dim1_hi,
      capacity,
	false,
	cell_order,
	tile_order);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk via TileDB Storage Manager
  write_dense_array_by_chunks(
      dim0,
      dim1,
      chunkDim0,
      chunkDim1);

  // Read the entire array back to memory
  int *before_update = read_dense_array(
                           dim0_lo,
                           dim0_hi,
                           dim1_lo,
                           dim1_hi,
                           TILEDB_ARRAY_READ);

  // Update random 100 elements with random seed = 7
  int length = 100;
  int srand_key = 7;
  // Prepare cell buffers for attributes "ATTR_INT32"
  int *buffer_a1 = new int [length];
  int64_t *buffer_coords = new int64_t [2*length];
  const void* buffers[] = { buffer_a1, buffer_coords};
  size_t buffer_sizes[2] = { length*sizeof(int), 2*length*sizeof(int64_t) };

  update_dense_array_2D(
      dim0,
      dim1,
      length,
      srand_key,
      buffer_a1,
      buffer_coords,
      buffers,
      buffer_sizes);

  // Read the entire array back to memory after update
  int *after_update =
      read_dense_array(
          dim0_lo,
          dim0_hi,
          dim1_lo,
          dim1_hi,
          TILEDB_ARRAY_READ);

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

  EXPECT_FALSE(fail);
}

/**
 * Test sorted writes to a dense array with both cells and tiles
 * ordered in a row-major fashion
 */
TEST_F(DenseArrayTestFixture, test_sorted_writes_row_major_tile_order) {
	int64_t dim0 = 10000;
	int64_t dim1 = 10000;
	int64_t chunkDim0 = 1000;
	int64_t chunkDim1 = 100;
	int64_t dim0_lo = 0;
	int64_t dim0_hi = dim0-1;
	int64_t dim1_lo = 0;
	int64_t dim1_hi = dim1-1;
	int capacity = 0; // 0 means use default capacity
	int cell_order = TILEDB_ROW_MAJOR;
	int tile_order = TILEDB_ROW_MAJOR;

	setArrayName("dense_test_10000x10000_1000x100");

	/**
	 *  Create a dense integer array
	 */
	create_dense_array_2D(
			chunkDim0,
			chunkDim1,
			0,
			dim0-1,
			0,
			dim1-1,
			capacity,
			false,
			cell_order,
			tile_order);

	/**
	 * Write array cells with value = row id * COLUMNS + col id
	 * to disk via TileDB Storage Manager
	 */
	int ret = write_dense_array_sorted_2D(
	              dim0,
	              dim1,
	              chunkDim0,
	              chunkDim1,
	              TILEDB_ARRAY_WRITE_SORTED_ROW);
	assert(ret==TILEDB_OK);

	/**
	 * Reading the array with read mode = TILEDB_ARRAY_READ
	 * will return the contiguous region on disk
	 * (chunk by chunk). Hence, check the buffer contents
	 * chunk by chunk and test the validity of sorted write
	 */
	int *after_write = read_dense_array(
                         dim0_lo,
                         dim0_hi,
                         dim1_lo,
                         dim1_hi,
                         TILEDB_ARRAY_READ);

	int64_t tiles[] = { (dim0/chunkDim0), (dim1/chunkDim1) };
	int64_t top_left[2];
	int index = 0;

	/**
	 * Traversing tiles in row major order
	 */
	for (int ti = 0; ti  < tiles[0]; ++ti) {
		top_left[0] = ti*chunkDim0;
		for (int tj = 0; tj  < tiles[1]; ++tj) {
			top_left[1] = tj*chunkDim1;
			for (int i = top_left[0]; i < top_left[0]+chunkDim0; ++i) {
				for (int j = top_left[1]; j < top_left[1]+chunkDim1; ++j) {
					EXPECT_EQ(after_write[index++], (i*dim1+j));
				}
			}
		}
	}

	delete[] after_write;
}	// end of test_sorted_writes_row_major_tile_order


/**
 * Test is to randomly read subregions of the array and
 * check with corresponding value set by row_id*dim1+col_id
 * Top left corner is always 4,4
 * Test runs through 100 iterations to choose random
 * width and height of the subregions
 */
TEST_F(DenseArrayTestFixture, test_random_sorted_reads) {
  int64_t dim0 = 5000;
  int64_t dim1 = 10000;
  int64_t chunkDim0 = 100;
  int64_t chunkDim1 = 100;
  int64_t dim0_lo = 0;
  int64_t dim0_hi = dim0-1;
  int64_t dim1_lo = 0;
  int64_t dim1_hi = dim1-1;
  int capacity = 0; // 0 means use default capacity
  int cell_order = TILEDB_ROW_MAJOR;
  int tile_order = TILEDB_ROW_MAJOR;

  setArrayName("dense_test_5000x10000_100x100");

  // Create a dense integer array
  create_dense_array_2D(
      chunkDim0,
      chunkDim1,
      dim0_lo,
      dim0_hi,
      dim1_lo,
      dim1_hi,
      capacity,
      false,
      cell_order,
      tile_order);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk chunk by chunk
  write_dense_array_by_chunks(
      dim0,
      dim1,
      chunkDim0,
      chunkDim1);

  // Test is to randomly read sub-regions of the array
  // and check with corresponding value set by
  // row_id*dim1+col_id
  // Top left corner is always 4,4
  int64_t d0_lo = 4;
  int64_t d0_hi = 0;
  int64_t d1_lo = 4;
  int64_t d1_hi = 0;
  int64_t height = 0, width = 0;

  for (int iter = 0; iter < 100; ++iter) {
    height = rand() % (dim0 - d0_lo);
    width = rand() % (dim1 - d1_lo);
    d0_hi = d0_lo + height;
    d1_hi = d1_lo + width;
    int index = 0;

    int *buffer = read_dense_array(
                    d0_lo,
                    d0_hi,
                    d1_lo,
                    d1_hi,
                    TILEDB_ARRAY_READ_SORTED_ROW);

    for (int i = d0_lo; i <= d0_hi; ++i) {
      for (int j = d1_lo; j <= d1_hi; ++j) {
        EXPECT_EQ(buffer[index], i*dim1+j);
        if (buffer[index] !=  (i*dim1+j)) {
          std::cout << "mismatch: " << i
              << "," << j << "=" << buffer[index] << "!="
              << ((i*dim1+j)) << "\n";
          return;
        }
        index++;
      }
    }
  }
} // end of test_random_sorted_reads


/**
 * Test is to randomly write regions of the 2D array and
 * read them back to validate the writes
 * Test runs through 100 iterations to choose random
 * width and height of the regions
 */
TEST_F(DenseArrayTestFixture, test_random_sorted_writes) {
  int64_t dim[2] = { 100, 100 };
  int64_t tile_extents[2] = { 10, 10 };
  int64_t dim_ranges[2][2] =
    {
        { 0, dim[0]-1 },
        { 0, dim[1]-1 }
    };
  int capacity = 0; // 0 means use default capacity
  int cell_order = TILEDB_ROW_MAJOR;
  int tile_order = TILEDB_ROW_MAJOR;

  setArrayName("dense_test_5000x10000_100x100");

  // Create a dense integer array
  create_dense_array_2D(
      tile_extents[0],
      tile_extents[1],
      dim_ranges[0][0],
      dim_ranges[0][1],
      dim_ranges[1][0],
      dim_ranges[1][1],
      capacity,
      false,
      cell_order,
      tile_order);

  int iterations = 10;
  int64_t d0[2], d1[2];

  for (int i = 0; i < iterations; ++i) {
    std::cout << "iteration: " << i << "\n";
    d0[0] = rand() % dim[0];
    d1[0] = rand() % dim[1];
    d0[1] = d0[0] + rand() % (dim[0] - d0[0]);
    d1[1] = d1[0] + rand() % (dim[1] - d1[0]);

    int64_t subarray[] = { d0[0], d0[1], d1[0], d1[1] };
    std::cout << "subarray: " << subarray[0] << ","
        << subarray[1] << "," << subarray[2] << ","
        << subarray[3] << "\n";
    size_t query_size[2] = { (size_t)(d0[1] - d0[0] + 1),
                              (size_t)(d1[1] - d1[0] + 1) };
    size_t buffer_size = query_size[0] * query_size[1];

    int *buffer = new int [buffer_size];
    size_t index = 0;

    for (size_t r = 0; r < query_size[0]; ++r)
      for (size_t c = 0; c < query_size[1]; ++c)
        buffer[index++] = - (rand() % 999999);

    write_dense_array_sorted_range_2D(
        subarray,
        TILEDB_ARRAY_WRITE_SORTED_ROW,
        query_size,
        buffer);

    int *out_buffer = read_dense_array(
                          subarray[0],
                          subarray[1],
                          subarray[2],
                          subarray[3],
                          TILEDB_ARRAY_READ_SORTED_ROW);

    for (index = 0; index < buffer_size; ++index)
      EXPECT_EQ(buffer[index], out_buffer[index]);

    delete [] buffer;
    delete [] out_buffer;
  }
}
