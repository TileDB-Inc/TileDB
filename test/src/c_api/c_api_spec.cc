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
 * Tests to check the TileDB APIs in C
 */

#include "gtest/gtest.h"
#include "c_api.h"
#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <cstring>

class TileDBAPITest: public testing::Test {
	const std::string WORKSPACE	= "/tmp/.__workspace/";
	const std::string ARRAY_100x100 = "dense_test_100x100_10x10";
	const int ARRAY_RANK = 2;

public:
	// Array schema object under test
	TileDB_ArraySchema schema;
	// TileDB context
	TileDB_CTX* tiledbCtx;
	// Array name is initialized with the workspace folder
	std::string arrayName;

	int **generatedBuffer(
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
	int update_array(
			const int dim0,
			const int dim1,
			int length,
			int srand_key,
			int *buffer_a1,
			int64_t *buffer_coords,
			const void* buffers[],
			size_t buffer_sizes[2]);
	int * read_array(
			const int64_t dim0_lo,
			const int64_t dim0_hi,
			const int64_t dim1_lo,
			const int64_t dim1_hi);

	virtual void SetUp() {
		// Initialize context with the default configuration parameters
		tiledb_ctx_init(&tiledbCtx, NULL);
		if (tiledb_workspace_create(tiledbCtx, WORKSPACE.c_str()) != TILEDB_OK) {
			exit(EXIT_FAILURE);
		}

		arrayName.append(WORKSPACE);
		arrayName.append(ARRAY_100x100);
	}

	virtual void TearDown() {
		// Finalize TileDB context
		tiledb_ctx_finalize(tiledbCtx);

		// Remove the temporary workspace
		std::string command = "rm -rf ";
		command.append(WORKSPACE);
		int ret = system(command.c_str());
	}
};

/**
 * Generate a test buffer to full up the dense array where
 * each cell value = row index * total number of columns + col index
 */
int **TileDBAPITest::generatedBuffer(const int dim0, const int dim1) {
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
 */
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

	tiledb_array_set_schema(&schema, arrayName.c_str(), attributes,
			attribute_num, dimensions, ARRAY_RANK, 1, domain, 100*sizeof(int64_t),
			tile_extents, 10*sizeof(int64_t), types, NULL, TILEDB_COL_MAJOR,
			0, capacity, compression);

	// Create the array
	int ret = tiledb_array_create(tiledbCtx, &schema);
	std::cout << "Array created\n";
	return ret;
}	// end of create_dense_array

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
	int **buffer = generatedBuffer(dim0, dim1);
	int64_t segmentSize = chunkDim0 * chunkDim1;
	int * buffer_a1 = new int [segmentSize];
	for (int i = 0; i < segmentSize; ++i)
		buffer_a1[i] = 0;

	/* Initialize the array in WRITE mode. */
	TileDB_Array* tiledb_array;
	tiledb_array_init(
			tiledbCtx,
			&tiledb_array,
			arrayName.c_str(),
			TILEDB_WRITE,
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
	std::cout << "Array loaded\n";
	return ret;
}	// end of write_dense_array

int TileDBAPITest::update_array(
	const int dim0,
	const int dim1,
	int length,
	int srand_key,
	int *buffer_a1,
	int64_t *buffer_coords,
	const void* buffers[],
	size_t buffer_sizes[2]) {


	/* Subset over attribute "a1" and the coordinates. */
	const char* attributes[] = { "ATTR_INT32", TILEDB_COORDS_NAME };

	/* Initialize the array in WRITE mode. */
	TileDB_Array* tiledb_array;
	tiledb_array_init(
			tiledbCtx,
			&tiledb_array,
			arrayName.c_str(),
			TILEDB_WRITE_UNSORTED,
			NULL,            // No range - entire domain
			attributes,            // No projection - all attributes
			2);              // Meaningless when "attributes" is NULL

	/* Populate attribute buffers with some arbitrary values. */
	// Random updates
	srand(srand_key);
	int64_t d0, d1, x;
	int64_t coords_index = 0L;
	for (int i = 0; i < length; ++i) {
		d0 = rand() % dim0;
		d1 = rand() % dim1;
		x = rand();
		buffer_coords[coords_index++] = d0;
		buffer_coords[coords_index++] = d1;
		buffer_a1[i] = x;
		std::cout << "(" << d0 << "," << d1 << "," << x << ")\n";
	}

	/* Write to array. */
	int ret = tiledb_array_write(tiledb_array, buffers, buffer_sizes);

	if (ret != TILEDB_OK) {
		return ret;
	}

	/* Finalize the array. */
	ret = tiledb_array_finalize(tiledb_array);
	std::cout << "Array updated\n";
	return ret;
}	// end of update_array

int * TileDBAPITest::read_array(
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
	      tiledbCtx,
	      &tiledb_array,
	      arrayName.c_str(),
	      TILEDB_READ,
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

	  std::cout << "Array read\n";

	  return buffer_a1;
}


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
	int length = 1;
	int srand_key = 0;
	int capacity = 0;	// 0 means use default capacity

	/* Prepare cell buffers for attributes "a1" and "a2". */
	int *buffer_a1 = new int [length];
	int64_t *buffer_coords = new int64_t [2*length];
	const void* buffers[] = { buffer_a1, buffer_coords};
	size_t buffer_sizes[2] = { length*sizeof(int), 2*length*sizeof(int64_t) };

	create_dense_array(chunkDim0, chunkDim1, 0, dim0-1, 0, dim1-1, capacity);
	write_dense_array(dim0, dim1, chunkDim0, chunkDim1);
	int *before_update = read_array(dim0_lo, dim0_hi, dim1_lo, dim1_hi);
	update_array(dim0, dim1, length, srand_key, buffer_a1, buffer_coords, buffers, buffer_sizes);
	int *after_update = read_array(dim0_lo, dim0_hi, dim1_lo, dim1_hi);
	bool fail = check_buffer(before_update, after_update, buffer_a1, buffer_coords, dim0, dim1, chunkDim0, chunkDim1, length);

	ASSERT_EQ(fail, false);
}
