/**
 * @file   c_api_dense_array_spec.h
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
 * Declarations for testing the C API dense array spec.
 */

#ifndef __C_API_DENSE_ARRAY_SPEC_H__
#define __C_API_DENSE_ARRAY_SPEC_H__

#include "c_api.h"
#include <gtest/gtest.h>



/** Test fixture for dense array operations. */
class DenseArrayTestFixture: public testing::Test {
 public:
  /* ********************************* */
  /*             CONSTANTS             */
  /* ********************************* */

  /** Workspace folder name. */
  const std::string WORKSPACE = ".__workspace/";




  /* ********************************* */
  /*          GTEST FUNCTIONS          */
  /* ********************************* */

  /** Test initialization. */
  virtual void SetUp(); 

  /** Test finalization. */
  virtual void TearDown();




  /* ********************************* */
  /*           PUBLIC METHODS          */
  /* ********************************* */

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
              const int64_t update_num);

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
      const int tile_order);

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
      const int64_t domain_size_0, 
      const int64_t domain_size_1);

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
      const int64_t domain_size_0, 
      const int64_t domain_size_1);

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
      const int read_mode);

  /** Sets the array name for the current test. */
  void set_array_name(const char *);

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
      const size_t* buffer_sizes);

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
      const int64_t tile_extent_1);

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
      int write_mode,
      int* buffer,
      size_t* buffer_sizes);




  /* ********************************* */
  /*         PUBLIC ATTRIBUTES         */
  /* ********************************* */

  /** Array name. */
  std::string array_name_;
  /** Array schema object under test. */
  TileDB_ArraySchema array_schema_;
  /** TileDB context. */
  TileDB_CTX* tiledb_ctx_;
};

#endif
