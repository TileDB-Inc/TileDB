/**
 * @file   c_api_sparse_array_spec.h
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
 * Declarations for testing the C API sparse array spec.
 */

#ifndef __C_API_SPARSE_ARRAY_SPEC_H__
#define __C_API_SPARSE_ARRAY_SPEC_H__

#include "tiledb.h"
#include <gtest/gtest.h>

class SparseArrayTestFixture: public testing::Test {
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
      const int tile_order);

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
      const int read_mode);

  /** Sets the array name for the current test. */
  void set_array_name(const char *);

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
      const int64_t domain_size_1);




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
