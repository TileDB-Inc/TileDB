/**
 * @file   helpers-sparse_refactored_readers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file declares some helper functions for the sparse refactored readers
 * tests.
 */

#ifndef TILEDB_SPARSE_REFACTORED_READERS_HELPERS_H
#define TILEDB_SPARSE_REFACTORED_READERS_HELPERS_H

#include "tiledb.h"
#include "tiledb/sm/query/query.h"

#include <chrono>
#include <cmath>
#include <iostream>

// namespace std::chrono {
namespace tiledb {

/** Helper struct for the dimensions of an array. */
template <typename T>
struct TestDimension {
  /** Array name. */
  std::string name_;
  /** 2D domain of the array. */
  std::array<T, 2> domain_;
  /** Tile extent of the array. */
  uint64_t tile_extent_;
};

/** Helper struct for the attributes in an array. */
struct TestAttribute {
  /** Attribute name. */
  std::string name_;
  /** Attribute type. */
  tiledb_datatype_t type_;
};

/**
 * Helper struct for the buffers of an attribute/dimension
 * (fixed- or var-sized).
 */
struct QueryBuffer {
  /** Buffer name. */
  std::string name_;
  /** Buffer type. */
  tiledb_datatype_t type_;
  /** The buffer data. */
  void* data_;
  /**
   * For fixed-sized attributes/dimensions, it is `nullptr`.
   * For var-sized attributes/dimensions, it contains the var-sized offsets.
   */
  std::vector<uint64_t>* offsets_;
};

/**
 * Helper method to create an array.
 *
 * @tparam T The datatype of the array dimensions.
 * @param dims The array dimensions.
 * @param attrs The array attributes.
 */
template <typename T>
void create_array(
    const std::vector<TestDimension<T>>& dims,
    const std::vector<TestAttribute>& attrs);

/**
 * Helper method which uses the dimensions of the array to fill the row and
 * column buffers as a TileDB_Sparse array.
 * Example:
 * array: 1 2 5 6
 *        3 4 7 8
 * tile_extent = 4
 * rows: {1, 2, 5, 6, 3, 4, 7, 8}
 * cols: {1, 3, 2, 4, 5, 7, 6, 8}
 *
 * @tparam T The datatype of the array dimensions.
 * @param iterator An iterator used to parse the coordinates.
 * @param dims The array dimensions.
 * @param coords The coordinates of the array.
 */
template <typename T>
void fill_coords(
    uint64_t iterator,
    const std::vector<TestDimension<T>>& dims,
    std::array<uint64_t, 2>& coords);

/**
 * Helper method to create and fill write query buffers.
 * @param min_bound Lower bound to fill the buffers.
 * @param max_bound Upper bound to fill the buffers.
 * @param dims The array dimensions.
 * @param row_buffer Row buffer.
 * @param col_buffer Column buffer.
 * @param data_buffer Fix-sized data buffer. Nullptr if writing var-sized data.
 * @param data_var_buffer Var-sized data buffer. Nullptr if writing fixed data.
 * @param offset_buffer Offset buffer. Nullptr if writing fixed data.
 * @param offset The offset to begin writing var-sized data.
 *  Nullptr if writing fixed data.
 * @param buffers The attribute/dimension buffers to be written.
 *
 */
void create_write_query_buffer(
    int min_bound,
    int max_bound,
    std::vector<TestDimension<uint64_t>> dims,
    std::vector<uint64_t>* row_buffer,
    std::vector<uint64_t>* col_buffer,
    std::vector<uint64_t>* data_buffer,
    std::vector<char>* data_var_buffer,
    std::vector<uint64_t>* offset_buffer,
    uint64_t* offset,
    std::vector<QueryBuffer>& buffers);

/**
 * Performs a single write to an array.
 *
 * @param test_query_buffers
 * @return Status OK if succesful
 */
Status write(std::vector<QueryBuffer>& test_query_buffers);

/**
 * Prepare buffers for writing and then write to them.
 *
 * @param full_domain The full array domain where the write will be performed.
 * @param num_fragments The number of fragments to write to the array.
 * @param dims The array dimensions.
 * @param layout The array layout.
 * @param which_attrs An integer representing which attributes to write.
 *  1 = first, 2 = second, 3 = both
 * @return true if the write is successful (Status::COMPLETE)
 * @return false if the write is not successful (!Status::COMPLETE)
 */
bool write_array(
    uint64_t full_domain,
    uint64_t num_fragments,
    std::vector<TestDimension<uint64_t>> dims,
    std::string layout,
    int which_attrs);

/**
 * Helper method that ensures the array's written data matches the read data.
 *
 * @param validation_min The lower domain bound to begin the validation.
 * @param validation_max The upper domain bound to complete the validation.
 * @param layout The array layout.
 * @param data Fix-sized data. Nullptr if writing var-sized data.
 * @param data_var Var-sized data. Nullptr if writing fixed data.
 * @param offsets Offsets. Nullptr if writing fixed data.
 * @param coords_rows The array dimension row coordinates.
 * @param coords_cols The array dimension column coordinates.
 */
void validate_data(
    uint64_t validation_min,
    uint64_t validation_max,
    std::string layout,
    std::vector<uint64_t>* data,
    std::vector<char>* data_var,
    std::vector<uint64_t>* offsets,
    std::vector<uint64_t>& coords_rows,
    std::vector<uint64_t>& coords_cols);

/**
 * Prepare buffers for reading and then read the array.
 *
 * @param full_domain The full array domain.
 * @param buffer_size The size of the read buffers.
 * @param set_subarray Boolean representing whether or not to set the subarray.
 *  true = set, false = unset
 * @param layout The array layout.
 * @param which_attrs An integer representing which attributes to write.
 *  1 = first, 2 = second, 3 = both
 * @return true if the read is successful (Status::COMPLETE)
 * @return false if the read is not successful (!Status::COMPLETE)
 */
bool read_array(
    uint64_t full_domain,
    uint64_t buffer_size,
    bool set_subarray,
    std::string layout,
    int which_attrs);

/**
 * Helper method to run a wrapped test on an array with fixed parameters.
 *
 * @param full_domain The full array domain.
 * @param num_fragments The number of fragments to write to the array.
 * @param read_buffer_size The size of the read buffers.
 * @param attrs The attributes to write and read.
 * @param set_subarray Boolean representing whether or not to set the subarray.
 *  true = set, false = unset
 * @param layout The array layout.
 * @param which_attrs An integer representing which attributes to write.
 *  1 = first, 2 = second, 3 = both
 */
void sparse_global_test(
    uint64_t full_domain,
    uint64_t num_fragments,
    uint64_t read_buffer_size,
    const std::vector<TestAttribute>& attrs,
    bool set_subarray,
    std::string layout,
    int which_attrs);

}  // End of namespace tiledb

//} // End of namespace std::chrono

#endif
