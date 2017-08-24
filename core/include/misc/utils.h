/**
 * @file   utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file contains useful (global) functions.
 */

#ifndef TILEDB_UTILS_H
#define TILEDB_UTILS_H

#ifdef HAVE_MPI
#include <mpi.h>
#endif

#include <string>
#include <vector>
#include "array_type.h"
#include "compressor.h"
#include "config.h"
#include "datatype.h"
#include "layout.h"
#include "status.h"
#include "uri.h"

#ifdef HAVE_OPENMP
#include <omp.h>
#endif

namespace tiledb {

namespace utils {

/* ********************************* */
/*             FUNCTIONS             */
/* ********************************* */

/** Returns the string representation of the input array type. */
const char* array_type_str(ArrayType array_type);

/**
 * Checks if the input cell is inside the input subarray.
 *
 * @tparam T The type of the cell and subarray.
 * @param cell The cell to be checked.
 * @param subarray The subarray to be checked, expresses as [low, high] pairs
 *     along each dimension.
 * @param dim_num The number of dimensions for the cell and subarray.
 * @return *true* if the input cell is inside the input range and
 *     *false* otherwise.
 */
template <class T>
bool cell_in_subarray(const T* cell, const T* subarray, int dim_num);

/**
 * Returns the number of cells in the input subarray (considering that the
 * subarray is dense).
 *
 * @tparam T The type of the subarray.
 * @param subarray The input subarray.
 * @param dim_num The number of dimensions of the subarray.
 * @return The number of cells in the input subarray.
 */
template <class T>
int64_t cell_num_in_subarray(const T* subarray, int dim_num);

/**
 * Compares the precedence of two coordinates based on the column-major order.
 *
 * @tparam T The type of the input coordinates.
 * @param coords_a The first coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template <class T>
int cmp_col_order(const T* coords_a, const T* coords_b, int dim_num);

/**
 * Compares the precedence of two coordinates associated with ids,
 * first on their ids (the smaller preceeds the larger) and then based
 * on the column-major order.
 *
 * @tparam T The type of the input coordinates.
 * @param id_a The id of the first coordinates.
 * @param coords_a The first coordinates.
 * @param id_b The id of the second coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template <class T>
int cmp_col_order(
    int64_t id_a,
    const T* coords_a,
    int64_t id_b,
    const T* coords_b,
    int dim_num);

/**
 * Compares the precedence of two coordinates based on the row-major order.
 *
 * @tparam T The type of the input coordinates.
 * @param coords_a The first coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template <class T>
int cmp_row_order(const T* coords_a, const T* coords_b, int dim_num);

/**
 * Compares the precedence of two coordinates associated with ids,
 * first on their ids (the smaller preceeds the larger) and then based
 * on the row-major order.
 *
 * @tparam T The type of the input coordinates.
 * @param id_a The id of the first coordinates.
 * @param coords_a The first coordinates.
 * @param id_b The id of the second coordinates.
 * @param coords_b The second coordinates.
 * @param dim_num The number of dimensions of the coordinates.
 * @return -1 if *coords_a* precedes *coords_b*, 0 if *coords_a* and
 *     *coords_b* are equal, and +1 if *coords_a* succeeds *coords_b*.
 */
template <class T>
int cmp_row_order(
    int64_t id_a,
    const T* coords_a,
    int64_t id_b,
    const T* coords_b,
    int dim_num);

/** Returns the string representation of the input compressor. */
const char* compressor_str(Compressor compressor);

/**
 * Checks if a fragment exists
 *
 * @param frag fragment uri
 * @return bool true if the fragment exists, false otherwise
 */
bool fragment_exists(const uri::URI& frag);

/**
 * Deletes a fragment represenation
 *
 * @param frag fragment path string
 * @return Status
 */
Status delete_fragment(const uri::URI& frag);

/**
 * Returns the parent path of the input path.
 *
 * @param pth The input path.
 * @return The parent path of the input path.
 */
std::string parent_path(const std::string& dir);

/**
 * Returns the size in bytes of the input datatype
 *
 * @param type The input datatype.
 * @return The size in bytes of the input datatype.
 */
uint64_t datatype_size(Datatype type);

/** Returns the string representation of the input data type. */
const char* datatype_str(Datatype type);

/**
 * Returns the input domain as a string of the form "[low, high]".
 *
 * @param domain A single dimension's domain.
 * @param type The type of the dimension.
 * @return A string of the form "[low, high]".
 */
std::string domain_str(const void* domain, Datatype type);

/**
 * Checks if the input is a special TileDB empty value.
 *
 * @tparam T The type of the input value.
 * @param value The value to be checked.
 * @return *true* if the input value is a special TileDB empty value, and
 *     *false* otherwise.
 */
template <class T>
bool empty_value(T value);

/**
 * Doubles the size of the buffer.
 *
 * @param buffer The buffer to be expanded.
 * @param buffer_allocated_size The original allocated size of the buffer.
 *     After the function call, this size doubles.
 * @return TILEDB_UT_OK for success, and TILEDB_UT_ERR for error.
 */
Status expand_buffer(void*& buffer, size_t& buffer_allocated_size);

/**
 * Expands the input MBR so that it encompasses the input coordinates.
 *
 * @tparam T The type of the MBR and coordinates.
 * @param mbr The input MBR to be expanded.
 * @param coords The input coordinates to expand the MBR.
 * @param dim_num The number of dimensions of the MBR and coordinates.
 * @return void
 */
template <class T>
void expand_mbr(T* mbr, const T* coords, int dim_num);

/**
 * Returns the MAC address of the machine as a 12-char string, e.g.,
 * 00332a0b8c64. Returns an empty string upon error.
 */
std::string get_mac_addr();

/**
 * Checks if there are duplicates in the input vector.
 *
 * @tparam T The type of the values in the input vector.
 * @param v The input vector.
 * @return *true* if the vector has duplicates, and *false* otherwise.
 */
template <class T>
bool has_duplicates(const std::vector<T>& v);

/**
 * Checks if the input coordinates lie inside the input subarray.
 *
 * @tparam T The coordinates and subarray type.
 * @param coords The input coordinates.
 * @param subarray The input subarray.
 * @param dim_num The number of dimensions of the subarray.
 * @return *true* if the coordinates lie in the subarray, and *false* otherwise.
 */
template <class T>
bool inside_subarray(const T* coords, const T* subarray, int dim_num);

/**
 * Checks if the input vectors have common elements.
 *
 * @tparam T The type of the elements of the input vectors.
 * @param v1 The first input vector.
 * @param v2 The second input vector.
 * @return *true* if the input vectors have common elements, and *false*
 *     otherwise.
 */
template <class T>
bool intersect(const std::vector<T>& v1, const std::vector<T>& v2);

/**
 * Checks if the input directory is an array.
 *
 * @param dir The directory to be checked.
 * @return *true* if the directory is an array, and *false* otherwise.
 */
bool is_array(const std::string& dir);

// TODO: uri
bool is_array(const uri::URI& uri);

/**
 * Checks if one range is fully contained in another.
 *
 * @tparam The domain type
 * @param range_A The first range.
 * @param range_B The second range.
 * @param dim_num The number of dimensions.
 * @return True if range_A is fully contained in range_B.
 */
template <class T>
bool is_contained(const T* range_A, const T* range_B, int dim_num);

/**
 * Checks if the input directory is a fragment.
 *
 * @param dir The directory to be checked.
 * @return *true* if the directory is a fragment, and *false* otherwise.
 */
bool is_fragment(const std::string& dir);

/**
 * Checks if the input directory is a group.
 *
 * @param dir The directory to be checked.
 * @return *true* if the directory is a group, and *false* otherwise.
 */
bool is_group(const std::string& dir);

// TODO: uri
bool is_group(const uri::URI& uri);

/**
 * Determine if a given path is a array schema
 * @param path
 * @return bool
 */
bool is_array_schema(const std::string& path);

/**
 * Determine if a given path is a consolidation lock file
 *
 * @param path  filesystem path
 * @return bool
 */
bool is_consolidation_lock(const std::string& path);

/** Returns *true* if the input string is a positive (>0) integer number. */
bool is_positive_integer(const char* s);

/** Returns *true* if the subarray contains a single element. */
template <class T>
bool is_unary_subarray(const T* subarray, int dim_num);

/** Returns the string representation of the input layout. */
const char* layout_str(Layout layout);

/**
 * Checks if a string starts with a certain prefix.
 *
 * @param value The base string.
 * @param prefix The prefix string to be tested.
 * @return *true* if *value* starts with the *prefix*, and *false* otherwise.
 */
bool starts_with(const std::string& value, const std::string& prefix);

/**
 * Returns a dimension's tile extent in string form.
 *
 * @param tile_extent The tile extent of a single dimension.
 * @param type The type of the dimension.
 * @return The tile extent in string form.
 */
std::string tile_extent_str(const void* tile_extent, Datatype type);

}  // namespace utils

}  // namespace tiledb

#endif  // TILEDB_UTILS_H
