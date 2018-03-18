/**
 * @file   utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/uri.h"

#include <string>
#include <vector>

namespace tiledb {
namespace sm {

namespace utils {

namespace parse {

/* ********************************* */
/*          PARSING FUNCTIONS        */
/* ********************************* */

/** Converts the input string into a `long` value. */
Status convert(const std::string& str, long* value);

/** Converts the input string into a `uint64_t` value. */
Status convert(const std::string& str, uint64_t* value);

/** Returns `true` if the input string is a (potentially signed) integer. */
bool is_int(const std::string& str);

/** Returns `true` if the input string is an unsigned integer. */
bool is_uint(const std::string& str);

}  // namespace parse

/* ********************************* */
/*             FUNCTIONS             */
/* ********************************* */

/**
 * Checks if `coords` are inside `rect`.
 *
 * @tparam T The type of the cell and subarray.
 * @param coords The coordinates to be checked.
 * @param rect The hyper-rectangle to be checked, expresses as [low, high] pairs
 *     along each dimension.
 * @param dim_num The number of dimensions for the coordinates and
 *     hyper-rectangle.
 * @return `true` if `coords` are inside `rect` and `false` otherwise.
 */
template <class T>
bool coords_in_rect(const T* coords, const T* rect, unsigned int dim_num);

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
uint64_t cell_num_in_subarray(const T* subarray, unsigned int dim_num);

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
int cmp_col_order(const T* coords_a, const T* coords_b, unsigned int dim_num);

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
    uint64_t id_a,
    const T* coords_a,
    uint64_t id_b,
    const T* coords_b,
    unsigned int dim_num);

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
int cmp_row_order(const T* coords_a, const T* coords_b, unsigned int dim_num);

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
    uint64_t id_a,
    const T* coords_a,
    uint64_t id_b,
    const T* coords_b,
    unsigned int dim_num);

/**
 * Returns the input domain as a string of the form "[low, high]".
 *
 * @param domain A single dimension's domain.
 * @param type The type of the dimension.
 * @return A string of the form "[low, high]".
 */
std::string domain_str(const void* domain, Datatype type);

/**
 * Doubles the size of the buffer.
 *
 * @param buffer The buffer to be expanded.
 * @param buffer_allocated_size The original allocated size of the buffer.
 *     After the function call, this size doubles.
 * @return Status.
 *
 * @note Upon failure to reallocate, the input buffer remains intact.
 */
Status expand_buffer(void*& buffer, uint64_t* buffer_allocated_size);

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
void expand_mbr(T* mbr, const T* coords, unsigned int dim_num);

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
bool inside_subarray(const T* coords, const T* subarray, unsigned int dim_num);

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
 * Checks if hyper-rectangle `a` is fully contained in hyper-rectangle `b`.
 *
 * @tparam The domain type
 * @param a The first hyper-rectangle.
 * @param b The second hyper-rectangle.
 * @param dim_num The number of dimensions.
 * @return True if `a` is fully contained in `b`.
 */
template <class T>
bool rect_in_rect(const T* a, const T* b, unsigned int dim_num);

/** Returns *true* if the input string is a positive (>0) integer number. */
bool is_positive_integer(const char* s);

/** Returns *true* if the subarray contains a single element. */
template <class T>
bool is_unary_subarray(const T* subarray, unsigned int dim_num);

/** Returns *true* if hyper-rectangles `a` overlaps with `b`. */
template <class T>
bool overlap(const T* a, const T* b, unsigned dim_num);

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

/**
 * Returns the current time in milliseconds.
 */
uint64_t timestamp_ms();

}  // namespace utils

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UTILS_H
