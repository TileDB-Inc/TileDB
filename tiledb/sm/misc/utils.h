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

/** Converts the input string into an `int` value. */
Status convert(const std::string& str, int* value);

/** Converts the input string into a `long` value. */
Status convert(const std::string& str, long* value);

/** Converts the input string into a `uint64_t` value. */
Status convert(const std::string& str, uint64_t* value);

/** Returns `true` if the input string is a (potentially signed) integer. */
bool is_int(const std::string& str);

/** Returns `true` if the input string is an unsigned integer. */
bool is_uint(const std::string& str);

/**
 * Returns the input domain as a string of the form "[low, high]".
 *
 * @param domain A single dimension's domain.
 * @param type The type of the dimension.
 * @return A string of the form "[low, high]".
 */
std::string domain_str(const void* domain, Datatype type);

/**
 * Returns a dimension's tile extent in string form.
 *
 * @param tile_extent The tile extent of a single dimension.
 * @param type The type of the dimension.
 * @return The tile extent in string form.
 */
std::string tile_extent_str(const void* tile_extent, Datatype type);

/**
 * Checks if a string starts with a certain prefix.
 *
 * @param value The base string.
 * @param prefix The prefix string to be tested.
 * @return *true* if *value* starts with the *prefix*, and *false* otherwise.
 */
bool starts_with(const std::string& value, const std::string& prefix);

/**
 * Checks if a string ends with a certain suffix.
 * @param value The base string.
 * @param suffix The suffix to be tested.
 * @return *true* if *value* ends with the *suffix*, and *false* otherwise.
 */
bool ends_with(const std::string& value, const std::string& suffix);

}  // namespace parse

/* ********************************* */
/*          TYPE FUNCTIONS           */
/* ********************************* */

namespace datatype {

/**
 * Check if a given type T is quivalent to the tiledb::sm::DataTtpe
 * @tparam T
 * @param datatype to compare T to
 * @return Status indicating Ok() on equal data types else Status:Error()
 */
template <class T>
Status check_template_type_to_datatype(Datatype datatype);

}  // namespace datatype

/* ********************************* */
/*        GEOMETRY FUNCTIONS         */
/* ********************************* */

namespace geometry {

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

/** Returns *true* if hyper-rectangles `a` overlaps with `b`. */
template <class T>
bool overlap(const T* a, const T* b, unsigned dim_num);

/**
 * Checks whether two hyper-rectangles overlap, and determines whether
 * the first rectangle contains the second.
 *
 * @tparam T The type of the rectangles.
 * @param a The first rectangle.
 * @param b The second rectangle.
 * @param dim_num The number of dimensions.
 * @param a_contains_b Determines whether the first rectangle contains the
 *     second.
 * @return `True` if the rectangles overlap, and `false` otherwise.
 */
template <class T>
bool overlap(const T* a, const T* b, unsigned dim_num, bool* a_contains_b);

/**
 * Computes the overlap between two rectangles.
 *
 * @tparam T The types of the rectangles.
 * @param a The first input rectangle.
 * @param b The second input rectangle.
 * @param o The overlap area between `a` and `b`.
 * @param overlap `true` if the two rectangles overlap and `false` otherwise.
 */
template <class T>
void overlap(const T* a, const T* b, unsigned dim_num, T* o, bool* overlap);

/**
 * Returns the percentage of coverage of hyper-rectangle `a` in `b`.
 * Note that the function assumes that `a` is fully contained in `b`.
 */
template <class T>
double coverage(const T* a, const T* b, unsigned dim_num);

}  // namespace geometry

/* ********************************* */
/*          TIME FUNCTIONS           */
/* ********************************* */

namespace time {

/**
 * Returns the current time in milliseconds since
 * 1970-01-01 00:00:00 +0000 (UTC).
 */
uint64_t timestamp_now_ms();

}  // namespace time

/* ********************************* */
/*          MATH FUNCTIONS           */
/* ********************************* */

namespace math {

/** Returns the value of x/y (integer division) rounded up. */
uint64_t ceil(uint64_t x, uint64_t y);

}  // namespace math

}  // namespace utils

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UTILS_H
