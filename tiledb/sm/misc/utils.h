/**
 * @file   utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include <cassert>
#include <cmath>
#include <string>
#include <type_traits>
#include <vector>

#include "constants.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/types.h"

namespace tiledb {
namespace sm {

using namespace tiledb::common;

class Posix;
class URI;

enum class Datatype : uint8_t;
enum class SerializationType : uint8_t;

namespace utils {

#ifdef __linux__
namespace https {
/**
 * Check hard coded paths for possible ca certificates to set for curl
 *
 * @param vfs to use to check if cert paths exist
 * @return ca cert bundle path or empty string if ca cert bundle was not found
 */
std::string find_ca_certs_linux(const Posix& posix);
}  // namespace https
#endif

namespace parse {

/* ********************************* */
/*          PARSING FUNCTIONS        */
/* ********************************* */

/**
 * Retrieves the timestamp range from the input
 * URI. For format version <= 2, only the range start is valid
 * (the range end is ignored).
 */
Status get_timestamp_range(
    const URI& uri, std::pair<uint64_t, uint64_t>* timestamp_range);

/**
 * Retrieves the fragment name version.
 *  - Version 1 corresponds to format versions 1 and 2
 *      * __uuid_<t1>{_t2}
 *  - Version 2 corresponds to version 3 and 4
 *      * __t1_t2_uuid
 *  - Version 3 corresponds to version 5 or higher
 *      * __t1_t2_uuid_version
 */
Status get_fragment_name_version(const std::string& name, uint32_t* version);

/**
 * Retrieves the fragment version. This will work only for
 * name versions > 2, otherwise the function sets `version`
 * to UINT32_MAX.
 */
Status get_fragment_version(const std::string& name, uint32_t* version);

/** Returns `true` if the input string is a (potentially signed) integer. */
bool is_int(const std::string& str);

/** Returns `true` if the input string is an unsigned integer. */
bool is_uint(const std::string& str);

/** Converts the input value to string. */
template <class T>
std::string to_str(const T& value);

/** Converts the input value of input type to string. */
std::string to_str(const void* value, Datatype type);

}  // namespace parse

/* ********************************* */
/*          TYPE FUNCTIONS           */
/* ********************************* */

namespace datatype {

/**
 * Check if a given type T is quivalent to the tiledb::sm::DataType
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
 * Returns the number of cells in the input rectangle. Applicable
 * only to integers.
 *
 * @tparam T The rectangle domain type.
 * @param rect The input rectangle.
 * @param dim_num The number of dimensions of the rectangle.
 * @return The number of cells in the rectangle.
 */
template <
    class T,
    typename std::enable_if<std::is_integral<T>::value, T>::type* = nullptr>
uint64_t cell_num(const T* rect, unsigned dim_num) {
  uint64_t ret = 1;
  for (unsigned i = 0; i < dim_num; ++i)
    ret *= rect[2 * i + 1] - rect[2 * i] + 1;
  return ret;
}

/** Non-applicable to non-integers. */
template <
    class T,
    typename std::enable_if<!std::is_integral<T>::value, T>::type* = nullptr>
uint64_t cell_num(const T* rect, unsigned dim_num) {
  assert(false);
  (void)rect;
  (void)dim_num;
  return 0;
}

/**
 * Checks if `coords` are inside `rect`.
 *
 * @tparam T The type of the cell and subarray.
 * @param coords The coordinates to be checked.
 * @param rect The hyper-rectangle to be checked, expressed as [low, high] pairs
 *     along each dimension.
 * @param dim_num The number of dimensions for the coordinates and
 *     hyper-rectangle.
 * @return `true` if `coords` are inside `rect` and `false` otherwise.
 */
template <class T>
bool coords_in_rect(const T* coords, const T* rect, unsigned int dim_num);

/**
 * Checks if `coords` are inside `rect`.
 *
 * @tparam T The type of the cell and subarray.
 * @param coords The coordinates to be checked.
 * @param rect The hyper-rectangle to be checked, expressed as [low, high] pairs
 *     along each dimension.
 * @param dim_num The number of dimensions for the coordinates and
 *     hyper-rectangle.
 * @return `true` if `coords` are inside `rect` and `false` otherwise.
 */
template <class T>
bool coords_in_rect(
    const T* coords, const std::vector<const T*>& rect, unsigned int dim_num);

/** Returns *true* if hyper-rectangle `a` overlaps with `b`. */
template <class T>
bool overlap(const T* a, const T* b, unsigned dim_num);

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

/**
 * Returns the intersection between r1 and r2.
 *
 * @param r1 A vector of 1D ranges, one range per dimension as a 2-element
 *     (start, end) array.
 * @param r2 A vector of 1D ranges, one range per dimension as a 2-element
 *     (start, end) array.
 * @return The intersection between r1 and r2, similarly as a vector of
 *     (start, end) 1D ranges.
 */
template <class T>
std::vector<std::array<T, 2>> intersection(
    const std::vector<std::array<T, 2>>& r1,
    const std::vector<std::array<T, 2>>& r2);

}  // namespace geometry

}  // namespace utils
}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UTILS_H
