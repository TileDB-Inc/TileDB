/**
 * @file   utils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
#include <string>
#include <type_traits>
#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/types.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

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

/** Converts the input string into an `int` value. */
Status convert(const std::string& str, int* value);

/** Converts the input string into an `int64_t` value. */
Status convert(const std::string& str, int64_t* value);

/** Converts the input string into a `uint64_t` value. */
Status convert(const std::string& str, uint64_t* value);

/** Converts the input string into a `uint32_t` value. */
Status convert(const std::string& str, uint32_t* value);

/** Converts the input string into a `float` value. */
Status convert(const std::string& str, float* value);

/** Converts the input string into a `double` value. */
Status convert(const std::string& str, double* value);

/** Converts the input string into a `bool` value. */
Status convert(const std::string& str, bool* value);

/** Converts the input string into a `SerializationType` value. */
Status convert(const std::string& str, SerializationType* value);

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

/** Converts the input value to string. */
template <class T>
std::string to_str(const T& value);

/** Converts the input value of input type to string. */
std::string to_str(const void* value, Datatype type);

/** Returns the size of the common prefix between `a` and `b`. */
uint64_t common_prefix_size(const std::string& a, const std::string& b);

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

}  // namespace geometry

/* ********************************* */
/*          HASH FUNCTIONS           */
/* ********************************* */

namespace hash {

struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(std::pair<T1, T2> const& pair) const {
    std::size_t h1 = std::hash<T1>()(pair.first);
    std::size_t h2 = std::hash<T2>()(pair.second);
    return h1 ^ h2;
  }
};

}  // namespace hash

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

/** Returns log_b(x). */
double log(double b, double x);

/**
 * Computes a * b, but it checks for overflow. In case the product
 * overflows, it returns std::numeric_limits<T>::max().
 */
template <class T>
T safe_mul(T a, T b);

/**
 * Returns the maximum power of 2 minus one that is smaller than
 * or equal to `value`.
 */
uint64_t left_p2_m1(uint64_t value);

/**
 * Returns the minimum power of 2 minus one that is larger than
 * or equal to `value`.
 */
uint64_t right_p2_m1(uint64_t value);

}  // namespace math

/* ********************************* */
/*          ENDIANNESS FUNCTIONS     */
/* ********************************* */

namespace endianness {

/**
 * Returns true if the current CPU architecture has little endian byte
 * ordering, false for big endian.
 */
inline bool is_little_endian() {
  const int n = 1;
  return *(char*)&n == 1;
}

/**
 * Returns true if the current CPU architecture has big endian byte
 * ordering, false for little endian.
 */
inline bool is_big_endian() {
  return !is_little_endian();
}

/**
 * Decodes a little-endian ordered buffer 'data' into a native
 * primitive type, T.
 */
template <class T>
inline T decode_le(const void* const data) {
  const T* const n = reinterpret_cast<const T* const>(data);
  if (is_little_endian()) {
    return *n;
  }

  T le_n;
  char* const p = reinterpret_cast<char*>(&le_n);
  for (std::size_t i = 0; i < sizeof(T); ++i) {
    p[i] = *n >> ((sizeof(T) - i - 1) * 8);
  }

  return le_n;
}

/**
 * Decodes a big-endian ordered buffer 'data' into a native
 * primitive type, T.
 */
template <class T>
inline T decode_be(const void* const data) {
  const T* const n = reinterpret_cast<const T* const>(data);
  if (is_big_endian()) {
    return *n;
  }

  T be_n;
  char* const p = reinterpret_cast<char*>(&be_n);
  for (std::size_t i = 0; i < sizeof(T); ++i) {
    p[i] = *n >> (i * 8);
  }

  return be_n;
}

}  // namespace endianness

}  // namespace utils

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_UTILS_H
