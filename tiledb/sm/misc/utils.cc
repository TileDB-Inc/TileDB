/**
 * @file   utils.cc
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
 * This file implements useful (global) functions.
 */

#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/misc/logger.h"

#include <iostream>
#include <set>
#include <sstream>

#ifdef _WIN32
#include <sys/timeb.h>
#include <sys/types.h>
#else
#include <sys/time.h>
#endif

namespace tiledb {
namespace sm {

namespace utils {

namespace parse {

/* ********************************* */
/*          PARSING FUNCTIONS        */
/* ********************************* */

Status convert(const std::string& str, long* value) {
  if (!is_int(str))
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to long; Invalid argument"));

  try {
    *value = std::stol(str);
  } catch (std::invalid_argument& e) {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to long; Invalid argument"));
  } catch (std::out_of_range& e) {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to long; Value out of range"));
  }

  return Status::Ok();
}

Status convert(const std::string& str, uint64_t* value) {
  if (!is_uint(str))
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to uint64_t; Invalid argument"));

  try {
    *value = std::stoull(str);
  } catch (std::invalid_argument& e) {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to uint64_t; Invalid argument"));
  } catch (std::out_of_range& e) {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to uint64_t; Value out of range"));
  }

  return Status::Ok();
}

bool is_int(const std::string& str) {
  // Check if empty
  if (str.empty())
    return false;

  // Check first character
  if (str[0] != '+' && str[0] != '-' && !(bool)isdigit(str[0]))
    return false;

  // Check rest of characters
  for (size_t i = 1; i < str.size(); ++i)
    if (!(bool)isdigit(str[i]))
      return false;

  return true;
}

bool is_uint(const std::string& str) {
  // Check if empty
  if (str.empty())
    return false;

  // Check first character
  if (str[0] != '+' && !isdigit(str[0]))
    return false;

  // Check characters
  for (size_t i = 1; i < str.size(); ++i)
    if (!(bool)isdigit(str[i]))
      return false;

  return true;
}

}  // namespace parse

/* ****************************** */
/*           FUNCTIONS            */
/* ****************************** */

template <class T>
inline bool coords_in_rect(
    const T* coords, const T* rect, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i) {
    if (coords[i] < rect[2 * i] || coords[i] > rect[2 * i + 1])
      return false;
  }

  return true;
}

template <class T>
uint64_t cell_num_in_subarray(const T* subarray, unsigned int dim_num) {
  uint64_t cell_num = 1;

  for (unsigned int i = 0; i < dim_num; ++i)
    cell_num *= subarray[2 * i + 1] - subarray[2 * i] + 1;

  return cell_num;
}

template <class T>
int cmp_col_order(const T* coords_a, const T* coords_b, unsigned int dim_num) {
  for (unsigned int i = dim_num - 1;; --i) {
    // a precedes b
    if (coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    if (coords_a[i] > coords_b[i])
      return 1;

    if (i == 0)
      break;
  }

  // a and b are equal
  return 0;
}

template <class T>
int cmp_col_order(
    uint64_t id_a,
    const T* coords_a,
    uint64_t id_b,
    const T* coords_b,
    unsigned int dim_num) {
  // a precedes b
  if (id_a < id_b)
    return -1;

  // b precedes a
  if (id_a > id_b)
    return 1;

  // ids are equal, check the coordinates
  for (unsigned int i = dim_num - 1;; --i) {
    // a precedes b
    if (coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    if (coords_a[i] > coords_b[i])
      return 1;

    if (i == 0)
      break;
  }

  // a and b are equal
  return 0;
}

template <class T>
int cmp_row_order(const T* coords_a, const T* coords_b, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i) {
    // a precedes b
    if (coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    if (coords_a[i] > coords_b[i])
      return 1;
  }

  // a and b are equal
  return 0;
}

template <class T>
int cmp_row_order(
    uint64_t id_a,
    const T* coords_a,
    uint64_t id_b,
    const T* coords_b,
    unsigned int dim_num) {
  // a precedes b
  if (id_a < id_b)
    return -1;

  // b precedes a
  if (id_a > id_b)
    return 1;

  // ids are equal, check the coordinates
  for (unsigned int i = 0; i < dim_num; ++i) {
    // a precedes b
    if (coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    if (coords_a[i] > coords_b[i])
      return 1;
  }

  // a and b are equal
  return 0;
}

std::string domain_str(const void* domain, Datatype type) {
  std::stringstream ss;

  if (domain == nullptr)
    return "";

  const int* domain_int32;
  const int64_t* domain_int64;
  const float* domain_float32;
  const double* domain_float64;
  const int8_t* domain_int8;
  const uint8_t* domain_uint8;
  const int16_t* domain_int16;
  const uint16_t* domain_uint16;
  const uint32_t* domain_uint32;
  const uint64_t* domain_uint64;

  switch (type) {
    case Datatype::INT32:
      domain_int32 = static_cast<const int*>(domain);
      ss << "[" << domain_int32[0] << "," << domain_int32[1] << "]";
      return ss.str();
    case Datatype::INT64:
      domain_int64 = static_cast<const int64_t*>(domain);
      ss << "[" << domain_int64[0] << "," << domain_int64[1] << "]";
      return ss.str();
    case Datatype::FLOAT32:
      domain_float32 = static_cast<const float*>(domain);
      ss << "[" << domain_float32[0] << "," << domain_float32[1] << "]";
      return ss.str();
    case Datatype::FLOAT64:
      domain_float64 = static_cast<const double*>(domain);
      ss << "[" << domain_float64[0] << "," << domain_float64[1] << "]";
      return ss.str();
    case Datatype::INT8:
      domain_int8 = static_cast<const int8_t*>(domain);
      ss << "[" << int(domain_int8[0]) << "," << int(domain_int8[1]) << "]";
      return ss.str();
    case Datatype::UINT8:
      domain_uint8 = static_cast<const uint8_t*>(domain);
      ss << "[" << int(domain_uint8[0]) << "," << int(domain_uint8[1]) << "]";
      return ss.str();
    case Datatype::INT16:
      domain_int16 = static_cast<const int16_t*>(domain);
      ss << "[" << domain_int16[0] << "," << domain_int16[1] << "]";
      return ss.str();
    case Datatype::UINT16:
      domain_uint16 = static_cast<const uint16_t*>(domain);
      ss << "[" << domain_uint16[0] << "," << domain_uint16[1] << "]";
      return ss.str();
    case Datatype::UINT32:
      domain_uint32 = static_cast<const uint32_t*>(domain);
      ss << "[" << domain_uint32[0] << "," << domain_uint32[1] << "]";
      return ss.str();
    case Datatype::UINT64:
      domain_uint64 = static_cast<const uint64_t*>(domain);
      ss << "[" << domain_uint64[0] << "," << domain_uint64[1] << "]";
      return ss.str();
    case Datatype::CHAR:
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      // Not supported domain type
      assert(false);
      return "";
  }

  assert(false);
  return "";
}

Status expand_buffer(void*& buffer, uint64_t* buffer_allocated_size) {
  *buffer_allocated_size *= 2;
  auto new_buffer = std::realloc(buffer, *buffer_allocated_size);
  if (new_buffer == nullptr)
    return LOG_STATUS(Status::MemError("Cannot reallocate buffer"));
  buffer = new_buffer;
  return Status::Ok();
}

template <class T>
void expand_mbr(T* mbr, const T* coords, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i) {
    // Update lower bound on dimension i
    if (mbr[2 * i] > coords[i])
      mbr[2 * i] = coords[i];

    // Update upper bound on dimension i
    if (mbr[2 * i + 1] < coords[i])
      mbr[2 * i + 1] = coords[i];
  }
}

const void* fill_value(Datatype type) {
  switch (type) {
    case Datatype::INT8:
      return &constants::empty_int8;
    case Datatype::UINT8:
      return &constants::empty_uint8;
    case Datatype::INT16:
      return &constants::empty_int16;
    case Datatype::UINT16:
      return &constants::empty_uint16;
    case Datatype::INT32:
      return &constants::empty_int32;
    case Datatype::UINT32:
      return &constants::empty_uint32;
    case Datatype::INT64:
      return &constants::empty_int64;
    case Datatype::UINT64:
      return &constants::empty_uint64;
    case Datatype::FLOAT32:
      return &constants::empty_float32;
    case Datatype::FLOAT64:
      return &constants::empty_float64;
    case Datatype::CHAR:
      return &constants::empty_char;
    case Datatype::ANY:
      return &constants::empty_any;
    case Datatype::STRING_ASCII:
      return &constants::empty_ascii;
    case Datatype::STRING_UTF8:
      return &constants::empty_utf8;
    case Datatype::STRING_UTF16:
      return &constants::empty_utf16;
    case Datatype::STRING_UTF32:
      return &constants::empty_utf32;
    case Datatype::STRING_UCS2:
      return &constants::empty_ucs2;
    case Datatype::STRING_UCS4:
      return &constants::empty_ucs4;
  }

  return nullptr;
}

template <class T>
bool has_duplicates(const std::vector<T>& v) {
  std::set<T> s(v.begin(), v.end());

  return s.size() != v.size();
}

template <class T>
bool inside_subarray(const T* coords, const T* subarray, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i)
    if (coords[i] < subarray[2 * i] || coords[i] > subarray[2 * i + 1])
      return false;

  return true;
}

template <class T>
bool intersect(const std::vector<T>& v1, const std::vector<T>& v2) {
  std::set<T> s1(v1.begin(), v1.end());
  std::set<T> s2(v2.begin(), v2.end());
  std::vector<T> intersect;
  std::set_intersection(
      s1.begin(),
      s1.end(),
      s2.begin(),
      s2.end(),
      std::back_inserter(intersect));

  return !intersect.empty();
}

template <class T>
bool rect_in_rect(const T* a, const T* b, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i)
    if (a[2 * i] < b[2 * i] || a[2 * i + 1] > b[2 * i + 1])
      return false;

  return true;
}

bool is_positive_integer(const char* s) {
  int i = 0;

  if (s[0] == '-')  // negative
    return false;

  if (s[0] == '0' && s[1] == '\0')  // equal to 0
    return false;

  if (s[0] == '+')
    i = 1;  // Skip the first character if it is the + sign

  for (; s[i] != '\0'; ++i) {
    if (isdigit(s[i]) == 0)
      return false;
  }

  return true;
}

template <class T>
bool is_unary_subarray(const T* subarray, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i) {
    if (subarray[2 * i] != subarray[2 * i + 1])
      return false;
  }

  return true;
}

template <class T>
bool overlap(const T* a, const T* b, unsigned dim_num) {
  for (unsigned i = 0; i < dim_num; ++i) {
    if (a[2 * i] > b[2 * i + 1] || a[2 * i + 1] < b[2 * i])
      return false;
  }

  return true;
}

bool starts_with(const std::string& value, const std::string& prefix) {
  if (prefix.size() > value.size())
    return false;
  return std::equal(prefix.begin(), prefix.end(), value.begin());
}

std::string tile_extent_str(const void* tile_extent, Datatype type) {
  std::stringstream ss;

  if (tile_extent == nullptr)
    return constants::null_str;

  const int* tile_extent_int32;
  const int64_t* tile_extent_int64;
  const float* tile_extent_float32;
  const double* tile_extent_float64;
  const int8_t* tile_extent_int8;
  const uint8_t* tile_extent_uint8;
  const int16_t* tile_extent_int16;
  const uint16_t* tile_extent_uint16;
  const uint32_t* tile_extent_uint32;
  const uint64_t* tile_extent_uint64;

  switch (type) {
    case Datatype::INT32:
      tile_extent_int32 = static_cast<const int*>(tile_extent);
      ss << *tile_extent_int32;
      return ss.str();
    case Datatype::INT64:
      tile_extent_int64 = static_cast<const int64_t*>(tile_extent);
      ss << *tile_extent_int64;
      return ss.str();
    case Datatype::FLOAT32:
      tile_extent_float32 = static_cast<const float*>(tile_extent);
      ss << *tile_extent_float32;
      return ss.str();
    case Datatype::FLOAT64:
      tile_extent_float64 = static_cast<const double*>(tile_extent);
      ss << *tile_extent_float64;
      return ss.str();
    case Datatype::INT8:
      tile_extent_int8 = static_cast<const int8_t*>(tile_extent);
      ss << int(*tile_extent_int8);
      return ss.str();
    case Datatype::UINT8:
      tile_extent_uint8 = static_cast<const uint8_t*>(tile_extent);
      ss << int(*tile_extent_uint8);
      return ss.str();
    case Datatype::INT16:
      tile_extent_int16 = static_cast<const int16_t*>(tile_extent);
      ss << *tile_extent_int16;
      return ss.str();
    case Datatype::UINT16:
      tile_extent_uint16 = static_cast<const uint16_t*>(tile_extent);
      ss << *tile_extent_uint16;
      return ss.str();
    case Datatype::UINT32:
      tile_extent_uint32 = static_cast<const uint32_t*>(tile_extent);
      ss << *tile_extent_uint32;
      return ss.str();
    case Datatype::UINT64:
      tile_extent_uint64 = static_cast<const uint64_t*>(tile_extent);
      ss << *tile_extent_uint64;
      return ss.str();
    case Datatype::CHAR:
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      // Not supported domain type
      assert(false);
      return "";
  }

  assert(false);
  return "";
}

uint64_t timestamp_ms() {
#ifdef _WIN32
  struct _timeb tb;
  memset(&tb, 0, sizeof(struct _timeb));
  _ftime_s(&tb);
  return static_cast<uint64_t>(tb.time * 1000L + tb.millitm);
#else
  struct timeval tp;
  memset(&tp, 0, sizeof(struct timeval));
  gettimeofday(&tp, nullptr);
  return static_cast<uint64_t>(tp.tv_sec * 1000L + tp.tv_usec / 1000);
#endif
}

uint64_t ceil(uint64_t x, uint64_t y) {
  if (y == 0)
    return 0;

  return x / y + (x % y != 0);
}

// Explicit template instantiations
template uint64_t cell_num_in_subarray<int>(
    const int* subarray, unsigned int dim_num);
template uint64_t cell_num_in_subarray<int64_t>(
    const int64_t* subarray, unsigned int dim_num);
template uint64_t cell_num_in_subarray<float>(
    const float* subarray, unsigned int dim_num);
template uint64_t cell_num_in_subarray<double>(
    const double* subarray, unsigned int dim_num);
template uint64_t cell_num_in_subarray<int8_t>(
    const int8_t* subarray, unsigned int dim_num);
template uint64_t cell_num_in_subarray<uint8_t>(
    const uint8_t* subarray, unsigned int dim_num);
template uint64_t cell_num_in_subarray<int16_t>(
    const int16_t* subarray, unsigned int dim_num);
template uint64_t cell_num_in_subarray<uint16_t>(
    const uint16_t* subarray, unsigned int dim_num);
template uint64_t cell_num_in_subarray<uint32_t>(
    const uint32_t* subarray, unsigned int dim_num);
template uint64_t cell_num_in_subarray<uint64_t>(
    const uint64_t* subarray, unsigned int dim_num);

template bool coords_in_rect<int>(
    const int* cell, const int* subarray, unsigned int dim_num);
template bool coords_in_rect<int64_t>(
    const int64_t* cell, const int64_t* subarray, unsigned int dim_num);
template bool coords_in_rect<float>(
    const float* cell, const float* subarray, unsigned int dim_num);
template bool coords_in_rect<double>(
    const double* cell, const double* subarray, unsigned int dim_num);
template bool coords_in_rect<int8_t>(
    const int8_t* cell, const int8_t* subarray, unsigned int dim_num);
template bool coords_in_rect<uint8_t>(
    const uint8_t* cell, const uint8_t* subarray, unsigned int dim_num);
template bool coords_in_rect<int16_t>(
    const int16_t* cell, const int16_t* subarray, unsigned int dim_num);
template bool coords_in_rect<uint16_t>(
    const uint16_t* cell, const uint16_t* subarray, unsigned int dim_num);
template bool coords_in_rect<uint32_t>(
    const uint32_t* cell, const uint32_t* subarray, unsigned int dim_num);
template bool coords_in_rect<uint64_t>(
    const uint64_t* cell, const uint64_t* subarray, unsigned int dim_num);

template int cmp_col_order<int>(
    const int* coords_a, const int* coords_b, unsigned int dim_num);
template int cmp_col_order<int64_t>(
    const int64_t* coords_a, const int64_t* coords_b, unsigned int dim_num);
template int cmp_col_order<float>(
    const float* coords_a, const float* coords_b, unsigned int dim_num);
template int cmp_col_order<double>(
    const double* coords_a, const double* coords_b, unsigned int dim_num);
template int cmp_col_order<int8_t>(
    const int8_t* coords_a, const int8_t* coords_b, unsigned int dim_num);
template int cmp_col_order<uint8_t>(
    const uint8_t* coords_a, const uint8_t* coords_b, unsigned int dim_num);
template int cmp_col_order<int16_t>(
    const int16_t* coords_a, const int16_t* coords_b, unsigned int dim_num);
template int cmp_col_order<uint16_t>(
    const uint16_t* coords_a, const uint16_t* coords_b, unsigned int dim_num);
template int cmp_col_order<uint32_t>(
    const uint32_t* coords_a, const uint32_t* coords_b, unsigned int dim_num);
template int cmp_col_order<uint64_t>(
    const uint64_t* coords_a, const uint64_t* coords_b, unsigned int dim_num);

template int cmp_col_order<int>(
    uint64_t id_a,
    const int* coords_a,
    uint64_t id_b,
    const int* coords_b,
    unsigned int dim_num);
template int cmp_col_order<int64_t>(
    uint64_t id_a,
    const int64_t* coords_a,
    uint64_t id_b,
    const int64_t* coords_b,
    unsigned int dim_num);
template int cmp_col_order<float>(
    uint64_t id_a,
    const float* coords_a,
    uint64_t id_b,
    const float* coords_b,
    unsigned int dim_num);
template int cmp_col_order<double>(
    uint64_t id_a,
    const double* coords_a,
    uint64_t id_b,
    const double* coords_b,
    unsigned int dim_num);
template int cmp_col_order<int8_t>(
    uint64_t id_a,
    const int8_t* coords_a,
    uint64_t id_b,
    const int8_t* coords_b,
    unsigned int dim_num);
template int cmp_col_order<uint8_t>(
    uint64_t id_a,
    const uint8_t* coords_a,
    uint64_t id_b,
    const uint8_t* coords_b,
    unsigned int dim_num);
template int cmp_col_order<int16_t>(
    uint64_t id_a,
    const int16_t* coords_a,
    uint64_t id_b,
    const int16_t* coords_b,
    unsigned int dim_num);
template int cmp_col_order<uint16_t>(
    uint64_t id_a,
    const uint16_t* coords_a,
    uint64_t id_b,
    const uint16_t* coords_b,
    unsigned int dim_num);
template int cmp_col_order<uint32_t>(
    uint64_t id_a,
    const uint32_t* coords_a,
    uint64_t id_b,
    const uint32_t* coords_b,
    unsigned int dim_num);
template int cmp_col_order<uint64_t>(
    uint64_t id_a,
    const uint64_t* coords_a,
    uint64_t id_b,
    const uint64_t* coords_b,
    unsigned int dim_num);

template int cmp_row_order<int>(
    const int* coords_a, const int* coords_b, unsigned int dim_num);
template int cmp_row_order<int64_t>(
    const int64_t* coords_a, const int64_t* coords_b, unsigned int dim_num);
template int cmp_row_order<float>(
    const float* coords_a, const float* coords_b, unsigned int dim_num);
template int cmp_row_order<double>(
    const double* coords_a, const double* coords_b, unsigned int dim_num);
template int cmp_row_order<int8_t>(
    const int8_t* coords_a, const int8_t* coords_b, unsigned int dim_num);
template int cmp_row_order<uint8_t>(
    const uint8_t* coords_a, const uint8_t* coords_b, unsigned int dim_num);
template int cmp_row_order<int16_t>(
    const int16_t* coords_a, const int16_t* coords_b, unsigned int dim_num);
template int cmp_row_order<uint16_t>(
    const uint16_t* coords_a, const uint16_t* coords_b, unsigned int dim_num);
template int cmp_row_order<uint32_t>(
    const uint32_t* coords_a, const uint32_t* coords_b, unsigned int dim_num);
template int cmp_row_order<uint64_t>(
    const uint64_t* coords_a, const uint64_t* coords_b, unsigned int dim_num);

template int cmp_row_order<int>(
    uint64_t id_a,
    const int* coords_a,
    uint64_t id_b,
    const int* coords_b,
    unsigned int dim_num);
template int cmp_row_order<int64_t>(
    uint64_t id_a,
    const int64_t* coords_a,
    uint64_t id_b,
    const int64_t* coords_b,
    unsigned int dim_num);
template int cmp_row_order<float>(
    uint64_t id_a,
    const float* coords_a,
    uint64_t id_b,
    const float* coords_b,
    unsigned int dim_num);
template int cmp_row_order<double>(
    uint64_t id_a,
    const double* coords_a,
    uint64_t id_b,
    const double* coords_b,
    unsigned int dim_num);
template int cmp_row_order<int8_t>(
    uint64_t id_a,
    const int8_t* coords_a,
    uint64_t id_b,
    const int8_t* coords_b,
    unsigned int dim_num);
template int cmp_row_order<uint8_t>(
    uint64_t id_a,
    const uint8_t* coords_a,
    uint64_t id_b,
    const uint8_t* coords_b,
    unsigned int dim_num);
template int cmp_row_order<int16_t>(
    uint64_t id_a,
    const int16_t* coords_a,
    uint64_t id_b,
    const int16_t* coords_b,
    unsigned int dim_num);
template int cmp_row_order<uint16_t>(
    uint64_t id_a,
    const uint16_t* coords_a,
    uint64_t id_b,
    const uint16_t* coords_b,
    unsigned int dim_num);
template int cmp_row_order<uint32_t>(
    uint64_t id_a,
    const uint32_t* coords_a,
    uint64_t id_b,
    const uint32_t* coords_b,
    unsigned int dim_num);
template int cmp_row_order<uint64_t>(
    uint64_t id_a,
    const uint64_t* coords_a,
    uint64_t id_b,
    const uint64_t* coords_b,
    unsigned int dim_num);

template void expand_mbr<int>(
    int* mbr, const int* coords, unsigned int dim_num);
template void expand_mbr<int64_t>(
    int64_t* mbr, const int64_t* coords, unsigned int dim_num);
template void expand_mbr<float>(
    float* mbr, const float* coords, unsigned int dim_num);
template void expand_mbr<double>(
    double* mbr, const double* coords, unsigned int dim_num);
template void expand_mbr<int8_t>(
    int8_t* mbr, const int8_t* coords, unsigned int dim_num);
template void expand_mbr<uint8_t>(
    uint8_t* mbr, const uint8_t* coords, unsigned int dim_num);
template void expand_mbr<int16_t>(
    int16_t* mbr, const int16_t* coords, unsigned int dim_num);
template void expand_mbr<uint16_t>(
    uint16_t* mbr, const uint16_t* coords, unsigned int dim_num);
template void expand_mbr<uint32_t>(
    uint32_t* mbr, const uint32_t* coords, unsigned int dim_num);
template void expand_mbr<uint64_t>(
    uint64_t* mbr, const uint64_t* coords, unsigned int dim_num);

template bool has_duplicates<std::string>(const std::vector<std::string>& v);

template bool inside_subarray<int>(
    const int* coords, const int* subarray, unsigned int dim_num);
template bool inside_subarray<int64_t>(
    const int64_t* coords, const int64_t* subarray, unsigned int dim_num);
template bool inside_subarray<float>(
    const float* coords, const float* subarray, unsigned int dim_num);
template bool inside_subarray<double>(
    const double* coords, const double* subarray, unsigned int dim_num);
template bool inside_subarray<int8_t>(
    const int8_t* coords, const int8_t* subarray, unsigned int dim_num);
template bool inside_subarray<uint8_t>(
    const uint8_t* coords, const uint8_t* subarray, unsigned int dim_num);
template bool inside_subarray<int16_t>(
    const int16_t* coords, const int16_t* subarray, unsigned int dim_num);
template bool inside_subarray<uint16_t>(
    const uint16_t* coords, const uint16_t* subarray, unsigned int dim_num);
template bool inside_subarray<uint32_t>(
    const uint32_t* coords, const uint32_t* subarray, unsigned int dim_num);
template bool inside_subarray<uint64_t>(
    const uint64_t* coords, const uint64_t* subarray, unsigned int dim_num);

template bool intersect<std::string>(
    const std::vector<std::string>& v1, const std::vector<std::string>& v2);

template bool rect_in_rect<int>(
    const int* range_A, const int* range_B, unsigned int dim_num);
template bool rect_in_rect<int64_t>(
    const int64_t* range_A, const int64_t* range_B, unsigned int dim_num);
template bool rect_in_rect<float>(
    const float* range_A, const float* range_B, unsigned int dim_num);
template bool rect_in_rect<double>(
    const double* range_A, const double* range_B, unsigned int dim_num);
template bool rect_in_rect<int8_t>(
    const int8_t* range_A, const int8_t* range_B, unsigned int dim_num);
template bool rect_in_rect<uint8_t>(
    const uint8_t* range_A, const uint8_t* range_B, unsigned int dim_num);
template bool rect_in_rect<int16_t>(
    const int16_t* range_A, const int16_t* range_B, unsigned int dim_num);
template bool rect_in_rect<uint16_t>(
    const uint16_t* range_A, const uint16_t* range_B, unsigned int dim_num);
template bool rect_in_rect<uint32_t>(
    const uint32_t* range_A, const uint32_t* range_B, unsigned int dim_num);
template bool rect_in_rect<uint64_t>(
    const uint64_t* range_A, const uint64_t* range_B, unsigned int dim_num);

template bool is_unary_subarray<int>(const int* subarray, unsigned int dim_num);
template bool is_unary_subarray<int64_t>(
    const int64_t* subarray, unsigned int dim_num);
template bool is_unary_subarray<float>(
    const float* subarray, unsigned int dim_num);
template bool is_unary_subarray<double>(
    const double* subarray, unsigned int dim_num);
template bool is_unary_subarray<int8_t>(
    const int8_t* subarray, unsigned int dim_num);
template bool is_unary_subarray<uint8_t>(
    const uint8_t* subarray, unsigned int dim_num);
template bool is_unary_subarray<int16_t>(
    const int16_t* subarray, unsigned int dim_num);
template bool is_unary_subarray<uint16_t>(
    const uint16_t* subarray, unsigned int dim_num);
template bool is_unary_subarray<uint32_t>(
    const uint32_t* subarray, unsigned int dim_num);
template bool is_unary_subarray<uint64_t>(
    const uint64_t* subarray, unsigned int dim_num);

template bool overlap<int8_t>(
    const int8_t* a, const int8_t* b, unsigned dim_num);
template bool overlap<uint8_t>(
    const uint8_t* a, const uint8_t* b, unsigned dim_num);
template bool overlap<int16_t>(
    const int16_t* a, const int16_t* b, unsigned dim_num);
template bool overlap<uint16_t>(
    const uint16_t* a, const uint16_t* b, unsigned dim_num);
template bool overlap<int>(const int* a, const int* b, unsigned dim_num);
template bool overlap<unsigned>(
    const unsigned* a, const unsigned* b, unsigned dim_num);
template bool overlap<int64_t>(
    const int64_t* a, const int64_t* b, unsigned dim_num);
template bool overlap<uint64_t>(
    const uint64_t* a, const uint64_t* b, unsigned dim_num);
template bool overlap<float>(const float* a, const float* b, unsigned dim_num);
template bool overlap<double>(
    const double* a, const double* b, unsigned dim_num);

}  // namespace utils

}  // namespace sm
}  // namespace tiledb
