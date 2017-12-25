/**
 * @file   utils.cc
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
 * This file implements useful (global) functions.
 */

#include "utils.h"
#include "logger.h"

#include <iostream>
#include <set>
#include <sstream>

namespace tiledb {

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
inline bool cell_in_subarray(
    const T* cell, const T* subarray, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i) {
    if (cell[i] >= subarray[2 * i] && cell[i] <= subarray[2 * i + 1])
      continue;  // Inside this dimension domain

    return false;  // NOT inside this dimension domain
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

  if (type == Datatype::INT32) {
    auto domain_ = static_cast<const int*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  }

  if (type == Datatype::INT64) {
    auto domain_ = static_cast<const int64_t*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  }

  if (type == Datatype::FLOAT32) {
    auto domain_ = static_cast<const float*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  }

  if (type == Datatype::FLOAT64) {
    auto domain_ = static_cast<const double*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  }

  if (type == Datatype::CHAR) {
    auto domain_ = static_cast<const char*>(domain);
    ss << "[" << int(domain_[0]) << "," << int(domain_[1]) << "]";
    return ss.str();
  }

  if (type == Datatype::INT8) {
    auto domain_ = static_cast<const int8_t*>(domain);
    ss << "[" << int(domain_[0]) << "," << int(domain_[1]) << "]";
    return ss.str();
  }

  if (type == Datatype::UINT8) {
    auto domain_ = static_cast<const uint8_t*>(domain);
    ss << "[" << int(domain_[0]) << "," << int(domain_[1]) << "]";
    return ss.str();
  }

  if (type == Datatype::INT16) {
    auto domain_ = static_cast<const int16_t*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  }

  if (type == Datatype::UINT16) {
    auto domain_ = static_cast<const uint16_t*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  }

  if (type == Datatype::UINT32) {
    auto domain_ = static_cast<const uint32_t*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  }

  if (type == Datatype::UINT64) {
    auto domain_ = static_cast<const uint64_t*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  }

  return "";
}

Status expand_buffer(void*& buffer, uint64_t* buffer_allocated_size) {
  *buffer_allocated_size *= 2;
  buffer = std::realloc(buffer, *buffer_allocated_size);
  if (buffer == nullptr) {
    return LOG_STATUS(Status::MemError("Cannot reallocate buffer"));
  }
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
bool is_contained(const T* range_A, const T* range_B, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i)
    if (range_A[2 * i] < range_B[2 * i] ||
        range_A[2 * i + 1] > range_B[2 * i + 1])
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

bool starts_with(const std::string& value, const std::string& prefix) {
  if (prefix.size() > value.size())
    return false;
  return std::equal(prefix.begin(), prefix.end(), value.begin());
}

std::string tile_extent_str(const void* tile_extent, Datatype type) {
  std::stringstream ss;

  if (tile_extent == nullptr) {
    return constants::null_str;
  }

  if (type == Datatype::INT32) {
    auto tile_extent_ = static_cast<const int*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  }

  if (type == Datatype::INT64) {
    auto tile_extent_ = static_cast<const int64_t*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  }

  if (type == Datatype::FLOAT32) {
    auto tile_extent_ = static_cast<const float*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  }

  if (type == Datatype::FLOAT64) {
    auto tile_extent_ = static_cast<const double*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  }

  if (type == Datatype::CHAR) {
    auto tile_extent_ = static_cast<const char*>(tile_extent);
    ss << int(*tile_extent_);
    return ss.str();
  }

  if (type == Datatype::INT8) {
    auto tile_extent_ = static_cast<const int8_t*>(tile_extent);
    ss << int(*tile_extent_);
    return ss.str();
  }

  if (type == Datatype::UINT8) {
    auto tile_extent_ = static_cast<const uint8_t*>(tile_extent);
    ss << int(*tile_extent_);
    return ss.str();
  }

  if (type == Datatype::INT16) {
    auto tile_extent_ = static_cast<const int16_t*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  }

  if (type == Datatype::UINT16) {
    auto tile_extent_ = static_cast<const uint16_t*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  }

  if (type == Datatype::UINT32) {
    auto tile_extent_ = static_cast<const uint32_t*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  }

  if (type == Datatype::UINT64) {
    auto tile_extent_ = static_cast<const uint64_t*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  }

  return "";
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

template bool cell_in_subarray<int>(
    const int* cell, const int* subarray, unsigned int dim_num);
template bool cell_in_subarray<int64_t>(
    const int64_t* cell, const int64_t* subarray, unsigned int dim_num);
template bool cell_in_subarray<float>(
    const float* cell, const float* subarray, unsigned int dim_num);
template bool cell_in_subarray<double>(
    const double* cell, const double* subarray, unsigned int dim_num);
template bool cell_in_subarray<int8_t>(
    const int8_t* cell, const int8_t* subarray, unsigned int dim_num);
template bool cell_in_subarray<uint8_t>(
    const uint8_t* cell, const uint8_t* subarray, unsigned int dim_num);
template bool cell_in_subarray<int16_t>(
    const int16_t* cell, const int16_t* subarray, unsigned int dim_num);
template bool cell_in_subarray<uint16_t>(
    const uint16_t* cell, const uint16_t* subarray, unsigned int dim_num);
template bool cell_in_subarray<uint32_t>(
    const uint32_t* cell, const uint32_t* subarray, unsigned int dim_num);
template bool cell_in_subarray<uint64_t>(
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

template bool is_contained<int>(
    const int* range_A, const int* range_B, unsigned int dim_num);
template bool is_contained<int64_t>(
    const int64_t* range_A, const int64_t* range_B, unsigned int dim_num);
template bool is_contained<float>(
    const float* range_A, const float* range_B, unsigned int dim_num);
template bool is_contained<double>(
    const double* range_A, const double* range_B, unsigned int dim_num);
template bool is_contained<int8_t>(
    const int8_t* range_A, const int8_t* range_B, unsigned int dim_num);
template bool is_contained<uint8_t>(
    const uint8_t* range_A, const uint8_t* range_B, unsigned int dim_num);
template bool is_contained<int16_t>(
    const int16_t* range_A, const int16_t* range_B, unsigned int dim_num);
template bool is_contained<uint16_t>(
    const uint16_t* range_A, const uint16_t* range_B, unsigned int dim_num);
template bool is_contained<uint32_t>(
    const uint32_t* range_A, const uint32_t* range_B, unsigned int dim_num);
template bool is_contained<uint64_t>(
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

}  // namespace utils

}  // namespace tiledb
