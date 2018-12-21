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

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {
namespace sm {

namespace utils {

namespace parse {

/* ********************************* */
/*          PARSING FUNCTIONS        */
/* ********************************* */

Status convert(const std::string& str, int* value) {
  if (!is_int(str))
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to int; Invalid argument"));

  try {
    *value = std::stoi(str);
  } catch (std::invalid_argument& e) {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to int; Invalid argument"));
  } catch (std::out_of_range& e) {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to int; Value out of range"));
  }

  return Status::Ok();
}

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

Status convert(const std::string& str, float* value) {
  try {
    *value = std::stof(str);
  } catch (std::invalid_argument& e) {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to float32_t; Invalid argument"));
  } catch (std::out_of_range& e) {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to float32_t; Value out of range"));
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

bool starts_with(const std::string& value, const std::string& prefix) {
  if (prefix.size() > value.size())
    return false;
  return std::equal(prefix.begin(), prefix.end(), value.begin());
}

bool ends_with(const std::string& value, const std::string& suffix) {
  if (suffix.size() > value.size())
    return false;
  return value.compare(value.size() - suffix.size(), suffix.size(), suffix) ==
         0;
}

}  // namespace parse

/* ****************************** */
/*         TYPE FUNCTIONS         */
/* ****************************** */

namespace datatype {

template <>
Status check_template_type_to_datatype<int8_t>(Datatype datatype) {
  if (datatype == Datatype::INT8)
    return Status::Ok();
  return Status::Error(
      "Template of type int8_t but datatype is not Datatype::INT8");
}
template <>
Status check_template_type_to_datatype<uint8_t>(Datatype datatype) {
  if (datatype == Datatype::UINT8)
    return Status::Ok();
  else if (datatype == Datatype::STRING_ASCII)
    return Status::Ok();
  else if (datatype == Datatype::STRING_UTF8)
    return Status::Ok();

  return Status::Error(
      "Template of type uint8_t but datatype is not Datatype::UINT8 nor "
      "Datatype::STRING_ASCII nor atatype::STRING_UTF8");
}
template <>
Status check_template_type_to_datatype<int16_t>(Datatype datatype) {
  if (datatype == Datatype::INT16)
    return Status::Ok();
  return Status::Error(
      "Template of type int16_t but datatype is not Datatype::INT16");
}
template <>
Status check_template_type_to_datatype<uint16_t>(Datatype datatype) {
  if (datatype == Datatype::UINT16)
    return Status::Ok();
  else if (datatype == Datatype::STRING_UTF16)
    return Status::Ok();
  else if (datatype == Datatype::STRING_UCS2)
    return Status::Ok();
  return Status::Error(
      "Template of type uint16_t but datatype is not Datatype::UINT16 nor "
      "Datatype::STRING_UTF16 nor Datatype::STRING_UCS2");
}
template <>
Status check_template_type_to_datatype<int32_t>(Datatype datatype) {
  if (datatype == Datatype::INT32)
    return Status::Ok();
  return Status::Error(
      "Template of type int32_t but datatype is not Datatype::INT32");
}
template <>
Status check_template_type_to_datatype<uint32_t>(Datatype datatype) {
  if (datatype == Datatype::UINT32)
    return Status::Ok();
  else if (datatype == Datatype::STRING_UTF32)
    return Status::Ok();
  else if (datatype == Datatype::STRING_UCS4)
    return Status::Ok();
  return Status::Error(
      "Template of type uint32_t but datatype is not Datatype::UINT32 nor "
      "Datatype::STRING_UTF32 nor Datatype::STRING_UCS4");
}
template <>
Status check_template_type_to_datatype<int64_t>(Datatype datatype) {
  if (datatype == Datatype::INT64)
    return Status::Ok();
  return Status::Error(
      "Template of type int64_t but datatype is not Datatype::INT64");
}
template <>
Status check_template_type_to_datatype<uint64_t>(Datatype datatype) {
  if (datatype == Datatype::UINT64)
    return Status::Ok();
  return Status::Error(
      "Template of type uint64_t but datatype is not Datatype::UINT64");
}
template <>
Status check_template_type_to_datatype<float>(Datatype datatype) {
  if (datatype == Datatype::FLOAT32)
    return Status::Ok();
  return Status::Error(
      "Template of type float but datatype is not Datatype::FLOAT32");
}
template <>
Status check_template_type_to_datatype<double>(Datatype datatype) {
  if (datatype == Datatype::FLOAT64)
    return Status::Ok();
  return Status::Error(
      "Template of type double but datatype is not Datatype::FLOAT64");
}
template <>
Status check_template_type_to_datatype<char>(Datatype datatype) {
  if (datatype == Datatype::CHAR)
    return Status::Ok();
  return Status::Error(
      "Template of type char but datatype is not Datatype::CHAR");
}

}  // namespace datatype

/* ********************************* */
/*        GEOMETRY FUNCTIONS         */
/* ********************************* */

namespace geometry {

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
bool overlap(const T* a, const T* b, unsigned dim_num) {
  for (unsigned i = 0; i < dim_num; ++i) {
    if (a[2 * i] > b[2 * i + 1] || a[2 * i + 1] < b[2 * i])
      return false;
  }

  return true;
}

template <class T>
bool overlap(const T* a, const T* b, unsigned dim_num, bool* a_contains_b) {
  for (unsigned i = 0; i < dim_num; ++i) {
    if (a[2 * i] > b[2 * i + 1] || a[2 * i + 1] < b[2 * i])
      return false;
  }

  *a_contains_b = true;
  for (unsigned i = 0; i < dim_num; ++i) {
    if (a[2 * i] > b[2 * i] || a[2 * i + 1] < b[2 * i + 1]) {
      *a_contains_b = false;
      break;
    }
  }

  return true;
}

template <class T>
void overlap(const T* a, const T* b, unsigned dim_num, T* o, bool* overlap) {
  // Get overlap range
  *overlap = true;
  for (unsigned int i = 0; i < dim_num; ++i) {
    o[2 * i] = MAX(a[2 * i], b[2 * i]);
    o[2 * i + 1] = MIN(a[2 * i + 1], b[2 * i + 1]);
    if (o[2 * i] > b[2 * i + 1] || o[2 * i + 1] < b[2 * i]) {
      *overlap = false;
      break;
    }
  }
}

template <class T>
double coverage(const T* a, const T* b, unsigned dim_num) {
  double c = 1.0;
  auto add = int(std::is_integral<T>::value);

  for (unsigned i = 0; i < dim_num; ++i) {
    if (b[2 * i] == b[2 * i + 1]) {
      c *= 1;
    } else {
      auto a_range = double(a[2 * i + 1]) - a[2 * i] + add;
      auto b_range = double(b[2 * i + 1]) - b[2 * i] + add;
      c *= a_range / b_range;
    }
  }
  return c;
}

}  // namespace geometry

/* ********************************* */
/*          TIME FUNCTIONS           */
/* ********************************* */

namespace time {

uint64_t timestamp_now_ms() {
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

}  // namespace time

/* ********************************* */
/*          MATH FUNCTIONS           */
/* ********************************* */

namespace math {

uint64_t ceil(uint64_t x, uint64_t y) {
  if (y == 0)
    return 0;

  return x / y + (x % y != 0);
}

}  // namespace math

// Explicit template instantiations

namespace geometry {

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

template bool overlap<int8_t>(
    const int8_t* a, const int8_t* b, unsigned dim_num, bool* a_contains_b);
template bool overlap<uint8_t>(
    const uint8_t* a, const uint8_t* b, unsigned dim_num, bool* a_contains_b);
template bool overlap<int16_t>(
    const int16_t* a, const int16_t* b, unsigned dim_num, bool* a_contains_b);
template bool overlap<uint16_t>(
    const uint16_t* a, const uint16_t* b, unsigned dim_num, bool* a_contains_b);
template bool overlap<int>(
    const int* a, const int* b, unsigned dim_num, bool* a_contains_b);
template bool overlap<unsigned>(
    const unsigned* a, const unsigned* b, unsigned dim_num, bool* a_contains_b);
template bool overlap<int64_t>(
    const int64_t* a, const int64_t* b, unsigned dim_num, bool* a_contains_b);
template bool overlap<uint64_t>(
    const uint64_t* a, const uint64_t* b, unsigned dim_num, bool* a_contains_b);
template bool overlap<float>(
    const float* a, const float* b, unsigned dim_num, bool* a_contains_b);
template bool overlap<double>(
    const double* a, const double* b, unsigned dim_num, bool* a_contains_b);

template void overlap<int>(
    const int* a, const int* b, unsigned dim_num, int* o, bool* overlap);
template void overlap<int64_t>(
    const int64_t* a,
    const int64_t* b,
    unsigned dim_num,
    int64_t* o,
    bool* overlap);
template void overlap<float>(
    const float* a, const float* b, unsigned dim_num, float* o, bool* overlap);
template void overlap<double>(
    const double* a,
    const double* b,
    unsigned dim_num,
    double* o,
    bool* overlap);
template void overlap<int8_t>(
    const int8_t* a,
    const int8_t* b,
    unsigned dim_num,
    int8_t* o,
    bool* overlap);
template void overlap<uint8_t>(
    const uint8_t* a,
    const uint8_t* b,
    unsigned dim_num,
    uint8_t* o,
    bool* overlap);
template void overlap<int16_t>(
    const int16_t* a,
    const int16_t* b,
    unsigned dim_num,
    int16_t* o,
    bool* overlap);
template void overlap<uint16_t>(
    const uint16_t* a,
    const uint16_t* b,
    unsigned dim_num,
    uint16_t* o,
    bool* overlap);
template void overlap<uint32_t>(
    const uint32_t* a,
    const uint32_t* b,
    unsigned dim_num,
    uint32_t* o,
    bool* overlap);
template void overlap<uint64_t>(
    const uint64_t* a,
    const uint64_t* b,
    unsigned dim_num,
    uint64_t* o,
    bool* overlap);

template double coverage<int8_t>(
    const int8_t* a, const int8_t* b, unsigned dim_num);
template double coverage<uint8_t>(
    const uint8_t* a, const uint8_t* b, unsigned dim_num);
template double coverage<int16_t>(
    const int16_t* a, const int16_t* b, unsigned dim_num);
template double coverage<uint16_t>(
    const uint16_t* a, const uint16_t* b, unsigned dim_num);
template double coverage<int>(const int* a, const int* b, unsigned dim_num);
template double coverage<unsigned>(
    const unsigned* a, const unsigned* b, unsigned dim_num);
template double coverage<int64_t>(
    const int64_t* a, const int64_t* b, unsigned dim_num);
template double coverage<uint64_t>(
    const uint64_t* a, const uint64_t* b, unsigned dim_num);
template double coverage<float>(
    const float* a, const float* b, unsigned dim_num);
template double coverage<double>(
    const double* a, const double* b, unsigned dim_num);

}  // namespace geometry

}  // namespace utils

}  // namespace sm
}  // namespace tiledb
