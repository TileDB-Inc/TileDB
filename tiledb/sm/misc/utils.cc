/**
 * @file   utils.cc
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
 * This file implements useful (global) functions.
 */

#include "tiledb/sm/misc/utils.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/constants.h"

#include <algorithm>
#include <iostream>
#include <set>
#include <sstream>

#ifdef __linux__
#include "tiledb/sm/filesystem/posix.h"
#endif

using namespace tiledb::common;

namespace tiledb {
namespace sm {

namespace utils {

/* ****************************** */
/*         TYPE FUNCTIONS         */
/* ****************************** */

namespace datatype {

template <>
Status check_template_type_to_datatype<int8_t>(Datatype datatype) {
  if (datatype == Datatype::INT8)
    return Status::Ok();
  return Status_Error(
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

  return Status_Error(
      "Template of type uint8_t but datatype is not Datatype::UINT8 nor "
      "Datatype::STRING_ASCII nor atatype::STRING_UTF8");
}
template <>
Status check_template_type_to_datatype<int16_t>(Datatype datatype) {
  if (datatype == Datatype::INT16)
    return Status::Ok();
  return Status_Error(
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
  return Status_Error(
      "Template of type uint16_t but datatype is not Datatype::UINT16 nor "
      "Datatype::STRING_UTF16 nor Datatype::STRING_UCS2");
}
template <>
Status check_template_type_to_datatype<int32_t>(Datatype datatype) {
  if (datatype == Datatype::INT32)
    return Status::Ok();
  return Status_Error(
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
  return Status_Error(
      "Template of type uint32_t but datatype is not Datatype::UINT32 nor "
      "Datatype::STRING_UTF32 nor Datatype::STRING_UCS4");
}
template <>
Status check_template_type_to_datatype<int64_t>(Datatype datatype) {
  if (datatype == Datatype::INT64)
    return Status::Ok();
  return Status_Error(
      "Template of type int64_t but datatype is not Datatype::INT64");
}
template <>
Status check_template_type_to_datatype<uint64_t>(Datatype datatype) {
  if (datatype == Datatype::UINT64)
    return Status::Ok();
  return Status_Error(
      "Template of type uint64_t but datatype is not Datatype::UINT64");
}
template <>
Status check_template_type_to_datatype<float>(Datatype datatype) {
  if (datatype == Datatype::FLOAT32)
    return Status::Ok();
  return Status_Error(
      "Template of type float but datatype is not Datatype::FLOAT32");
}
template <>
Status check_template_type_to_datatype<double>(Datatype datatype) {
  if (datatype == Datatype::FLOAT64)
    return Status::Ok();
  return Status_Error(
      "Template of type double but datatype is not Datatype::FLOAT64");
}
template <>
Status check_template_type_to_datatype<char>(Datatype datatype) {
  if (datatype == Datatype::CHAR)
    return Status::Ok();
  return Status_Error(
      "Template of type char but datatype is not Datatype::CHAR");
}

}  // namespace datatype

/* ********************************* */
/*        GEOMETRY FUNCTIONS         */
/* ********************************* */

namespace geometry {

template <class T>
inline bool coords_in_rect(
    const T* coords, const std::vector<const T*>& rect, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i) {
    if (coords[i] < rect[i][0] || coords[i] > rect[i][1])
      return false;
  }

  return true;
}

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
bool overlap(const T* a, const T* b, unsigned dim_num) {
  for (unsigned i = 0; i < dim_num; ++i) {
    if (a[2 * i] > b[2 * i + 1] || a[2 * i + 1] < b[2 * i])
      return false;
  }

  return true;
}

template <class T>
void overlap(const T* a, const T* b, unsigned dim_num, T* o, bool* overlap) {
  // Get overlap range
  *overlap = true;
  for (unsigned int i = 0; i < dim_num; ++i) {
    o[2 * i] = std::max(a[2 * i], b[2 * i]);
    o[2 * i + 1] = std::min(a[2 * i + 1], b[2 * i + 1]);
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
      if (std::is_integral<T>::value) {
        auto max = std::numeric_limits<T>::max();
        if (a_range == 0)
          a_range = std::nextafter(a_range, max);
        if (b_range == 0)
          b_range = std::nextafter(b_range, max);
      }
      c *= a_range / b_range;
    }
  }
  return c;
}

template <class T>
std::vector<std::array<T, 2>> intersection(
    const std::vector<std::array<T, 2>>& r1,
    const std::vector<std::array<T, 2>>& r2) {
  auto dim_num = r1.size();
  assert(r2.size() == dim_num);

  std::vector<std::array<T, 2>> ret(dim_num);
  for (size_t d = 0; d < dim_num; ++d)
    ret[d] = {std::max(r1[d][0], r2[d][0]), std::min(r1[d][1], r2[d][1])};

  return ret;
}

}  // namespace geometry

// Explicit template instantiations

namespace geometry {

template bool coords_in_rect<int>(
    const int* corrds, const int* subarray, unsigned int dim_num);
template bool coords_in_rect<int64_t>(
    const int64_t* corrds, const int64_t* subarray, unsigned int dim_num);
template bool coords_in_rect<float>(
    const float* coords, const float* subarray, unsigned int dim_num);
template bool coords_in_rect<double>(
    const double* coords, const double* subarray, unsigned int dim_num);
template bool coords_in_rect<int8_t>(
    const int8_t* coords, const int8_t* subarray, unsigned int dim_num);
template bool coords_in_rect<uint8_t>(
    const uint8_t* coords, const uint8_t* subarray, unsigned int dim_num);
template bool coords_in_rect<int16_t>(
    const int16_t* coords, const int16_t* subarray, unsigned int dim_num);
template bool coords_in_rect<uint16_t>(
    const uint16_t* coords, const uint16_t* subarray, unsigned int dim_num);
template bool coords_in_rect<uint32_t>(
    const uint32_t* coords, const uint32_t* subarray, unsigned int dim_num);
template bool coords_in_rect<uint64_t>(
    const uint64_t* coords, const uint64_t* subarray, unsigned int dim_num);

template bool coords_in_rect<int>(
    const int* corrds,
    const std::vector<const int*>& subarray,
    unsigned int dim_num);
template bool coords_in_rect<int64_t>(
    const int64_t* corrds,
    const std::vector<const int64_t*>& subarray,
    unsigned int dim_num);
template bool coords_in_rect<float>(
    const float* coords,
    const std::vector<const float*>& subarray,
    unsigned int dim_num);
template bool coords_in_rect<double>(
    const double* coords,
    const std::vector<const double*>& subarray,
    unsigned int dim_num);
template bool coords_in_rect<int8_t>(
    const int8_t* coords,
    const std::vector<const int8_t*>& subarray,
    unsigned int dim_num);
template bool coords_in_rect<uint8_t>(
    const uint8_t* coords,
    const std::vector<const uint8_t*>& subarray,
    unsigned int dim_num);
template bool coords_in_rect<int16_t>(
    const int16_t* coords,
    const std::vector<const int16_t*>& subarray,
    unsigned int dim_num);
template bool coords_in_rect<uint16_t>(
    const uint16_t* coords,
    const std::vector<const uint16_t*>& subarray,
    unsigned int dim_num);
template bool coords_in_rect<uint32_t>(
    const uint32_t* coords,
    const std::vector<const uint32_t*>& subarray,
    unsigned int dim_num);
template bool coords_in_rect<uint64_t>(
    const uint64_t* coords,
    const std::vector<const uint64_t*>& subarray,
    unsigned int dim_num);

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

template std::vector<std::array<int8_t, 2>> intersection<int8_t>(
    const std::vector<std::array<int8_t, 2>>& r1,
    const std::vector<std::array<int8_t, 2>>& r2);
template std::vector<std::array<uint8_t, 2>> intersection<uint8_t>(
    const std::vector<std::array<uint8_t, 2>>& r1,
    const std::vector<std::array<uint8_t, 2>>& r2);
template std::vector<std::array<int16_t, 2>> intersection<int16_t>(
    const std::vector<std::array<int16_t, 2>>& r1,
    const std::vector<std::array<int16_t, 2>>& r2);
template std::vector<std::array<uint16_t, 2>> intersection<uint16_t>(
    const std::vector<std::array<uint16_t, 2>>& r1,
    const std::vector<std::array<uint16_t, 2>>& r2);
template std::vector<std::array<int32_t, 2>> intersection<int32_t>(
    const std::vector<std::array<int32_t, 2>>& r1,
    const std::vector<std::array<int32_t, 2>>& r2);
template std::vector<std::array<uint32_t, 2>> intersection<uint32_t>(
    const std::vector<std::array<uint32_t, 2>>& r1,
    const std::vector<std::array<uint32_t, 2>>& r2);
template std::vector<std::array<int64_t, 2>> intersection<int64_t>(
    const std::vector<std::array<int64_t, 2>>& r1,
    const std::vector<std::array<int64_t, 2>>& r2);
template std::vector<std::array<uint64_t, 2>> intersection<uint64_t>(
    const std::vector<std::array<uint64_t, 2>>& r1,
    const std::vector<std::array<uint64_t, 2>>& r2);

}  // namespace geometry
}  // namespace utils
}  // namespace sm
}  // namespace tiledb
