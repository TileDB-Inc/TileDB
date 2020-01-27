/**
 * @file   utils.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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

#ifdef __linux__
namespace https {
std::string find_ca_certs_linux(const Posix& posix) {
  // Check ever cert file location to see if the certificate exists
  for (const std::string& cert : constants::cert_files_linux) {
    // Check if the file exists, any errors are treated as the file not existing
    if (posix.is_file(cert)) {
      return cert;
    }
  }
  // Could not find the ca bundle
  return "";
}
}  // namespace https
#endif

namespace parse {

/* ********************************* */
/*          PARSING FUNCTIONS        */
/* ********************************* */

Status convert(const std::string& str, int* value) {
  if (!is_int(str)) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int; Invalid argument";
    return LOG_STATUS(Status::UtilsError(errmsg));
  }

  try {
    *value = std::stoi(str);
  } catch (std::invalid_argument& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int; Invalid argument";
    return LOG_STATUS(Status::UtilsError(errmsg));
  } catch (std::out_of_range& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int; Value out of range";
    return LOG_STATUS(Status::UtilsError(errmsg));
  }

  return Status::Ok();
}

Status convert(const std::string& str, uint32_t* value) {
  if (!is_uint(str)) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint32_t; Invalid argument";
    return LOG_STATUS(Status::UtilsError(errmsg));
  }

  try {
    auto v = std::stoul(str);
    if (v > UINT32_MAX)
      throw std::out_of_range("Cannot convert long to unsigned int");
    *value = (uint32_t)v;
  } catch (std::invalid_argument& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint32_t; Invalid argument";
    return LOG_STATUS(Status::UtilsError(errmsg));
  } catch (std::out_of_range& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint32_t; Value out of range";
    return LOG_STATUS(Status::UtilsError(errmsg));
  }

  return Status::Ok();
}

Status convert(const std::string& str, uint64_t* value) {
  if (!is_uint(str)) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint64_t; Invalid argument";
    return LOG_STATUS(Status::UtilsError(errmsg));
  }

  try {
    *value = std::stoull(str);
  } catch (std::invalid_argument& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint64_t; Invalid argument";
    return LOG_STATUS(Status::UtilsError(errmsg));
  } catch (std::out_of_range& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint64_t; Value out of range";
    return LOG_STATUS(Status::UtilsError(errmsg));
  }

  return Status::Ok();
}

Status convert(const std::string& str, int64_t* value) {
  if (!is_int(str)) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int64_t; Invalid argument";
    return LOG_STATUS(Status::UtilsError(errmsg));
  }

  try {
    *value = std::stoll(str);
  } catch (std::invalid_argument& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int64_t; Invalid argument";
    return LOG_STATUS(Status::UtilsError(errmsg));
  } catch (std::out_of_range& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int64_t; Value out of range";
    return LOG_STATUS(Status::UtilsError(errmsg));
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

Status convert(const std::string& str, double* value) {
  try {
    *value = std::stod(str);
  } catch (std::invalid_argument& e) {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to float64_t; Invalid argument"));
  } catch (std::out_of_range& e) {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to float64_t; Value out of range"));
  }

  return Status::Ok();
}

Status convert(const std::string& str, bool* value) {
  std::string lvalue = str;
  std::transform(lvalue.begin(), lvalue.end(), lvalue.begin(), ::tolower);
  if (lvalue == "true") {
    *value = true;
  } else if (lvalue == "false") {
    *value = false;
  } else {
    return LOG_STATUS(Status::UtilsError(
        "Failed to convert string to bool; Value not 'true' or 'false'"));
  }

  return Status::Ok();
}

Status convert(const std::string& str, SerializationType* value) {
  std::string lvalue = str;
  std::transform(lvalue.begin(), lvalue.end(), lvalue.begin(), ::tolower);
  if (lvalue == "json") {
    *value = SerializationType::JSON;
  } else if (lvalue == "capnp") {
    *value = SerializationType::CAPNP;
  } else {
    return LOG_STATUS(
        Status::UtilsError("Failed to convert string to SerializationType; "
                           "Value not 'json' or 'capnp'"));
  }

  return Status::Ok();
}

std::pair<uint64_t, uint64_t> get_timestamp_range(
    uint32_t version, const std::string& fragment_name) {
  std::pair<uint64_t, uint64_t> ret = {0, 0};
  if (version == 1) {  // This is equivalent to format version <=2
    assert(fragment_name.find_last_of('_') != std::string::npos);
    auto t_str = fragment_name.substr(fragment_name.find_last_of('_') + 1);
    sscanf(
        t_str.c_str(),
        (std::string("%") + std::string(PRId64)).c_str(),
        (long long int*)&ret.first);
    ret.second = ret.first;
  } else {
    assert(fragment_name.find_last_of('_') != std::string::npos);
    sscanf(
        fragment_name.c_str(),
        (std::string("__%") + std::string(PRId64) + "_%" + std::string(PRId64))
            .c_str(),
        (long long int*)&ret.first,
        (long long int*)&ret.second);
  }

  return ret;
}

Status get_fragment_name_version(
    const std::string& fragment_name, uint32_t* version) {
  auto t_str = fragment_name.substr(fragment_name.find_last_of('_') + 1);

  // The newest version has the 32-byte long UUID at the end
  *version = (t_str.size() == 32) ? 2 : 1;

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
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      domain_int64 = static_cast<const int64_t*>(domain);
      ss << "[" << domain_int64[0] << "," << domain_int64[1] << "]";
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
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
      tile_extent_int64 = static_cast<const int64_t*>(tile_extent);
      ss << *tile_extent_int64;
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

template <class T>
std::string to_str(const T& value) {
  std::stringstream ss;
  ss << value;
  return ss.str();
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
inline bool rect_in_rect(
    const T* rect_a, const T* rect_b, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i) {
    if (rect_a[2 * i] < rect_b[2 * i] || rect_a[2 * i] > rect_b[2 * i + 1] ||
        rect_a[2 * i + 1] < rect_b[2 * i] ||
        rect_a[2 * i + 1] > rect_b[2 * i + 1])
      return false;
  }

  return true;
}

template <class T>
void compute_mbr_union(
    unsigned dim_num, const T* mbrs, uint64_t mbr_num, T* mbr_union) {
  // Sanity check
  if (dim_num == 0 || mbr_num == 0)
    return;

  // Set the first rectangle to the union
  auto mbr_size = 2 * dim_num * sizeof(T);
  std::memcpy(mbr_union, mbrs, mbr_size);

  // Expand the union with every other MBR
  for (uint64_t i = 1; i < mbr_num; ++i)
    expand_mbr_with_mbr<T>(mbr_union, &mbrs[i * 2 * dim_num], dim_num);
}

template <class T>
void expand_mbr(T* mbr, const T* coords, unsigned int dim_num) {
  for (unsigned int d = 0; d < dim_num; ++d) {
    expand_mbr(d, coords[d], mbr);
  }
}

template <class T>
void expand_mbr(const uint64_t d, const T coord, T* const mbr) {
  // Update lower bound on dimension i
  if (mbr[2 * d] > coord)
    mbr[2 * d] = coord;

  // Update upper bound on dimension i
  if (mbr[2 * d + 1] < coord)
    mbr[2 * d + 1] = coord;
}

template <class T>
void expand_mbr_with_mbr(T* mbr_a, const T* mbr_b, unsigned int dim_num) {
  for (unsigned int i = 0; i < dim_num; ++i) {
    // Update lower bound on dimension i
    if (mbr_a[2 * i] > mbr_b[2 * i])
      mbr_a[2 * i] = mbr_b[2 * i];

    // Update upper bound on dimension i
    if (mbr_a[2 * i + 1] < mbr_b[2 * i + 1])
      mbr_a[2 * i + 1] = mbr_b[2 * i + 1];
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
bool overlap(const std::vector<const T*>& a, const T* b) {
  auto dim_num = (unsigned)a.size();
  for (unsigned i = 0; i < dim_num; ++i) {
    if (a[i][0] > b[2 * i + 1] || a[i][1] < b[2 * i])
      return false;
  }

  return true;
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
      if (std::numeric_limits<T>::is_integer) {
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

double log(double b, double x) {
  return ::log(x) / ::log(b);
}

template <class T>
T safe_mul(T a, T b) {
  T prod = a * b;

  // Check for overflow only for integers
  if (std::is_integral<T>::value) {
    if (prod / a != b)  // Overflow
      return std::numeric_limits<T>::max();
  }

  return prod;
}

}  // namespace math

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

template bool rect_in_rect<int>(
    const int* rect_a, const int* rect_b, unsigned int dim_num);
template bool rect_in_rect<int64_t>(
    const int64_t* rect_a, const int64_t* rect_b, unsigned int dim_num);
template bool rect_in_rect<float>(
    const float* react_a, const float* rect_b, unsigned int dim_num);
template bool rect_in_rect<double>(
    const double* rect_a, const double* rect_b, unsigned int dim_num);
template bool rect_in_rect<int8_t>(
    const int8_t* rect_a, const int8_t* rect_b, unsigned int dim_num);
template bool rect_in_rect<uint8_t>(
    const uint8_t* rect_a, const uint8_t* rect_b, unsigned int dim_num);
template bool rect_in_rect<int16_t>(
    const int16_t* rect_a, const int16_t* rect_b, unsigned int dim_num);
template bool rect_in_rect<uint16_t>(
    const uint16_t* rect_a, const uint16_t* rect_b, unsigned int dim_num);
template bool rect_in_rect<uint32_t>(
    const uint32_t* rect_a, const uint32_t* rect_b, unsigned int dim_num);
template bool rect_in_rect<uint64_t>(
    const uint64_t* rect_a, const uint64_t* rect_b, unsigned int dim_num);

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

template void expand_mbr(uint64_t d, int8_t coord, int8_t* mbr);
template void expand_mbr(uint64_t d, uint8_t coord, uint8_t* mbr);
template void expand_mbr(uint64_t d, int16_t coord, int16_t* mbr);
template void expand_mbr(uint64_t d, uint16_t coord, uint16_t* mbr);
template void expand_mbr(uint64_t d, int32_t coord, int32_t* mbr);
template void expand_mbr(uint64_t d, uint32_t coord, uint32_t* mbr);
template void expand_mbr(uint64_t d, int64_t coord, int64_t* mbr);
template void expand_mbr(uint64_t d, uint64_t coord, uint64_t* mbr);
template void expand_mbr(uint64_t d, float coord, float* mbr);
template void expand_mbr(uint64_t d, double coord, double* mbr);

template void expand_mbr_with_mbr<int>(
    int* mbr_a, const int* mbr_b, unsigned int dim_num);
template void expand_mbr_with_mbr<int64_t>(
    int64_t* mbr_a, const int64_t* mbr_b, unsigned int dim_num);
template void expand_mbr_with_mbr<float>(
    float* mbr_a, const float* mbr_b, unsigned int dim_num);
template void expand_mbr_with_mbr<double>(
    double* mbr_a, const double* mbr_b, unsigned int dim_num);
template void expand_mbr_with_mbr<int8_t>(
    int8_t* mbr_a, const int8_t* mbr_b, unsigned int dim_num);
template void expand_mbr_with_mbr<uint8_t>(
    uint8_t* mbr_a, const uint8_t* mbr_b, unsigned int dim_num);
template void expand_mbr_with_mbr<int16_t>(
    int16_t* mbr_a, const int16_t* mbr_b, unsigned int dim_num);
template void expand_mbr_with_mbr<uint16_t>(
    uint16_t* mbr_a, const uint16_t* mbr_b, unsigned int dim_num);
template void expand_mbr_with_mbr<uint32_t>(
    uint32_t* mbr_a, const uint32_t* mbr_b, unsigned int dim_num);
template void expand_mbr_with_mbr<uint64_t>(
    uint64_t* mbr_a, const uint64_t* mbr_b, unsigned int dim_num);

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

template bool overlap<int8_t>(
    const std::vector<const int8_t*>& a, const int8_t* b);
template bool overlap<uint8_t>(
    const std::vector<const uint8_t*>& a, const uint8_t* b);
template bool overlap<int16_t>(
    const std::vector<const int16_t*>& a, const int16_t* b);
template bool overlap<uint16_t>(
    const std::vector<const uint16_t*>& a, const uint16_t* b);
template bool overlap<int32_t>(
    const std::vector<const int32_t*>& a, const int32_t* b);
template bool overlap<uint32_t>(
    const std::vector<const uint32_t*>& a, const uint32_t* b);
template bool overlap<int64_t>(
    const std::vector<const int64_t*>& a, const int64_t* b);
template bool overlap<uint64_t>(
    const std::vector<const uint64_t*>& a, const uint64_t* b);
template bool overlap<float>(
    const std::vector<const float*>& a, const float* b);
template bool overlap<double>(
    const std::vector<const double*>& a, const double* b);

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

template void compute_mbr_union<int8_t>(
    unsigned dim_num, const int8_t* mbrs, uint64_t mbr_num, int8_t* mbr_union);
template void compute_mbr_union<uint8_t>(
    unsigned dim_num,
    const uint8_t* mbrs,
    uint64_t mbr_num,
    uint8_t* mbr_union);
template void compute_mbr_union<int16_t>(
    unsigned dim_num,
    const int16_t* mbrs,
    uint64_t mbr_num,
    int16_t* mbr_union);
template void compute_mbr_union<uint16_t>(
    unsigned dim_num,
    const uint16_t* mbrs,
    uint64_t mbr_num,
    uint16_t* mbr_union);
template void compute_mbr_union<int32_t>(
    unsigned dim_num,
    const int32_t* mbrs,
    uint64_t mbr_num,
    int32_t* mbr_union);
template void compute_mbr_union<uint32_t>(
    unsigned dim_num,
    const uint32_t* mbrs,
    uint64_t mbr_num,
    uint32_t* mbr_union);
template void compute_mbr_union<int64_t>(
    unsigned dim_num,
    const int64_t* mbrs,
    uint64_t mbr_num,
    int64_t* mbr_union);
template void compute_mbr_union<uint64_t>(
    unsigned dim_num,
    const uint64_t* mbrs,
    uint64_t mbr_num,
    uint64_t* mbr_union);
template void compute_mbr_union<float>(
    unsigned dim_num, const float* mbrs, uint64_t mbr_num, float* mbr_union);
template void compute_mbr_union<double>(
    unsigned dim_num, const double* mbrs, uint64_t mbr_num, double* mbr_union);

}  // namespace geometry

namespace math {

template int8_t safe_mul<int8_t>(int8_t a, int8_t b);
template uint8_t safe_mul<uint8_t>(uint8_t a, uint8_t b);
template int16_t safe_mul<int16_t>(int16_t a, int16_t b);
template uint16_t safe_mul<uint16_t>(uint16_t a, uint16_t b);
template int32_t safe_mul<int32_t>(int32_t a, int32_t b);
template uint32_t safe_mul<uint32_t>(uint32_t a, uint32_t b);
template int64_t safe_mul<int64_t>(int64_t a, int64_t b);
template uint64_t safe_mul<uint64_t>(uint64_t a, uint64_t b);
template float safe_mul<float>(float a, float b);
template double safe_mul<double>(double a, double b);

}  // namespace math

namespace parse {

template std::string to_str<int32_t>(const int32_t& value);
template std::string to_str<uint32_t>(const uint32_t& value);

}  // namespace parse

}  // namespace utils

}  // namespace sm
}  // namespace tiledb
