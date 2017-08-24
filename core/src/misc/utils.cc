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

#include <dirent.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>
#include <set>
#include <sstream>
#include "config.h"
#include "constants.h"
#include "logger.h"
#if defined(__APPLE__) && defined(__MACH__)
#include <arpa/inet.h>
#include <net/if.h>
#include <net/if_dl.h>
#include <netinet/in.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#else
#include <linux/if.h>
#endif

#include "../../include/vfs/filesystem.h"
#include "tiledb.h"
#include "utils.h"

#define STR(s) #s
#define XSTR(s) STR(s)

namespace tiledb {

namespace utils {

/* ****************************** */
/*           FUNCTIONS            */
/* ****************************** */

const char* array_type_str(ArrayType array_type) {
  if (array_type == ArrayType::DENSE)
    return constants::dense_str;
  else if (array_type == ArrayType::SPARSE)
    return constants::sparse_str;
  else
    return nullptr;
}

template <class T>
inline bool cell_in_subarray(const T* cell, const T* subarray, int dim_num) {
  for (int i = 0; i < dim_num; ++i) {
    if (cell[i] >= subarray[2 * i] && cell[i] <= subarray[2 * i + 1])
      continue;  // Inside this dimension domain

    return false;  // NOT inside this dimension domain
  }

  return true;
}

template <class T>
int64_t cell_num_in_subarray(const T* subarray, int dim_num) {
  int64_t cell_num = 1;

  for (int i = 0; i < dim_num; ++i)
    cell_num *= subarray[2 * i + 1] - subarray[2 * i] + 1;

  return cell_num;
}

template <class T>
int cmp_col_order(const T* coords_a, const T* coords_b, int dim_num) {
  for (int i = dim_num - 1; i >= 0; --i) {
    // a precedes b
    if (coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    else if (coords_a[i] > coords_b[i])
      return 1;
  }

  // a and b are equal
  return 0;
}

template <class T>
int cmp_col_order(
    int64_t id_a,
    const T* coords_a,
    int64_t id_b,
    const T* coords_b,
    int dim_num) {
  // a precedes b
  if (id_a < id_b)
    return -1;

  // b precedes a
  if (id_a > id_b)
    return 1;

  // ids are equal, check the coordinates
  for (int i = dim_num - 1; i >= 0; --i) {
    // a precedes b
    if (coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    else if (coords_a[i] > coords_b[i])
      return 1;
  }

  // a and b are equal
  return 0;
}

template <class T>
int cmp_row_order(const T* coords_a, const T* coords_b, int dim_num) {
  for (int i = 0; i < dim_num; ++i) {
    // a precedes b
    if (coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    else if (coords_a[i] > coords_b[i])
      return 1;
  }

  // a and b are equal
  return 0;
}

template <class T>
int cmp_row_order(
    int64_t id_a,
    const T* coords_a,
    int64_t id_b,
    const T* coords_b,
    int dim_num) {
  // a precedes b
  if (id_a < id_b)
    return -1;

  // b precedes a
  if (id_a > id_b)
    return 1;

  // ids are equal, check the coordinates
  for (int i = 0; i < dim_num; ++i) {
    // a precedes b
    if (coords_a[i] < coords_b[i])
      return -1;
    // b precedes a
    else if (coords_a[i] > coords_b[i])
      return 1;
  }

  // a and b are equal
  return 0;
}

const char* compressor_str(Compressor type) {
  switch (type) {
    case Compressor::NO_COMPRESSION:
      return constants::no_compression_str;
    case Compressor::GZIP:
      return constants::gzip_str;
    case Compressor::ZSTD:
      return constants::zstd_str;
    case Compressor::LZ4:
      return constants::lz4_str;
    case Compressor::BLOSC:
      return constants::blosc_str;
    case Compressor::BLOSC_LZ4:
      return constants::blosc_lz4_str;
    case Compressor::BLOSC_LZ4HC:
      return constants::blosc_lz4hc_str;
    case Compressor::BLOSC_SNAPPY:
      return constants::blosc_snappy_str;
    case Compressor::BLOSC_ZLIB:
      return constants::blosc_zlib_str;
    case Compressor::BLOSC_ZSTD:
      return constants::blosc_zstd_str;
    case Compressor::RLE:
      return constants::rle_str;
    case Compressor::BZIP2:
      return constants::bzip2_str;
    default:
      return nullptr;
  }
}

uint64_t datatype_size(Datatype type) {
  uint64_t size;

  switch (type) {
    case Datatype::INT32:
      size = sizeof(int);
      break;
    case Datatype::INT64:
      size = sizeof(int64_t);
      break;
    case Datatype::FLOAT32:
      size = sizeof(float);
      break;
    case Datatype::FLOAT64:
      size = sizeof(double);
      break;
    case Datatype::CHAR:
      size = sizeof(char);
      break;
    case Datatype::INT8:
      size = sizeof(int8_t);
      break;
    case Datatype::UINT8:
      size = sizeof(uint8_t);
      break;
    case Datatype::INT16:
      size = sizeof(int16_t);
      break;
    case Datatype::UINT16:
      size = sizeof(uint16_t);
      break;
    case Datatype::UINT32:
      size = sizeof(uint32_t);
      break;
    case Datatype::UINT64:
      size = sizeof(uint64_t);
      break;
    default:
      break;
  }

  return size;
}

const char* datatype_str(Datatype type) {
  switch (type) {
    case Datatype::INT32:
      return constants::int32_str;
    case Datatype::INT64:
      return constants::int64_str;
    case Datatype::FLOAT32:
      return constants::float32_str;
    case Datatype::FLOAT64:
      return constants::float64_str;
    case Datatype::CHAR:
      return constants::char_str;
    case Datatype::INT8:
      return constants::int8_str;
    case Datatype::UINT8:
      return constants::uint8_str;
    case Datatype::INT16:
      return constants::int16_str;
    case Datatype::UINT16:
      return constants::uint16_str;
    case Datatype::UINT32:
      return constants::uint32_str;
    case Datatype::UINT64:
      return constants::uint64_str;
    default:
      return nullptr;
  }
}

std::string domain_str(const void* domain, Datatype type) {
  std::stringstream ss;

  if (type == Datatype::INT32) {
    const int* domain_ = static_cast<const int*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  } else if (type == Datatype::INT64) {
    const int64_t* domain_ = static_cast<const int64_t*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  } else if (type == Datatype::FLOAT32) {
    const float* domain_ = static_cast<const float*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  } else if (type == Datatype::FLOAT64) {
    const double* domain_ = static_cast<const double*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  } else if (type == Datatype::CHAR) {
    const char* domain_ = static_cast<const char*>(domain);
    ss << "[" << int(domain_[0]) << "," << int(domain_[1]) << "]";
    return ss.str();
  } else if (type == Datatype::INT8) {
    const int8_t* domain_ = static_cast<const int8_t*>(domain);
    ss << "[" << int(domain_[0]) << "," << int(domain_[1]) << "]";
    return ss.str();
  } else if (type == Datatype::UINT8) {
    const uint8_t* domain_ = static_cast<const uint8_t*>(domain);
    ss << "[" << int(domain_[0]) << "," << int(domain_[1]) << "]";
    return ss.str();
  } else if (type == Datatype::INT16) {
    const int16_t* domain_ = static_cast<const int16_t*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  } else if (type == Datatype::UINT16) {
    const uint16_t* domain_ = static_cast<const uint16_t*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  } else if (type == Datatype::UINT32) {
    const uint32_t* domain_ = static_cast<const uint32_t*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  } else if (type == Datatype::UINT64) {
    const uint64_t* domain_ = static_cast<const uint64_t*>(domain);
    ss << "[" << domain_[0] << "," << domain_[1] << "]";
    return ss.str();
  } else {
    return "";
  }
}

template <class T>
bool empty_value(T value) {
  if (&typeid(T) == &typeid(int))
    return value == constants::empty_int32;
  else if (&typeid(T) == &typeid(int64_t))
    return value == constants::empty_int64;
  else if (&typeid(T) == &typeid(float))
    return value == constants::empty_float32;
  else if (&typeid(T) == &typeid(double))
    return value == constants::empty_float64;
  else if (&typeid(T) == &typeid(int8_t))
    return value == constants::empty_int8;
  else if (&typeid(T) == &typeid(uint8_t))
    return value == constants::empty_uint8;
  else if (&typeid(T) == &typeid(int16_t))
    return value == constants::empty_int16;
  else if (&typeid(T) == &typeid(uint16_t))
    return value == constants::empty_uint16;
  else if (&typeid(T) == &typeid(uint32_t))
    return value == constants::empty_uint32;
  else if (&typeid(T) == &typeid(uint64_t))
    return value == constants::empty_uint64;
  else
    return false;
}

Status expand_buffer(void*& buffer, size_t& buffer_allocated_size) {
  buffer_allocated_size *= 2;
  buffer = std::realloc(buffer, buffer_allocated_size);
  if (buffer == nullptr) {
    return LOG_STATUS(Status::MemError("Cannot reallocate buffer"));
  }
  return Status::Ok();
}

template <class T>
void expand_mbr(T* mbr, const T* coords, int dim_num) {
  for (int i = 0; i < dim_num; ++i) {
    // Update lower bound on dimension i
    if (mbr[2 * i] > coords[i])
      mbr[2 * i] = coords[i];

    // Update upper bound on dimension i
    if (mbr[2 * i + 1] < coords[i])
      mbr[2 * i + 1] = coords[i];
  }
}

Status delete_fragment(const uri::URI& frag) {
  return vfs::delete_dir(frag);
}

bool fragment_exists(const uri::URI& frag) {
  return vfs::is_dir(frag);
}

std::string parent_path(const std::string& dir) {
  // Get real dir
  std::string real_dir = vfs::real_dir(dir);

  // Start from the end of the string
  int pos = real_dir.size() - 1;

  // Skip the potential last '/'
  if (real_dir[pos] == '/')
    --pos;

  // Scan backwords until you find the next '/'
  while (pos > 0 && real_dir[pos] != '/')
    --pos;

  return real_dir.substr(0, pos);
}

#if defined(__APPLE__) && defined(__MACH__)
// TODO: Errors are ignored, this is kind of a mess with the preprocessor order
// we need to include tiledb.h just to get the mac address!
std::string get_mac_addr() {
  int mib[6];
  char mac[13];
  size_t len;
  char* buf;
  unsigned char* ptr;
  struct if_msghdr* ifm;
  struct sockaddr_dl* sdl;
  mib[0] = CTL_NET;
  mib[1] = AF_ROUTE;
  mib[2] = 0;
  mib[3] = AF_LINK;
  mib[4] = NET_RT_IFLIST;
  const char* ifname = XSTR(TILEDB_MAC_ADDRESS_INTERFACE);
  mib[5] = if_nametoindex(ifname);
  if (mib[5] == 0 || sysctl(mib, 6, nullptr, &len, nullptr, 0) < 0) {
    assert(0);
    LOG_ERROR("Cannot get MAC address");
    return "";
  }
  buf = (char*)std::malloc(len);
  if (sysctl(mib, 6, buf, &len, nullptr, 0) < 0) {
    assert(0);
    LOG_ERROR("Cannot get MAC address");
    return "";
  }

  ifm = (struct if_msghdr*)buf;
  sdl = (struct sockaddr_dl*)(ifm + 1);
  ptr = (unsigned char*)LLADDR(sdl);
  for (int i = 0; i < 6; ++i)
    sprintf(mac + 2 * i, "%02x", *(ptr + i));
  mac[12] = '\0';

  free(buf);
  return mac;
}
#else
std::string get_mac_addr() {
  struct ifreq s;
  int fd = socket(PF_INET, SOCK_DGRAM, IPPROTO_IP);
  char mac[13];

  strcpy(s.ifr_name, XSTR(TILEDB_MAC_ADDRESS_INTERFACE));
  if (0 == ioctl(fd, SIOCGIFHWADDR, &s)) {
    for (int i = 0; i < 6; ++i)
      sprintf(mac + 2 * i, "%02x", (unsigned char)s.ifr_addr.sa_data[i]);
    mac[12] = '\0';
    close(fd);

    return mac;
  } else {  // Error
    close(fd);
    LOG_ERROR("Cannot get MAC address");
    return "";
  }
}
#endif

template <class T>
bool has_duplicates(const std::vector<T>& v) {
  std::set<T> s(v.begin(), v.end());

  return s.size() != v.size();
}

template <class T>
bool inside_subarray(const T* coords, const T* subarray, int dim_num) {
  for (int i = 0; i < dim_num; ++i)
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

  return intersect.size() != 0;
}

template <class T>
bool is_contained(const T* range_A, const T* range_B, int dim_num) {
  for (int i = 0; i < dim_num; ++i)
    if (range_A[2 * i] < range_B[2 * i] ||
        range_A[2 * i + 1] > range_B[2 * i + 1])
      return false;

  return true;
}

bool is_array(const std::string& dir) {
  // Check existence
  if (vfs::is_dir(dir) &&
      vfs::is_file(dir + "/" + constants::array_schema_filename))
    return true;
  else
    return false;
}

bool is_array(const uri::URI& uri) {
  // Check existence
  if (vfs::is_dir(uri)) {
    auto array_schema_uri = uri.join_path(constants::array_schema_filename);
    if (vfs::is_file(array_schema_uri)) {
      return true;
    }
  }
  return false;
}

bool is_fragment(const std::string& dir) {
  // Check existence
  if (vfs::is_dir(dir) &&
      vfs::is_file(dir + "/" + constants::fragment_filename))
    return true;
  else
    return false;
}

bool is_group(const uri::URI& uri) {
  // Check existence
  if (vfs::is_dir(uri)) {
    auto group_uri = uri.join_path(constants::group_filename);
    if (vfs::is_file(group_uri)) {
      return true;
    }
  }
  return false;
}

bool is_group(const std::string& dir) {
  // Check existence
  if (vfs::is_dir(dir) && vfs::is_file(dir + "/" + constants::group_filename))
    return true;
  else
    return false;
}

inline bool ends_with(std::string const& value, std::string const& ending) {
  if (ending.size() > value.size())
    return false;
  return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

bool is_array_schema(const std::string& path) {
  return ends_with(path, constants::array_schema_filename);
}

bool is_consolidation_lock(const std::string& path) {
  return ends_with(path, constants::consolidation_filelock_name);
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
    if (!isdigit(s[i]))
      return false;
  }

  return true;
}

template <class T>
bool is_unary_subarray(const T* subarray, int dim_num) {
  for (int i = 0; i < dim_num; ++i) {
    if (subarray[2 * i] != subarray[2 * i + 1])
      return false;
  }

  return true;
}

const char* layout_str(Layout layout) {
  if (layout == Layout::COL_MAJOR)
    return constants::col_major_str;
  else if (layout == Layout::ROW_MAJOR)
    return constants::row_major_str;
  else
    return nullptr;
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
    const int* tile_extent_ = static_cast<const int*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  } else if (type == Datatype::INT64) {
    const int64_t* tile_extent_ = static_cast<const int64_t*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  } else if (type == Datatype::FLOAT32) {
    const float* tile_extent_ = static_cast<const float*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  } else if (type == Datatype::FLOAT64) {
    const double* tile_extent_ = static_cast<const double*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  } else if (type == Datatype::CHAR) {
    const char* tile_extent_ = static_cast<const char*>(tile_extent);
    ss << int(*tile_extent_);
    return ss.str();
  } else if (type == Datatype::INT8) {
    const int8_t* tile_extent_ = static_cast<const int8_t*>(tile_extent);
    ss << int(*tile_extent_);
    return ss.str();
  } else if (type == Datatype::UINT8) {
    const uint8_t* tile_extent_ = static_cast<const uint8_t*>(tile_extent);
    ss << int(*tile_extent_);
    return ss.str();
  } else if (type == Datatype::INT16) {
    const int16_t* tile_extent_ = static_cast<const int16_t*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  } else if (type == Datatype::UINT16) {
    const uint16_t* tile_extent_ = static_cast<const uint16_t*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  } else if (type == Datatype::UINT32) {
    const uint32_t* tile_extent_ = static_cast<const uint32_t*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  } else if (type == Datatype::UINT64) {
    const uint64_t* tile_extent_ = static_cast<const uint64_t*>(tile_extent);
    ss << *tile_extent_;
    return ss.str();
  } else {
    return "";
  }
}

// Explicit template instantiations
template int64_t cell_num_in_subarray<int>(const int* subarray, int dim_num);
template int64_t cell_num_in_subarray<int64_t>(
    const int64_t* subarray, int dim_num);
template int64_t cell_num_in_subarray<float>(
    const float* subarray, int dim_num);
template int64_t cell_num_in_subarray<double>(
    const double* subarray, int dim_num);
template int64_t cell_num_in_subarray<int8_t>(
    const int8_t* subarray, int dim_num);
template int64_t cell_num_in_subarray<uint8_t>(
    const uint8_t* subarray, int dim_num);
template int64_t cell_num_in_subarray<int16_t>(
    const int16_t* subarray, int dim_num);
template int64_t cell_num_in_subarray<uint16_t>(
    const uint16_t* subarray, int dim_num);
template int64_t cell_num_in_subarray<uint32_t>(
    const uint32_t* subarray, int dim_num);
template int64_t cell_num_in_subarray<uint64_t>(
    const uint64_t* subarray, int dim_num);

template bool cell_in_subarray<int>(
    const int* cell, const int* subarray, int dim_num);
template bool cell_in_subarray<int64_t>(
    const int64_t* cell, const int64_t* subarray, int dim_num);
template bool cell_in_subarray<float>(
    const float* cell, const float* subarray, int dim_num);
template bool cell_in_subarray<double>(
    const double* cell, const double* subarray, int dim_num);
template bool cell_in_subarray<int8_t>(
    const int8_t* cell, const int8_t* subarray, int dim_num);
template bool cell_in_subarray<uint8_t>(
    const uint8_t* cell, const uint8_t* subarray, int dim_num);
template bool cell_in_subarray<int16_t>(
    const int16_t* cell, const int16_t* subarray, int dim_num);
template bool cell_in_subarray<uint16_t>(
    const uint16_t* cell, const uint16_t* subarray, int dim_num);
template bool cell_in_subarray<uint32_t>(
    const uint32_t* cell, const uint32_t* subarray, int dim_num);
template bool cell_in_subarray<uint64_t>(
    const uint64_t* cell, const uint64_t* subarray, int dim_num);

template int cmp_col_order<int>(
    const int* coords_a, const int* coords_b, int dim_num);
template int cmp_col_order<int64_t>(
    const int64_t* coords_a, const int64_t* coords_b, int dim_num);
template int cmp_col_order<float>(
    const float* coords_a, const float* coords_b, int dim_num);
template int cmp_col_order<double>(
    const double* coords_a, const double* coords_b, int dim_num);
template int cmp_col_order<int8_t>(
    const int8_t* coords_a, const int8_t* coords_b, int dim_num);
template int cmp_col_order<uint8_t>(
    const uint8_t* coords_a, const uint8_t* coords_b, int dim_num);
template int cmp_col_order<int16_t>(
    const int16_t* coords_a, const int16_t* coords_b, int dim_num);
template int cmp_col_order<uint16_t>(
    const uint16_t* coords_a, const uint16_t* coords_b, int dim_num);
template int cmp_col_order<uint32_t>(
    const uint32_t* coords_a, const uint32_t* coords_b, int dim_num);
template int cmp_col_order<uint64_t>(
    const uint64_t* coords_a, const uint64_t* coords_b, int dim_num);

template int cmp_col_order<int>(
    int64_t id_a,
    const int* coords_a,
    int64_t id_b,
    const int* coords_b,
    int dim_num);
template int cmp_col_order<int64_t>(
    int64_t id_a,
    const int64_t* coords_a,
    int64_t id_b,
    const int64_t* coords_b,
    int dim_num);
template int cmp_col_order<float>(
    int64_t id_a,
    const float* coords_a,
    int64_t id_b,
    const float* coords_b,
    int dim_num);
template int cmp_col_order<double>(
    int64_t id_a,
    const double* coords_a,
    int64_t id_b,
    const double* coords_b,
    int dim_num);
template int cmp_col_order<int8_t>(
    int64_t id_a,
    const int8_t* coords_a,
    int64_t id_b,
    const int8_t* coords_b,
    int dim_num);
template int cmp_col_order<uint8_t>(
    int64_t id_a,
    const uint8_t* coords_a,
    int64_t id_b,
    const uint8_t* coords_b,
    int dim_num);
template int cmp_col_order<int16_t>(
    int64_t id_a,
    const int16_t* coords_a,
    int64_t id_b,
    const int16_t* coords_b,
    int dim_num);
template int cmp_col_order<uint16_t>(
    int64_t id_a,
    const uint16_t* coords_a,
    int64_t id_b,
    const uint16_t* coords_b,
    int dim_num);
template int cmp_col_order<uint32_t>(
    int64_t id_a,
    const uint32_t* coords_a,
    int64_t id_b,
    const uint32_t* coords_b,
    int dim_num);
template int cmp_col_order<uint64_t>(
    int64_t id_a,
    const uint64_t* coords_a,
    int64_t id_b,
    const uint64_t* coords_b,
    int dim_num);

template int cmp_row_order<int>(
    const int* coords_a, const int* coords_b, int dim_num);
template int cmp_row_order<int64_t>(
    const int64_t* coords_a, const int64_t* coords_b, int dim_num);
template int cmp_row_order<float>(
    const float* coords_a, const float* coords_b, int dim_num);
template int cmp_row_order<double>(
    const double* coords_a, const double* coords_b, int dim_num);
template int cmp_row_order<int8_t>(
    const int8_t* coords_a, const int8_t* coords_b, int dim_num);
template int cmp_row_order<uint8_t>(
    const uint8_t* coords_a, const uint8_t* coords_b, int dim_num);
template int cmp_row_order<int16_t>(
    const int16_t* coords_a, const int16_t* coords_b, int dim_num);
template int cmp_row_order<uint16_t>(
    const uint16_t* coords_a, const uint16_t* coords_b, int dim_num);
template int cmp_row_order<uint32_t>(
    const uint32_t* coords_a, const uint32_t* coords_b, int dim_num);
template int cmp_row_order<uint64_t>(
    const uint64_t* coords_a, const uint64_t* coords_b, int dim_num);

template int cmp_row_order<int>(
    int64_t id_a,
    const int* coords_a,
    int64_t id_b,
    const int* coords_b,
    int dim_num);
template int cmp_row_order<int64_t>(
    int64_t id_a,
    const int64_t* coords_a,
    int64_t id_b,
    const int64_t* coords_b,
    int dim_num);
template int cmp_row_order<float>(
    int64_t id_a,
    const float* coords_a,
    int64_t id_b,
    const float* coords_b,
    int dim_num);
template int cmp_row_order<double>(
    int64_t id_a,
    const double* coords_a,
    int64_t id_b,
    const double* coords_b,
    int dim_num);
template int cmp_row_order<int8_t>(
    int64_t id_a,
    const int8_t* coords_a,
    int64_t id_b,
    const int8_t* coords_b,
    int dim_num);
template int cmp_row_order<uint8_t>(
    int64_t id_a,
    const uint8_t* coords_a,
    int64_t id_b,
    const uint8_t* coords_b,
    int dim_num);
template int cmp_row_order<int16_t>(
    int64_t id_a,
    const int16_t* coords_a,
    int64_t id_b,
    const int16_t* coords_b,
    int dim_num);
template int cmp_row_order<uint16_t>(
    int64_t id_a,
    const uint16_t* coords_a,
    int64_t id_b,
    const uint16_t* coords_b,
    int dim_num);
template int cmp_row_order<uint32_t>(
    int64_t id_a,
    const uint32_t* coords_a,
    int64_t id_b,
    const uint32_t* coords_b,
    int dim_num);
template int cmp_row_order<uint64_t>(
    int64_t id_a,
    const uint64_t* coords_a,
    int64_t id_b,
    const uint64_t* coords_b,
    int dim_num);

template bool empty_value<int>(int value);
template bool empty_value<int64_t>(int64_t value);
template bool empty_value<float>(float value);
template bool empty_value<double>(double value);
template bool empty_value<int8_t>(int8_t value);
template bool empty_value<uint8_t>(uint8_t value);
template bool empty_value<int16_t>(int16_t value);
template bool empty_value<uint16_t>(uint16_t value);
template bool empty_value<uint32_t>(uint32_t value);
template bool empty_value<uint64_t>(uint64_t value);

template void expand_mbr<int>(int* mbr, const int* coords, int dim_num);
template void expand_mbr<int64_t>(
    int64_t* mbr, const int64_t* coords, int dim_num);
template void expand_mbr<float>(float* mbr, const float* coords, int dim_num);
template void expand_mbr<double>(
    double* mbr, const double* coords, int dim_num);
template void expand_mbr<int8_t>(
    int8_t* mbr, const int8_t* coords, int dim_num);
template void expand_mbr<uint8_t>(
    uint8_t* mbr, const uint8_t* coords, int dim_num);
template void expand_mbr<int16_t>(
    int16_t* mbr, const int16_t* coords, int dim_num);
template void expand_mbr<uint16_t>(
    uint16_t* mbr, const uint16_t* coords, int dim_num);
template void expand_mbr<uint32_t>(
    uint32_t* mbr, const uint32_t* coords, int dim_num);
template void expand_mbr<uint64_t>(
    uint64_t* mbr, const uint64_t* coords, int dim_num);

template bool has_duplicates<std::string>(const std::vector<std::string>& v);

template bool inside_subarray<int>(
    const int* coords, const int* subarray, int dim_num);
template bool inside_subarray<int64_t>(
    const int64_t* coords, const int64_t* subarray, int dim_num);
template bool inside_subarray<float>(
    const float* coords, const float* subarray, int dim_num);
template bool inside_subarray<double>(
    const double* coords, const double* subarray, int dim_num);
template bool inside_subarray<int8_t>(
    const int8_t* coords, const int8_t* subarray, int dim_num);
template bool inside_subarray<uint8_t>(
    const uint8_t* coords, const uint8_t* subarray, int dim_num);
template bool inside_subarray<int16_t>(
    const int16_t* coords, const int16_t* subarray, int dim_num);
template bool inside_subarray<uint16_t>(
    const uint16_t* coords, const uint16_t* subarray, int dim_num);
template bool inside_subarray<uint32_t>(
    const uint32_t* coords, const uint32_t* subarray, int dim_num);
template bool inside_subarray<uint64_t>(
    const uint64_t* coords, const uint64_t* subarray, int dim_num);

template bool intersect<std::string>(
    const std::vector<std::string>& v1, const std::vector<std::string>& v2);

template bool is_contained<int>(
    const int* range_A, const int* range_B, int dim_num);
template bool is_contained<int64_t>(
    const int64_t* range_A, const int64_t* range_B, int dim_num);
template bool is_contained<float>(
    const float* range_A, const float* range_B, int dim_num);
template bool is_contained<double>(
    const double* range_A, const double* range_B, int dim_num);
template bool is_contained<int8_t>(
    const int8_t* range_A, const int8_t* range_B, int dim_num);
template bool is_contained<uint8_t>(
    const uint8_t* range_A, const uint8_t* range_B, int dim_num);
template bool is_contained<int16_t>(
    const int16_t* range_A, const int16_t* range_B, int dim_num);
template bool is_contained<uint16_t>(
    const uint16_t* range_A, const uint16_t* range_B, int dim_num);
template bool is_contained<uint32_t>(
    const uint32_t* range_A, const uint32_t* range_B, int dim_num);
template bool is_contained<uint64_t>(
    const uint64_t* range_A, const uint64_t* range_B, int dim_num);

template bool is_unary_subarray<int>(const int* subarray, int dim_num);
template bool is_unary_subarray<int64_t>(const int64_t* subarray, int dim_num);
template bool is_unary_subarray<float>(const float* subarray, int dim_num);
template bool is_unary_subarray<double>(const double* subarray, int dim_num);
template bool is_unary_subarray<int8_t>(const int8_t* subarray, int dim_num);
template bool is_unary_subarray<uint8_t>(const uint8_t* subarray, int dim_num);
template bool is_unary_subarray<int16_t>(const int16_t* subarray, int dim_num);
template bool is_unary_subarray<uint16_t>(
    const uint16_t* subarray, int dim_num);
template bool is_unary_subarray<uint32_t>(
    const uint32_t* subarray, int dim_num);
template bool is_unary_subarray<uint64_t>(
    const uint64_t* subarray, int dim_num);

}  // namespace utils

}  // namespace tiledb
