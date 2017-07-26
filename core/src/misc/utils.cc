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
#include "configurator.h"
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

#include "filesystem.h"
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
    return Configurator::dense_str();
  else if (array_type == ArrayType::SPARSE)
    return Configurator::sparse_str();
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
      return Configurator::no_compression_str();
    case Compressor::GZIP:
      return Configurator::gzip_str();
    case Compressor::ZSTD:
      return Configurator::zstd_str();
    case Compressor::LZ4:
      return Configurator::lz4_str();
    case Compressor::BLOSC:
      return Configurator::blosc_str();
    case Compressor::BLOSC_LZ4:
      return Configurator::blosc_lz4_str();
    case Compressor::BLOSC_LZ4HC:
      return Configurator::blosc_lz4hc_str();
    case Compressor::BLOSC_SNAPPY:
      return Configurator::blosc_snappy_str();
    case Compressor::BLOSC_ZLIB:
      return Configurator::blosc_zlib_str();
    case Compressor::BLOSC_ZSTD:
      return Configurator::blosc_zstd_str();
    case Compressor::RLE:
      return Configurator::rle_str();
    case Compressor::BZIP2:
      return Configurator::bzip2_str();
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
      return Configurator::int32_str();
    case Datatype::INT64:
      return Configurator::int64_str();
    case Datatype::FLOAT32:
      return Configurator::float32_str();
    case Datatype::FLOAT64:
      return Configurator::float64_str();
    case Datatype::CHAR:
      return Configurator::char_str();
    case Datatype::INT8:
      return Configurator::int8_str();
    case Datatype::UINT8:
      return Configurator::uint8_str();
    case Datatype::INT16:
      return Configurator::int16_str();
    case Datatype::UINT16:
      return Configurator::uint16_str();
    case Datatype::UINT32:
      return Configurator::uint32_str();
    case Datatype::UINT64:
      return Configurator::uint64_str();
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
    return value == Configurator::empty_int32();
  else if (&typeid(T) == &typeid(int64_t))
    return value == Configurator::empty_int64();
  else if (&typeid(T) == &typeid(float))
    return value == Configurator::empty_float32();
  else if (&typeid(T) == &typeid(double))
    return value == Configurator::empty_float64();
  else if (&typeid(T) == &typeid(int8_t))
    return value == Configurator::empty_int8();
  else if (&typeid(T) == &typeid(uint8_t))
    return value == Configurator::empty_uint8();
  else if (&typeid(T) == &typeid(int16_t))
    return value == Configurator::empty_int16();
  else if (&typeid(T) == &typeid(uint16_t))
    return value == Configurator::empty_uint16();
  else if (&typeid(T) == &typeid(uint32_t))
    return value == Configurator::empty_uint32();
  else if (&typeid(T) == &typeid(uint64_t))
    return value == Configurator::empty_uint64();
  else
    return false;
}

Status expand_buffer(void*& buffer, size_t& buffer_allocated_size) {
  buffer_allocated_size *= 2;
  buffer = realloc(buffer, buffer_allocated_size);

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
  return filesystem::delete_dir(frag);
}

bool fragment_exists(const uri::URI& frag) {
  return filesystem::is_dir(frag);
}

std::string parent_path(const std::string& dir) {
  // Get real dir
  std::string real_dir = filesystem::real_dir(dir);

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
  buf = (char*)malloc(len);
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
  if (filesystem::is_dir(dir) &&
      filesystem::is_file(dir + "/" + Configurator::array_schema_filename()))
    return true;
  else
    return false;
}

bool is_array(const uri::URI& uri) {
  // Check existence
  if (filesystem::is_dir(uri)) {
    auto array_schema_uri =
        uri.join_path(Configurator::array_schema_filename());
    if (filesystem::is_file(array_schema_uri)) {
      return true;
    }
  }
  return false;
}

bool is_fragment(const std::string& dir) {
  // Check existence
  if (filesystem::is_dir(dir) &&
      filesystem::is_file(dir + "/" + Configurator::fragment_filename()))
    return true;
  else
    return false;
}

bool is_group(const uri::URI& uri) {
  // Check existence
  if (filesystem::is_dir(uri)) {
    auto group_uri = uri.join_path(Configurator::group_filename());
    if (filesystem::is_file(group_uri)) {
      return true;
    }
  }
  return false;
}

bool is_group(const std::string& dir) {
  // Check existence
  if (filesystem::is_dir(dir) &&
      filesystem::is_file(dir + "/" + Configurator::group_filename()))
    return true;
  else
    return false;
}

bool is_metadata(const uri::URI& uri) {
  // Check existence
  if (filesystem::is_dir(uri)) {
    auto meta_schema_uri =
        uri.join_path(Configurator::metadata_schema_filename());
    if (filesystem::is_file(meta_schema_uri)) {
      return true;
    }
  }
  return false;
}

bool is_metadata(const std::string& dir) {
  // Check existence
  if (filesystem::is_dir(dir) &&
      filesystem::is_file(dir + "/" + Configurator::metadata_schema_filename()))
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
  return ends_with(path, Configurator::array_schema_filename());
}

bool is_metadata_schema(const std::string& path) {
  return ends_with(path, Configurator::metadata_schema_filename());
}

bool is_consolidation_lock(const std::string& path) {
  return ends_with(path, Configurator::consolidation_filelock_name());
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
    return Configurator::col_major_str();
  else if (layout == Layout::ROW_MAJOR)
    return Configurator::row_major_str();
  else
    return nullptr;
}

size_t RLE_compress_bound_coords(
    size_t input_size, size_t value_size, int dim_num) {
  // In the worst case, RLE adds two bytes per every value in the buffer for
  // each of its dim_num-1 coordinates (one dimension is never compressed).
  // The last sizeof(int64_t) is to record the number of cells compressed.
  int64_t cell_num = input_size / (dim_num * value_size);
  return input_size + cell_num * (dim_num - 1) * 2 + sizeof(int64_t);
}

Status RLE_compress_coords_col(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num,
    int64_t* output_size) {
  // Initializations
  int cur_run_len;
  int max_run_len = 65535;
  const unsigned char* input_cur;
  const unsigned char* input_prev = input;
  unsigned char* output_cur = output;
  size_t coords_size = value_size * dim_num;
  size_t run_size = value_size + 2 * sizeof(char);
  int64_t coords_num = input_size / coords_size;
  int64_t _output_size = 0;
  unsigned char byte;

  *output_size = -1;

  // Sanity check on input buffer format
  if (input_size % coords_size) {
    return LOG_STATUS(
        Status::CompressionError("failed compressing coordinates with RLE; "
                                 "invalid input buffer format"));
  }

  // Trivial case
  if (coords_num == 0) {
    *output_size = 0;
    return Status::Ok();
  };

  // Copy the number of coordinates
  if (_output_size + sizeof(int64_t) > output_allocated_size) {
    return LOG_STATUS(Status::CompressionError(
        "failed compressing coordinates with RLE; output buffer overflow"));
  }
  memcpy(output_cur, &coords_num, sizeof(int64_t));
  output_cur += sizeof(int64_t);
  _output_size += sizeof(int64_t);

  // Copy the first dimension intact
  // --- Sanity check on size
  if (_output_size + coords_num * value_size > output_allocated_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing coordinates with RLE; output buffer overflow"));
  }
  // --- Copy to output buffer
  for (int64_t i = 0; i < coords_num; ++i) {
    memcpy(output_cur, input_prev, value_size);
    input_prev += coords_size;
    output_cur += value_size;
  }
  _output_size += coords_num * value_size;

  // Make runs for each of the last (dim_num-1) dimensions
  for (int d = 1; d < dim_num; ++d) {
    cur_run_len = 1;
    input_prev = input + d * value_size;
    input_cur = input_prev + coords_size;

    // Make single dimension run
    for (int64_t i = 1; i < coords_num; ++i) {
      if (!memcmp(input_cur, input_prev, value_size) &&
          cur_run_len < max_run_len) {  // Expand the run
        ++cur_run_len;
      } else {  // Save the run
        // Sanity check on output size
        if (_output_size + run_size > output_allocated_size) {
          return LOG_STATUS(Status::CompressionError(
              "Failed compressing coordinates with RLE; output buffer "
              "overflow"));
        }

        // Copy to output buffer
        memcpy(output_cur, input_prev, value_size);
        output_cur += value_size;
        byte = (unsigned char)(cur_run_len >> 8);
        memcpy(output_cur, &byte, sizeof(char));
        output_cur += sizeof(char);
        byte = (unsigned char)(cur_run_len % 256);
        memcpy(output_cur, &byte, sizeof(char));
        output_cur += sizeof(char);
        _output_size += value_size + 2 * sizeof(char);

        // Update current run length
        cur_run_len = 1;
      }

      // Update run info
      input_prev = input_cur;
      input_cur = input_prev + coords_size;
    }

    // Save final run
    //---  Sanity check on ouput size
    if (_output_size + run_size > output_allocated_size) {
      return LOG_STATUS(Status::CompressionError(
          "Failed compressing coordinates with RLE; output buffer overflow"));
    }

    // --- Copy to output buffer
    memcpy(output_cur, input_prev, value_size);
    output_cur += value_size;
    byte = (unsigned char)(cur_run_len >> 8);
    memcpy(output_cur, &byte, sizeof(char));
    output_cur += sizeof(char);
    byte = (unsigned char)(cur_run_len % 256);
    memcpy(output_cur, &byte, sizeof(char));
    output_cur += sizeof(char);
    _output_size += value_size + 2 * sizeof(char);
  }

  // Success
  *output_size = _output_size;
  return Status::Ok();
}

Status RLE_compress_coords_row(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num,
    int64_t* output_size) {
  // Initializations
  int cur_run_len;
  int max_run_len = 65535;
  const unsigned char* input_cur;
  const unsigned char* input_prev;
  unsigned char* output_cur = output;
  size_t coords_size = value_size * dim_num;
  int64_t coords_num = input_size / coords_size;
  int64_t _output_size = 0;
  size_t run_size = value_size + 2 * sizeof(char);
  unsigned char byte;

  *output_size = -1;

  // Sanity check on input buffer format
  if (input_size % coords_size) {
    return LOG_STATUS(
        Status::CompressionError("failed compressing coordinates with RLE; "
                                 "invalid input buffer format"));
  }

  // Trivial case
  if (coords_num == 0) {
    *output_size = _output_size;
    return Status::Ok();
  }
  // Copy the number of coordinates
  if (_output_size + sizeof(int64_t) > output_allocated_size) {
    return LOG_STATUS(Status::CompressionError(
        "failed compressing coordinates with RLE; output buffer overflow"));
  }
  memcpy(output_cur, &coords_num, sizeof(int64_t));
  output_cur += sizeof(int64_t);
  _output_size += sizeof(int64_t);

  // Make runs for each of the first (dim_num-1) dimensions
  for (int d = 0; d < dim_num - 1; ++d) {
    cur_run_len = 1;
    input_prev = input + d * value_size;
    input_cur = input_prev + coords_size;

    // Make single dimension run
    for (int64_t i = 1; i < coords_num; ++i) {
      if (!memcmp(input_cur, input_prev, value_size) &&
          cur_run_len < max_run_len) {  // Expand the run
        ++cur_run_len;
      } else {  // Save the run
        // Sanity check on size
        if (_output_size + run_size > output_allocated_size) {
          return LOG_STATUS(Status::CompressionError(
              "Failed compressing coordinates with RLE; output buffer "
              "overflow"));
        }

        // Copy to output buffer
        memcpy(output_cur, input_prev, value_size);
        output_cur += value_size;
        byte = (unsigned char)(cur_run_len >> 8);
        memcpy(output_cur, &byte, sizeof(char));
        output_cur += sizeof(char);
        byte = (unsigned char)(cur_run_len % 256);
        memcpy(output_cur, &byte, sizeof(char));
        output_cur += sizeof(char);
        _output_size += value_size + 2 * sizeof(char);

        // Update current run length
        cur_run_len = 1;
      }

      // Update run info
      input_prev = input_cur;
      input_cur = input_prev + coords_size;
    }

    // Save final run
    // --- Sanity check on size
    if (_output_size + run_size > output_allocated_size) {
      return LOG_STATUS(Status::CompressionError(
          "Failed compressing coordinates with RLE; output buffer overflow"));
    }

    // --- Copy to output buffer
    memcpy(output_cur, input_prev, value_size);
    output_cur += value_size;
    byte = (unsigned char)(cur_run_len >> 8);
    memcpy(output_cur, &byte, sizeof(char));
    output_cur += sizeof(char);
    byte = (unsigned char)(cur_run_len % 256);
    memcpy(output_cur, &byte, sizeof(char));
    output_cur += sizeof(char);
    _output_size += run_size;
  }

  // Copy the final dimension intact
  // --- Sanity check on size
  if (_output_size + coords_num * value_size > output_allocated_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing coordinates with RLE; output buffer overflow"));
  }
  // --- Copy to output buffer
  input_prev = input + (dim_num - 1) * value_size;
  for (int64_t i = 0; i < coords_num; ++i) {
    memcpy(output_cur, input_prev, value_size);
    input_prev += coords_size;
    output_cur += value_size;
  }
  _output_size += coords_num * value_size;

  // Success
  *output_size = _output_size;
  return Status::Ok();
}

Status RLE_decompress_coords_col(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num) {
  // Initializations
  const unsigned char* input_cur = input;
  unsigned char* output_cur = output;
  int64_t input_offset = 0;
  size_t run_size = value_size + 2 * sizeof(char);
  size_t coords_size = value_size * dim_num;
  int64_t run_len, coords_num;
  unsigned char byte;

  // Get the number of coordinates
  // --- Sanity check on input buffer size
  if (input_offset + sizeof(int64_t) > input_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing coordinates with RLE; input buffer overflow"));
  }
  // --- Copy number of coordinates
  memcpy(&coords_num, input_cur, sizeof(int64_t));
  input_cur += sizeof(int64_t);
  input_offset += sizeof(int64_t);

  // Trivial case
  if (coords_num == 0)
    return Status::Ok();

  // Copy the first dimension intact
  // --- Sanity check on output buffer size
  if (coords_num * coords_size > output_allocated_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing coordinates with RLE; output buffer overflow"));
  }
  // --- Sanity check on output buffer size
  if (input_offset + coords_num * value_size > input_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing coordinates with RLE; input buffer overflow"));
  }
  // --- Copy first dimension to output
  for (int64_t i = 0; i < coords_num; ++i) {
    memcpy(output_cur, input_cur, value_size);
    input_cur += value_size;
    output_cur += coords_size;
  }
  input_offset += coords_num * value_size;

  // Get number of runs
  int64_t run_num = (input_size - input_offset) / run_size;

  // Sanity check on input buffer format
  if ((input_size - input_offset) % run_size) {
    return LOG_STATUS(
        Status::CompressionError("Failed decompressing coordinates with RLE; "
                                 "invalid input buffer format"));
  }

  // Decompress runs for each of the last (dim_num-1) dimensions
  int64_t coords_i = 0;
  int d = 1;
  output_cur = output;
  for (int64_t i = 0; i < run_num; ++i) {
    // Retrieve the current run length
    memcpy(&byte, input_cur + value_size, sizeof(char));
    run_len = (((int64_t)byte) << 8);
    memcpy(&byte, input_cur + value_size + sizeof(char), sizeof(char));
    run_len += (int64_t)byte;

    // Copy to output buffer
    for (int64_t j = 0; j < run_len; ++j) {
      memcpy(
          output_cur + d * value_size + coords_i * coords_size,
          input_cur,
          value_size);
      ++coords_i;
    }

    // Update input tracking info
    input_cur += run_size;
    if (coords_i == coords_num) {  // Change dimension
      coords_i = 0;
      ++d;
    }
  }

  // Success
  return Status::Ok();
}

Status RLE_decompress_coords_row(
    const unsigned char* input,
    size_t input_size,
    unsigned char* output,
    size_t output_allocated_size,
    size_t value_size,
    int dim_num) {
  // Initializations
  const unsigned char* input_cur = input;
  unsigned char* output_cur = output;
  int64_t input_offset = 0;
  size_t run_size = value_size + 2 * sizeof(char);
  size_t coords_size = value_size * dim_num;
  int64_t run_len, coords_num;
  unsigned char byte;

  // Get the number of coordinates
  // --- Sanity check on input buffer size
  if (input_offset + sizeof(int64_t) > input_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing coordinates with RLE; input buffer overflow"));
  }
  // --- Copy number of coordinates
  memcpy(&coords_num, input_cur, sizeof(int64_t));
  input_cur += sizeof(int64_t);
  input_offset += sizeof(int64_t);

  // Trivial case
  if (coords_num == 0)
    return Status::Ok();

  // Sanity check on output buffer size
  if (coords_num * coords_size > output_allocated_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing coordinates with RLE; output buffer overflow"));
  }

  // Get number of runs
  int64_t run_num =
      (input_size - input_offset - coords_num * value_size) / run_size;

  // Sanity check on input buffer format
  if ((input_size - input_offset - coords_num * value_size) % run_size) {
    return LOG_STATUS(
        Status::CompressionError("Failed decompressing coordinates with RLE; "
                                 "invalid input buffer format"));
  }

  // Decompress runs for each of the first (dim_num-1) dimensions
  int64_t coords_i = 0;
  int d = 0;
  for (int64_t i = 0; i < run_num; ++i) {
    // Retrieve the current run length
    memcpy(&byte, input_cur + value_size, sizeof(char));
    run_len = (((int64_t)byte) << 8);
    memcpy(&byte, input_cur + value_size + sizeof(char), sizeof(char));
    run_len += (int64_t)byte;

    // Copy to output buffer
    for (int64_t j = 0; j < run_len; ++j) {
      memcpy(
          output_cur + d * value_size + coords_i * coords_size,
          input_cur,
          value_size);
      ++coords_i;
    }

    // Update input/output tracking info
    input_cur += run_size;
    input_offset += run_size;
    if (coords_i == coords_num) {  // Change dimension
      coords_i = 0;
      ++d;
    }
  }

  // Copy the last dimension intact
  // --- Sanity check on input buffer size
  if (input_offset + coords_num * value_size > input_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing coordinates with RLE; input buffer overflow"));
  }
  // --- Copy to output buffer
  for (int64_t i = 0; i < coords_num; ++i) {
    memcpy(
        output_cur + (dim_num - 1) * value_size + i * coords_size,
        input_cur,
        value_size);
    input_cur += value_size;
  }

  // Success
  return Status::Ok();
}

void split_coordinates(
    void* tile, size_t tile_size, int dim_num, size_t coords_size) {
  // For easy reference
  size_t coord_size = coords_size / dim_num;
  int64_t cell_num = tile_size / coords_size;
  char* tile_c = (char*)tile;
  size_t ptr = 0, ptr_tmp = 0;

  // Create a tile clone
  char* tile_tmp = (char*)malloc(tile_size);
  memcpy(tile_tmp, tile, tile_size);

  // Split coordinates
  for (int j = 0; j < dim_num; ++j) {
    ptr_tmp = j * coord_size;
    for (int64_t i = 0; i < cell_num; ++i) {
      memcpy(tile_c + ptr, tile_tmp + ptr_tmp, coord_size);
      ptr += coord_size;
      ptr_tmp += coords_size;
    }
  }

  // Clean up
  free((void*)tile_tmp);
}

bool starts_with(const std::string& value, const std::string& prefix) {
  if (prefix.size() > value.size())
    return false;
  return std::equal(prefix.begin(), prefix.end(), value.begin());
}

std::string tile_extent_str(const void* tile_extent, Datatype type) {
  std::stringstream ss;

  if (tile_extent == nullptr) {
    return Configurator::null_str();
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

void zip_coordinates(
    void* tile, size_t tile_size, int dim_num, size_t coords_size) {
  // For easy reference
  size_t coord_size = coords_size / dim_num;
  int64_t cell_num = tile_size / coords_size;
  char* tile_c = (char*)tile;
  size_t ptr = 0, ptr_tmp = 0;

  // Create a tile clone
  char* tile_tmp = (char*)malloc(tile_size);
  memcpy(tile_tmp, tile, tile_size);

  // Zip coordinates
  for (int j = 0; j < dim_num; ++j) {
    ptr = j * coord_size;
    for (int64_t i = 0; i < cell_num; ++i) {
      memcpy(tile_c + ptr, tile_tmp + ptr_tmp, coord_size);
      ptr += coords_size;
      ptr_tmp += coord_size;
    }
  }
  // Clean up
  free((void*)tile_tmp);
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
