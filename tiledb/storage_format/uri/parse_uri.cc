/**
 * @file tiledb/sm/storage_format/uri/parse_uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2023 TileDB, Inc.
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
 * This file contains functions for parsing URIs for storage of an array.
 */

#include <algorithm>
#include <cinttypes>  // For sscanf PRId64
#include <sstream>

#include "parse_uri.h"
#include "tiledb/common/exception/exception.h"

namespace tiledb::sm::utils::parse {

class ParseURIException : public StatusException {
 public:
  explicit ParseURIException(const std::string& message)
      : StatusException("ParseURI", message) {
  }
};

class FragmentURIException : public StatusException {
 public:
  explicit FragmentURIException(const std::string& message)
      : StatusException("FragmentURI", message) {
  }
};

bool is_element_of(const URI uri, const URI intersecting_uri) {
  std::string prefix = uri.to_string().substr(
      0, std::string(uri.c_str()).size() - (uri.last_path_part()).size());

  std::string intersecting_prefix = intersecting_uri.to_string().substr(
      0,
      std::string(intersecting_uri.c_str()).size() -
          (intersecting_uri.last_path_part()).size());

  return (prefix == intersecting_prefix);
}

FragmentURI::FragmentURI(const URI& uri)
    : uri_(uri)
    , name_(uri.remove_trailing_slash().last_path_part())
    , timestamp_range_({0, 0})
    , name_version_(FragmentNameVersion::ONE) {
  // Ensure input uri is valid (non-empty)
  if (uri.empty()) {
    throw FragmentURIException(
        "Failed to construct FragmentURI; input URI is invalid.");
  }

  // Set name
  auto pos = name_.find_last_of('.');
  name_ = (pos == std::string::npos) ? name_ : name_.substr(0, pos);
  assert(name_.find_last_of('_') != std::string::npos);
  auto version_str = name_.substr(name_.find_last_of('_') + 1);

  // Set name version
  size_t n = std::count(name_.begin(), name_.end(), '_');
  if (n == 5) {
    name_version_ = FragmentNameVersion::THREE;
  } else if (version_str.size() == 32) {
    name_version_ = FragmentNameVersion::TWO;
  }

  // Set fragment version
  uint32_t ret;
  std::stringstream ss(version_str);
  ss >> ret;
  version_ = ret;

  // Set timestamp range
  if (name_version_ == FragmentNameVersion::ONE) {
    version_ = 2;
    sscanf(
        version_str.c_str(),
        (std::string("%") + std::string(PRId64)).c_str(),
        (long long int*)&timestamp_range_.first);
    timestamp_range_.second = timestamp_range_.first;
  } else {
    if (name_version_ == FragmentNameVersion::TWO) {
      version_ = 4;
    }
    sscanf(
        name_.c_str(),
        (std::string("__%") + std::string(PRId64) + "_%" + std::string(PRId64))
            .c_str(),
        (long long int*)&timestamp_range_.first,
        (long long int*)&timestamp_range_.second);
  }

  if (timestamp_range_.first > timestamp_range_.second) {
    throw FragmentURIException(
        "Failed to construct FragmentURI; start timestamp cannot "
        "be after end timestamp");
  }
}

const URI& FragmentURI::uri() {
  return uri_;
}

const std::string& FragmentURI::name() {
  return name_;
}

timestamp_range_type FragmentURI::timestamp_range() {
  return timestamp_range_;
}

/**
 * Retrieves the fragment name version.
 *  - Name version 1 corresponds to format versions 1 and 2
 *      * __uuid_<t1>{_t2}
 *  - Name version 2 corresponds to format version 3 and 4
 *      * __t1_t2_uuid
 *  - Name version 3 corresponds to format version 5 or higher
 *      * __t1_t2_uuid_version
 */
format_version_t FragmentURI::version() {
  return version_;
}

}  // namespace tiledb::sm::utils::parse
