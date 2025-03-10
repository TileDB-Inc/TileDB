/**
 * @file tiledb/sm/fragment/fragment_identifier.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file implements class FragmentID.
 */

#include <algorithm>
#include <cinttypes>  // For sscanf PRId64
#include <sstream>

#include "fragment_identifier.h"
#include "tiledb/common/exception/exception.h"

namespace tiledb::sm {

class FragmentIDException : public StatusException {
 public:
  explicit FragmentIDException(const std::string& message)
      : StatusException("FragmentID", message) {
  }
};

class InvalidURIException : public FragmentIDException {
 public:
  explicit InvalidURIException(const std::string& message)
      : FragmentIDException("input URI is invalid. " + message) {
  }
};

FragmentID::FragmentID(const URI& uri)
    : name_(uri.remove_trailing_slash().last_path_part())
    , timestamp_range_({0, 0})
    , name_version_(FragmentNameVersion::ONE) {
  // Ensure input uri is valid (non-empty)
  if (uri.empty()) {
    throw InvalidURIException("URI may not be empty.");
  }

  // Set name
  auto pos = name_.find_last_of('.');
  name_ = (pos == std::string::npos) ? name_ : name_.substr(0, pos);
  if (name_.find_last_of('_') == std::string::npos) {
    throw InvalidURIException("Provided URI does not contain a fragment name.");
  }

  // Set array format version
  auto array_format_version_str = name_.substr(name_.find_last_of('_') + 1);
  std::stringstream ss(array_format_version_str);
  ss >> array_format_version_;

  // Set name version
  size_t n = std::count(name_.begin(), name_.end(), '_');
  if (n == 5) {
    name_version_ = FragmentNameVersion::THREE;
  } else if (array_format_version_str.size() == 32) {
    name_version_ = FragmentNameVersion::TWO;
  }

  // Set timestamp range
  if (name_version_ == FragmentNameVersion::ONE) {
    array_format_version_ = 2;
    sscanf(
        array_format_version_str.c_str(),
        "%" PRId64,
        (int64_t*)&timestamp_range_.first);
    timestamp_range_.second = timestamp_range_.first;
  } else {
    if (name_version_ == FragmentNameVersion::TWO) {
      array_format_version_ = 4;
    }
    sscanf(
        name_.c_str(),
        "__%" PRId64 "_%" PRId64,
        (int64_t*)&timestamp_range_.first,
        (int64_t*)&timestamp_range_.second);
  }
  if (timestamp_range_.first > timestamp_range_.second) {
    throw FragmentIDException(
        "Failed to construct FragmentID; start timestamp cannot "
        "be after end timestamp");
  }
}

bool FragmentID::has_fragment_name(const URI& uri) {
  if (uri.empty()) {
    throw InvalidURIException("URI may not be empty.");
  }

  auto name = uri.remove_trailing_slash().last_path_part();
  auto pos = name.find_last_of('.');
  if (pos != std::string::npos) {
    name = name.substr(0, pos);
  }
  return name.find_last_of('_') != std::string::npos;
}

FragmentID::FragmentID(const std::string_view& path)
    : FragmentID(URI(path)) {
}

}  // namespace tiledb::sm
