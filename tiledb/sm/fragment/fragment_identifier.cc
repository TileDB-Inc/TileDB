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
    : URI(uri)
    , name_(uri.remove_trailing_slash().last_path_part())
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
        (std::string("%") + std::string(PRId64)).c_str(),
        (long long int*)&timestamp_range_.first);
    timestamp_range_.second = timestamp_range_.first;
  } else {
    if (name_version_ == FragmentNameVersion::TWO) {
      array_format_version_ = 4;
    }
    sscanf(
        name_.c_str(),
        (std::string("__%") + std::string(PRId64) + "_%" + std::string(PRId64))
            .c_str(),
        (long long int*)&timestamp_range_.first,
        (long long int*)&timestamp_range_.second);
  }
  if (timestamp_range_.first > timestamp_range_.second) {
    throw FragmentIDException(
        "Failed to construct FragmentID; start timestamp cannot "
        "be after end timestamp");
  }
}

FragmentID::FragmentID(const std::string_view& path)
    : FragmentID(URI(path)) {
}

std::string_view FragmentID::uuid() const {
  // Set UUID
  constexpr size_t UUID_PRINT_LEN = 32;
  if (name_version_ == FragmentNameVersion::ONE) {
    return std::string_view(
        name_.begin() + strlen("__"),
        name_.begin() + strlen("__") + UUID_PRINT_LEN);
  } else if (name_version_ == FragmentNameVersion::TWO) {
    return std::string_view(name_.end() - UUID_PRINT_LEN, name_.end());
  } else {
    const size_t trailing = name_.substr(name_.find_last_of('_')).size();
    return std::string_view(
        name_.end() - UUID_PRINT_LEN - trailing, name_.end() - trailing);
  }
}

std::optional<std::string_view> FragmentID::submillisecond_counter() const {
  // version was bumped to 21 in Nov 2023
  // sub-millisecond was added in #4800 (8ea85dcfc), March 2024
  // version was bumped to 22 in June 2024
  if (array_format_version() < SUBMILLI_PREFIX_FORMAT_VERSION) {
    return std::nullopt;
  }

  return std::string_view(uuid().data(), 8);
}

}  // namespace tiledb::sm
