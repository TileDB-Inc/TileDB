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

#include "parse_uri.h"
#include <algorithm>
#include <cinttypes>  // For sscanf PRId64
#include <sstream>

namespace tiledb::sm::utils::parse {

/**
 * The possible fragment name versions.
 */
enum class FragmentNameVersion { ONE, TWO, THREE };

/**
 * Retrieves the fragment name version.
 *  - Name version 1 corresponds to format versions 1 and 2
 *      * __uuid_<t1>{_t2}
 *  - Name version 2 corresponds to format version 3 and 4
 *      * __t1_t2_uuid
 *  - Name version 3 corresponds to format version 5 or higher
 *      * __t1_t2_uuid_version
 */
FragmentNameVersion get_fragment_name_version(const std::string& name) {
  // Format Version 3: __t1_t2_uuid_version
  size_t n = std::count(name.begin(), name.end(), '_');
  if (n == 5) {
    return FragmentNameVersion::THREE;
  }

  // Format Version 2: __t1_t2_uuid
  auto maybe_uuid = name.substr(name.find_last_of('_') + 1);
  if (maybe_uuid.size() == 32) {
    return FragmentNameVersion::TWO;
  }

  // Else, Format Version 1: __uuid_t1{_t2}
  return FragmentNameVersion::ONE;
}

Status get_timestamp_range(
    const URI& uri, std::pair<uint64_t, uint64_t>* timestamp_range) {
  // Initializations
  auto name = uri.remove_trailing_slash().last_path_part();
  *timestamp_range = {0, 0};

  // Cut the suffix
  auto pos = name.find_last_of('.');
  name = (pos == std::string::npos) ? name : name.substr(0, pos);

  // Get fragment name version
  auto name_version = get_fragment_name_version(name);

  // FragmentNameVersion::ONE is equivalent to fragment format version <= 2
  if (name_version == FragmentNameVersion::ONE) {
    assert(name.find_last_of('_') != std::string::npos);
    auto t_str = name.substr(name.find_last_of('_') + 1);
    sscanf(
        t_str.c_str(),
        (std::string("%") + std::string(PRId64)).c_str(),
        (long long int*)&timestamp_range->first);
    timestamp_range->second = timestamp_range->first;
  } else {
    assert(name.find_last_of('_') != std::string::npos);
    sscanf(
        name.c_str(),
        (std::string("__%") + std::string(PRId64) + "_%" + std::string(PRId64))
            .c_str(),
        (long long int*)&timestamp_range->first,
        (long long int*)&timestamp_range->second);
  }

  if (timestamp_range->first > timestamp_range->second) {
    throw std::logic_error(
        "Error retrieving timestamp range from URI; start timestamp cannot "
        "be after end timestamp");
  }

  return Status::Ok();
}

format_version_t get_fragment_version(const std::string& name) {
  auto name_version = get_fragment_name_version(name);

  if (name_version == FragmentNameVersion::ONE) {
    return 2;
  }

  if (name_version == FragmentNameVersion::TWO) {
    return 4;
  }

  uint32_t ret;
  auto version_str = name.substr(name.find_last_of('_') + 1);
  std::stringstream ss(version_str);
  ss >> ret;
  return ret;
}

bool is_element_of(const URI uri, const URI intersecting_uri) {
  std::string prefix = uri.to_string().substr(
      0, std::string(uri.c_str()).size() - (uri.last_path_part()).size());

  std::string intersecting_prefix = intersecting_uri.to_string().substr(
      0,
      std::string(intersecting_uri.c_str()).size() -
          (intersecting_uri.last_path_part()).size());

  return (prefix == intersecting_prefix);
}

std::string backend_name(const std::string& path) {
  if (path.substr(0, 1) == "/") {
    return "posix";
  }

  auto backend = path.substr(0, path.find_first_of(':'));
  if (backend == "file") {
    return "windows";
  } else {
    return backend;
  }
}

}  // namespace tiledb::sm::utils::parse
