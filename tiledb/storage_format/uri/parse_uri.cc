/**
 * @file tiledb/sm/storage_format/uri/parse_uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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

Status get_timestamp_range(
    const URI& uri, std::pair<uint64_t, uint64_t>* timestamp_range) {
  // Initializations
  auto name = uri.remove_trailing_slash().last_path_part();
  *timestamp_range = {0, 0};

  // Cut the suffix
  auto pos = name.find_last_of('.');
  name = (pos == std::string::npos) ? name : name.substr(0, pos);

  // Get fragment version
  auto version = get_fragment_name_version(name);
  if (version == FragmentNameVersion::ONE) {
    // This is equivalent to format_version_t <=2
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

fragment_name_version_t get_fragment_name_version(const std::string& name) {
  // First check if it is version 3 or greater, which has 5 '_'
  // characters in the name.
  size_t n = std::count(name.begin(), name.end(), '_');
  if (n == 5) {
    // Fetch the fragment version from the fragment name. If the fragment
    // version is greater than or equal to 10, we have a footer version of 5.
    // version is greater than or equal to 7, we have a footer version of 4.
    // Otherwise, it is version 3.
    const uint32_t vsn = std::stoul(name.substr(name.find_last_of('_') + 1));
    if (vsn >= 10) {
      return fragment_name_version_t{5};
    } else if (vsn >= 7) {
      return fragment_name_version_t{4};
    } else {
      return fragment_name_version_t{3};
    }
  }

  // Check if it is in version 1 or 2
  // Version 2 has the 32-byte long UUID at the end
  auto t_str = name.substr(name.find_last_of('_') + 1);
  if (t_str.size() == 32) {
    return fragment_name_version_t{2};
  } else {
    return fragment_name_version_t{1};
  }
}

format_version_t get_fragment_version(const std::string& name) {
  auto name_vsn = get_fragment_name_version(name);

  if (name_vsn < FragmentNameVersion::THREE) {
    return format_version_t::invalid_version();
  }

  // name version >= 3
  uint32_t v_int;
  auto v_str = name.substr(name.find_last_of('_') + 1);
  std::stringstream ss(v_str);
  ss >> v_int;
  return format_version_t{v_int};
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

}  // namespace tiledb::sm::utils::parse
