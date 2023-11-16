/**
 * @file fragment_name.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 */

#include "tiledb/storage_format/uri/fragment_name.h"
#include "tiledb/sm/misc/uuid.h"
#include "tiledb/storage_format/uri/parse_uri.h"

#include <sstream>

using namespace tiledb::common;

namespace tiledb::storage_format {

std::string compute_consolidated_fragment_name(
    const URI& first, const URI& last, format_version_t format_version) {
  // Get uuid
  std::string uuid;
  throw_if_not_ok(uuid::generate_uuid(&uuid, false));

  // Get timestamp ranges
  std::pair<uint64_t, uint64_t> t_first, t_last;
  throw_if_not_ok(utils::parse::get_timestamp_range(first, &t_first));
  throw_if_not_ok(utils::parse::get_timestamp_range(last, &t_last));

  if (t_first.first > t_last.second) {
    throw std::logic_error(
        "Error computing consolidated fragment name; "
        "start timestamp cannot be after end timestamp.");
  }

  // Create new URI
  std::stringstream ss;
  ss << "/__" << t_first.first << "_" << t_last.second << "_" << uuid << "_"
     << format_version;

  return ss.str();
}

}  // namespace tiledb::storage_format
