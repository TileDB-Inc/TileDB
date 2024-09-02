/**
 * @file generate_uri.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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

#include "tiledb/storage_format/uri/generate_uri.h"
#include "tiledb/common/random/random_label.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/storage_format/uri/parse_uri.h"

#include <sstream>

using namespace tiledb::common;

namespace tiledb::storage_format {

std::string generate_timestamped_name(
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    std::optional<format_version_t> version) {
  if (timestamp_start > timestamp_end) {
    throw std::logic_error(
        "Error generating timestamped name; "
        "start timestamp cannot be after end timestamp.");
  }

  auto lbl = random_label_with_timestamp();
  if (timestamp_start == 0 && timestamp_end == 0) {
    timestamp_start = timestamp_end = lbl.timestamp_;
  }

  std::stringstream ss;
  ss << "/__" << timestamp_start << "_" << timestamp_end << "_"
     << lbl.random_label_;

  if (version.has_value()) {
    ss << "_" << version.value();
  }

  return ss.str();
}

std::string generate_timestamped_name(
    uint64_t timestamp, format_version_t format_version) {
  return generate_timestamped_name(timestamp, timestamp, format_version);
}

std::string generate_consolidated_fragment_name(
    const URI& first, const URI& last, format_version_t format_version) {
  // Get timestamp ranges
  tiledb::sm::FragmentID fragment_id_first{first};
  auto t_first{fragment_id_first.timestamp_range()};

  tiledb::sm::FragmentID fragment_id_last{last};
  auto t_last{fragment_id_last.timestamp_range()};

  return generate_timestamped_name(
      t_first.first, t_last.second, format_version);
}

}  // namespace tiledb::storage_format
