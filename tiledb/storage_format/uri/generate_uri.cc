/**
 * @file generate_uri.cc
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
 */

#include "tiledb/storage_format/uri/generate_uri.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/misc/uuid.h"

#include <sstream>

using namespace tiledb::common;

namespace tiledb::storage_format {

std::string generate_uri(
    uint64_t timestamp_start, uint64_t timestamp_end, uint32_t version) {
  std::stringstream ss;
  ss << "/__" << timestamp_start << "_" << timestamp_end << "_"
     << sm::uuid::generate_uuid(false) << "_" << version;

  return ss.str();
}

std::string generate_fragment_name(
    uint64_t timestamp, format_version_t format_version) {
  timestamp =
      (timestamp != 0) ? timestamp : sm::utils::time::timestamp_now_ms();
  return generate_uri(timestamp, timestamp, format_version);
}

}  // namespace tiledb::storage_format
