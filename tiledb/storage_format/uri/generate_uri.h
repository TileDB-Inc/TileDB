/**
 * @file generate_uri.h
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
 * Functions for generating TileDB URIs.
 */

#ifndef TILEDB_GENERATE_URI_H
#define TILEDB_GENERATE_URI_H

#include "tiledb/common/common.h"
#include "tiledb/sm/filesystem/uri.h"

#include <optional>

using namespace tiledb::common;
using namespace tiledb::sm;

namespace tiledb::storage_format {

/**
 * Generate a new URI in the form `__t1_t2_uuid_v`, where `t1` is the starting
 * timestamp, `t2` is the ending timestamp,  and `v` is the current format
 * version, if provided. For instance,
 * `__1458759561320_1458759561480_6ba7b8129dad11d180b400c04fd430c8_3`.
 *
 * @param timestamp_start The staring timestamp. It is in ms since
 *     1970-01-01 00:00:00 +0000 (UTC).
 * @param timestamp_end The ending timestamp. It is in ms since
 *     1970-01-01 00:00:00 +0000 (UTC).
 * @param version Optional version number to append to the URI.
 *
 * @return The new URI.
 */
std::string generate_timestamped_name(
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    std::optional<format_version_t> version);

/**
 * Generates a new fragment name.
 *
 * Generates a fragment name in the form `__t_t_uuid_v`, where `t` is the input
 * timestamp and `v` is the current format version. For instance,
 * `__1458759561320_1458759561320_6ba7b8129dad11d180b400c04fd430c8_3`.
 *
 * If `timestamp` is 0, then it is set to the current time.
 *
 * @param timestamp The timestamp of when the array got opened for writes. It
 *     is in ms since 1970-01-01 00:00:00 +0000 (UTC).
 * @param format_version The write version.
 *
 * @return new fragment name.
 */
std::string generate_timestamped_name(
    uint64_t timestamp, format_version_t format_version);

/**
 * Generates a consolidated fragment name in the form
 * `__<first_URI_timestamp>_<last_URI_timestamp>_<uuid>`.
 *
 * @param first The first URI.
 * @param last The last URI.
 * @param format_version The write version.
 *
 * @return consolidated fragment name.
 */
std::string generate_consolidated_fragment_name(
    const URI& first, const URI& last, format_version_t format_version);

}  // namespace tiledb::storage_format

#endif
