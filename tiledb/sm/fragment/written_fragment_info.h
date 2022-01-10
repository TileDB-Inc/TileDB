/**
 * @file   written_fragment_info.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines struct WrittenFragmentInfo.
 */

#ifndef TILEDB_WRITTEN_FRAGMENT_INFO_H
#define TILEDB_WRITTEN_FRAGMENT_INFO_H

#include <utility>

#include "tiledb/sm/filesystem/uri.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Stores information about a fragment that is being written by a
 * WRITE query.
 */
struct WrittenFragmentInfo {
  /** The URI of the fragment. */
  URI uri_;

  /** The timestamp range of the fragment. */
  std::pair<uint64_t, uint64_t> timestamp_range_;

  /** Constructor. */
  WrittenFragmentInfo(
      const URI& uri, const std::pair<uint64_t, uint64_t>& timestamp_range)
      : uri_(uri)
      , timestamp_range_(timestamp_range) {
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_WRITTEN_FRAGMENT_INFO_H
