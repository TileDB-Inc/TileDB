/**
 * @file tiledb/sm/storage_format/uri/parse_uri.h
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

#ifndef TILEDB_PARSE_URI_H
#define TILEDB_PARSE_URI_H

#include "tiledb/common/status.h"
#include "tiledb/sm/filesystem/uri.h"

namespace tiledb::sm::utils::parse {

using timestamp_range_type = std::pair<uint64_t, uint64_t>;

/**
 * The possible fragment name versions.
 *
 * ONE: __uuid_t1{_t2}
 * TWO: __t1_t2_uuid
 * THREE: __t1_t2_uuid_version
 */
enum class FragmentNameVersion { ONE, TWO, THREE };

/**
 * Returns true if the given URIs have the same "prefix" and could
 * potentially intersect one another.
 * i.e. The second URI is an element of the first (or vice versa).
 *
 * Note: The order of the arguments does not matter;
 * the API is checking for working tree intersection.
 */
bool is_element_of(const URI uri, const URI intersecting_uri);

/** Validate, parse and handle the different components of a fragment uri. */
class FragmentURI {
 private:
  /** The original fragment uri. */
  const URI& uri_;

  /** The fragment name. */
  std::string name_;

  /** The timestamp range. */
  timestamp_range_type timestamp_range_;

  /** The fragment name version. */
  FragmentNameVersion name_version_;

  /** The fragment version. */
  format_version_t version_;

 public:
  /** Constructor. */
  FragmentURI(const URI& uri);

  /** Destructor. */
  ~FragmentURI() = default;

  /** Accessors. */
  const URI& uri();
  const std::string& name();

  /**
   * Accessor to the timestamp range.
   * For format version <= 2, only the range start is valid
   * (the range end is ignored).
   */
  timestamp_range_type timestamp_range();

  /**
   * Accessor to the fragment version.
   * Returns UINT32_MAX for name versions <= 2.
   */
  format_version_t version();
};

}  // namespace tiledb::sm::utils::parse

#endif  // TILEDB_PARSE_URI_H
