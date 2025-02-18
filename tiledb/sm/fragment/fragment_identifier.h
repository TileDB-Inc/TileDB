/**
 * @file tiledb/sm/fragment/fragment_identifier.h
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
 * This file defines class FragmentID.
 */

#ifndef TILEDB_FRAGMENT_ID_H
#define TILEDB_FRAGMENT_ID_H

#include "tiledb/common/status.h"
#include "tiledb/sm/filesystem/uri.h"

namespace tiledb::sm {

using timestamp_range_type = std::pair<uint64_t, uint64_t>;

/**
 * The possible fragment name versions.
 *
 * ONE: __uuid_t1{_t2}
 * TWO: __t1_t2_uuid
 * THREE: __t1_t2_uuid_version
 *
 * @ref `format_spec/timestamped_name.md`
 */
enum class FragmentNameVersion { ONE, TWO, THREE };

/**
 * Validate, parse and handle the different components of a fragment identifer.
 *
 * @section Known Defects:
 * Construction should fail (but currently does not)
 * with the following _name_ (not uri) inputs:
 *
 * Empty.
 * "X" Non-empty, no underscores.
 * "_" 1 underscore only, no fields.
 * "__" 2 underscores only, no fields.
 * "___" 3 underscores only, all fields empty (version 1).
 * "____" 4 underscores only, all fields empty (versions 2, 3).
 * "_____"5 underscores, all fields are empty (version 3).
 * Missing fields.
 * Correct number of fields, incorrect order.
 * Trailing "_".
 * Otherwise potentially-malformed names, which may not begin with "__".
 */
class FragmentID : private URI {
 private:
  /**
   * Whitebox testing class provides additional accessors to components of the
   * fragment name.
   */
  friend class WhiteboxFragmentID;

  /** The fragment name. */
  std::string name_;
  /** The timestamp range. */
  timestamp_range_type timestamp_range_;

  /** The fragment name version. */
  FragmentNameVersion name_version_;

  /** The array format version. */
  format_version_t array_format_version_;

 public:
  /** Constructor. */
  FragmentID(const URI& uri);

  /** Constructor. */
  FragmentID(const std::string_view& path);

  /** Destructor. */
  ~FragmentID() = default;

  /** Accessor to the fragment name. */
  inline const std::string& name() const {
    return name_;
  }

  /**
   * Accessor to the timestamp range.
   * For array format version <= 2, only the range start is valid
   * (the range end is ignored).
   */
  inline timestamp_range_type timestamp_range() const {
    return timestamp_range_;
  }

  /**
   * Accessor to the fragment name version.
   * Returns an integer corresponding to the FragmentNameVersion.
   *
   * - Name version 1 corresponds to format versions 1 and 2
   *      * __uuid_<t1>{_t2}
   * - Name version 2 corresponds to format version 3 and 4
   *      * __t1_t2_uuid
   * - Name version 3 corresponds to format version 5 or higher
   *      * __t1_t2_uuid_version
   */
  inline int name_version() const {
    if (name_version_ == FragmentNameVersion::ONE) {
      return 1;
    } else if (name_version_ == FragmentNameVersion::TWO) {
      return 2;
    } else {
      return 3;
    }
  }

  /**
   * Accessor to the array format version.
   * Returns UINT32_MAX for name versions <= 2.
   */
  inline format_version_t array_format_version() const {
    return array_format_version_;
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_FRAGMENT_ID_H
