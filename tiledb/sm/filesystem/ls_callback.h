/**
 * @file ls_callback.h
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
 *
 * @section DESCRIPTION
 *
 * This defines the type definition used for ls callback functions in VFS.
 */

#ifndef TILEDB_LS_CALLBACK_H
#define TILEDB_LS_CALLBACK_H

#include "tiledb/sm/filesystem/uri.h"

#include <cstdint>
#include <functional>
#include <stdexcept>

/** Inclusion predicate for objects collected by ls */
template <class F>
concept FilePredicate = true;

/**
 * DirectoryPredicate is currently unused, but is kept here for adding directory
 * pruning support in the future.
 */
template <class F>
concept DirectoryPredicate = true;

namespace tiledb::sm {
using FileFilter = std::function<bool(const std::string_view&, uint64_t)>;
// TODO: rename or remove
[[maybe_unused]] static bool no_file_filter(const std::string_view&, uint64_t) {
  return true;
}
using DirectoryFilter = std::function<bool(const std::string_view&)>;
// TODO: rename or remove
static bool no_filter(const std::string_view&) {
  return true;
}

using LsObject = std::pair<std::string, uint64_t>;
using LsObjects = std::vector<LsObject>;

class LsIterator {
 public:
  using value_type = LsObject;
  using difference_type = ptrdiff_t;
  using pointer = LsObject*;
  using reference = LsObject&;
  using iterator_category = std::forward_iterator_tag;

  LsIterator()
      : pos_(0) {
  }

 private:
  LsObjects::iterator it_;
  LsObjects objects_;
  LsObjects::size_type pos_;
};

template <FilePredicate F, DirectoryPredicate D>
class LsScanner {
 public:
  using iterator = LsIterator;

  LsScanner(
      const URI& prefix, F file_filter, D dir_filter, bool recursive = false)
      : prefix_(prefix)
      , file_filter_(file_filter)
      , dir_filter_(dir_filter)
      , is_recursive_(recursive) {
  }

  inline const LsObjects& results() const {
    return results_;
  }

 protected:
  URI prefix_;
  F file_filter_;
  D dir_filter_;
  bool is_recursive_;
  // TODO: remove
  LsObjects results_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_LS_CALLBACK_H