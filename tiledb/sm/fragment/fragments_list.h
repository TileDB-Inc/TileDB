/**
 * @file  fragments_list.h
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
 * This file defines struct FragmentsList.
 * #TODO Refactor fragment code to use this class,
 * which is currently only used to deserialize a fragments list into a handle.
 */

#ifndef TILEDB_FRAGMENTS_LIST_H
#define TILEDB_FRAGMENTS_LIST_H

#include "tiledb/common/common.h"
#include "tiledb/sm/filesystem/uri.h"

#include <algorithm>
#include <vector>

using namespace tiledb::common;

namespace tiledb::sm {

class FragmentsList {
 public:
  using size_type = std::vector<URI>::size_type;
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  FragmentsList()
      : fragments_() {
  }

  /** Constructor. */
  // #TODO Check for dups
  FragmentsList(const std::vector<URI>& fragments)
      : fragments_(fragments) {
  }

  /** Destructor. */
  ~FragmentsList() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the fragment URI at the given index. */
  const URI& fragment_uri(unsigned index) const;

  /**
   * Returns the index at which the given fragment resides in the list.
   * Throws if the fragment is not in the list.
   */
  size_type fragment_index(const URI& fragment);

  /**
   * Returns true if the FragmentsList is empty, false if it contains fragments.
   */
  inline bool empty() const {
    return fragments_.empty();
  }

  /** Checks the FragmentsList contains fragments, throws if it's empty. */
  void ensure_fragments_list_has_fragments() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The fragments in the FragmentsList. */
  std::vector<URI> fragments_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_FRAGMENTS_LIST_H
