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
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  FragmentsList();

  /** Constructor. */
  FragmentsList(const std::vector<URI>& fragments);

  /** Destructor. */
  ~FragmentsList();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the fragment URI at the given index. */
  URI& get_fragment_uri(int index);

  /**
   * Returns the index at which the given fragment resides in the list.
   * Throws if the fragment is not in the list.
   */
  int get_fragment_index(const URI& fragment);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The fragments in the FragmentsList. */
  std::vector<URI> fragments_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_FRAGMENTS_LIST_H
