/**
 * @file   fragments_list.cc
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
 * This file implements the FragmentsList class.
 */

#include "tiledb/sm/fragment/fragments_list.h"

using namespace tiledb::common;

namespace tiledb::sm {

class FragmentsListStatusException : public StatusException {
 public:
  explicit FragmentsListStatusException(const std::string& message)
      : StatusException("FragmentsList", message) {
  }
};

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

FragmentsList::FragmentsList()
    : fragments_() {
}

FragmentsList::FragmentsList(const std::vector<URI>& fragments)
    : fragments_(fragments) {
}

FragmentsList::~FragmentsList() = default;

/* ********************************* */
/*                API                */
/* ********************************* */

URI& FragmentsList::get_fragment_uri(int index) {
  if (index >= int(fragments_.size())) {
    throw FragmentsListStatusException(
        "[get_fragment_uri] There is no fragment at the given index");
  }
  return fragments_[index];
}

int FragmentsList::get_fragment_index(const URI& fragment) {
  auto iter = std::find(fragments_.begin(), fragments_.end(), fragment);
  if (iter == fragments_.end()) {
    throw FragmentsListStatusException(
        "[get_fragment_index] Given fragment is not in the FragmentsList");
  } else {
    return std::distance(fragments_.begin(), iter);
  }
}

}  // namespace tiledb::sm
