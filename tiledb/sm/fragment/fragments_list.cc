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
 * #TODO Refactor fragment code to use this class,
 * which is currently only used to deserialize a fragments list into a handle.
 */

#include "tiledb/sm/fragment/fragments_list.h"

using namespace tiledb::common;

namespace tiledb::sm {

class FragmentsListException : public StatusException {
 public:
  explicit FragmentsListException(const std::string& message)
      : StatusException("FragmentsList", message) {
  }
};

/* ********************************* */
/*                API                */
/* ********************************* */

const URI& FragmentsList::fragment_uri(unsigned index) const {
  ensure_fragments_list_has_fragments();
  if (index >= fragments_.size()) {
    throw FragmentsListException(
        "[get_fragment_uri] There is no fragment at the given index");
  }
  return fragments_[index];
}

FragmentsList::size_type FragmentsList::fragment_index(const URI& fragment) {
  ensure_fragments_list_has_fragments();
  for (size_type i = 0; i < fragments_.size(); i++) {
    if (fragments_[i] == fragment) {
      return i;
    }
  }

  throw FragmentsListException(
      "[get_fragment_index] Given fragment is not in the FragmentsList");
}

void FragmentsList::ensure_fragments_list_has_fragments() const {
  if (empty()) {
    throw FragmentsListException(
        "[ensure_fragments_list_has_fragments] FragmentsList is empty");
  }
}

}  // namespace tiledb::sm
