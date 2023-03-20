/**
 * @file tiledb/api/c_api/fragments_list/fragments_list_api_internal.h
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
 * This file contains internal implementation details of the fragments list
 * section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_FRAGMENTS_LIST_INTERNAL_H
#define TILEDB_CAPI_FRAGMENTS_LIST_INTERNAL_H

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/fragment/fragments_list.h"

/**
 * Handle `struct` for API fragments list objects.
 */
struct tiledb_fragments_list_handle_t
    : public tiledb::api::CAPIHandle<tiledb_fragments_list_handle_t> {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"fragments list"};

 private:
  tiledb::sm::FragmentsList frag_list_;

 public:
  /** Default constructor constructs an empty fragments list */
  tiledb_fragments_list_handle_t()
      : frag_list_{} {
  }

  /**
   * Ordinary constructor.
   * @param f A list of fragments
   */
  explicit tiledb_fragments_list_handle_t(const std::vector<tiledb::sm::URI>& f)
      : frag_list_{f} {
  }

  [[nodiscard]] inline const tiledb::sm::FragmentsList& fragments_list() const {
    return frag_list_;
  }

  tiledb::sm::URI& get_fragment_uri(int index) {
    return frag_list_.get_fragment_uri(index);
  }

  int get_fragment_index(const char* uri) {
    return frag_list_.get_fragment_index(tiledb::sm::URI(uri));
  }
};

namespace tiledb::api {

/**
 * Returns after successfully validating a fragments list handle.
 * Throws otherwise.
 *
 * @param f Possibly-valid pointer to a fragments list handle
 */
inline void ensure_fragments_list_is_valid(
    const tiledb_fragments_list_handle_t* f) {
  ensure_handle_is_valid(f);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_FRAGMENTS_LIST_INTERNAL_H
