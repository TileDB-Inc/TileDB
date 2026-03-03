/**
 * @file tiledb/api/c_api/filter_list/filter_list_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file declares the C API for TileDB.
 */

#ifndef TILEDB_CAPI_FILTER_LIST_INTERNAL_H
#define TILEDB_CAPI_FILTER_LIST_INTERNAL_H

#include "tiledb/api/c_api_support/handle/handle.h"
#include "tiledb/sm/filter/filter_pipeline.h"

/**
 * Handle `struct` for API filter objects.
 */
struct tiledb_filter_list_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"filter list"};

 private:
  using filter_list_type = tiledb::sm::FilterPipeline;
  /**
   * The underling value is a FilterPipeline object.
   */
  filter_list_type value_;

 public:
  /**
   * @param f An object to wrap this handle around
   */
  explicit tiledb_filter_list_handle_t(filter_list_type&& f)
      : value_{f} {
  }

  [[nodiscard]] filter_list_type& pipeline() {
    return value_;
  }

  [[nodiscard]] const filter_list_type& pipeline() const {
    return value_;
  }
};

namespace tiledb::api {
/**
 * Returns after successfully validating a filter.
 *
 * @param filter_list Possibly-valid pointer to a filter
 */
inline void ensure_filter_list_is_valid(
    const tiledb_filter_list_handle_t* filter_list) {
  ensure_handle_is_valid(filter_list);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_FILTER_LIST_INTERNAL_H
