/**
 * @file tiledb/api/c_api/dimension_label/dimension_label_api_internal.h
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
 * This file declares the internals of the dimension label section of the C API.
 */

#ifndef TILEDB_CAPI_DIMENSION_LABEL_INTERNAL_H
#define TILEDB_CAPI_DIMENSION_LABEL_INTERNAL_H

#include "../../c_api_support/handle/handle.h"
#include "../config/config_api_internal.h"
#include "../error/error_api_internal.h"
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/filesystem/uri.h"

/**
 * Handle `struct` for API dimension label objects.
 */
struct tiledb_dimension_label_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"dimension label"};

 private:
  tiledb::sm::DimensionLabel dim_label_;
  tiledb::sm::URI uri_;

 public:
  tiledb_dimension_label_handle_t() = delete;

  /**
   * Construct for the dimension label handle.
   *
   * This copies the dimension label
   */
  explicit tiledb_dimension_label_handle_t(
      const tiledb::sm::URI& array_uri,
      const tiledb::sm::DimensionLabel& dim_label)
      : dim_label_{dim_label}
      , uri_{array_uri.empty() ? dim_label_.uri() : dim_label_.uri(array_uri)} {
  }

  [[nodiscard]] inline const tiledb::sm::DimensionLabel& dimension_label()
      const {
    return dim_label_;
  }

  [[nodiscard]] inline const tiledb::sm::URI& uri() const {
    return uri_;
  }
};

namespace tiledb::api {
/**
 * Returns if the argument is a valid dimension label: non-null, valid as a
 * handle
 *
 * @param dim_label A dimension label of unknown validity
 */
inline void ensure_dimension_label_is_valid(
    const tiledb_dimension_label_handle_t* dim_label) {
  ensure_handle_is_valid(dim_label);
}
}  // namespace tiledb::api

#endif
