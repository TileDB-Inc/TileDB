/**
 * @file tiledb/api/c_api/group/group_api_internal.h
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
 * This file declares the internal group section of the C API for TileDB.
 */

#ifndef TILEDB_CAPI_GROUP_API_INTERNAL_H
#define TILEDB_CAPI_GROUP_API_INTERNAL_H

#include "../../c_api_support/handle/handle.h"
#include "../error/error_api_internal.h"
#include "tiledb/sm/group/group.h"

struct tiledb_group_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"group"};

 private:
  tiledb::sm::Group group_;

 public:
  tiledb_group_handle_t() = delete;

  explicit tiledb_group_handle_t(
      tiledb::sm::ContextResources& resources, const tiledb::sm::URI& uri)
      : group_(resources, uri) {
  }

  [[nodiscard]] inline tiledb::sm::Group& group() {
    return group_;
  }

  [[nodiscard]] inline tiledb::sm::Group& group() const {
    return static_cast<tiledb::sm::Group&>(
        const_cast<tiledb::sm::Group&>(group_));
  }
};

namespace tiledb::api {

/**
 * Returns if the argument is a valid group: non-null, valid as a handle
 *
 * @param group A group of unknown validity
 */
inline void ensure_group_is_valid(const tiledb_group_handle_t* group) {
  ensure_handle_is_valid(group);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_GROUP_API_INTERNAL_H
