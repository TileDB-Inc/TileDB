/**
 * @file   object.cc
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
 * This file implements standalone object functions.
 */

#include "tiledb/sm/object/object.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/rest/rest_client.h"

namespace tiledb::sm {

/* ********************************* */
/*                API                */
/* ********************************* */

bool is_array(ContextResources& resources, const URI& uri) {
  // Handle remote array
  if (uri.is_tiledb()) {
    auto&& [st, exists] =
        resources.rest_client()->check_array_exists_from_rest(uri);
    throw_if_not_ok(st);
    assert(exists.has_value());
    return exists.value();
  } else {
    // Check if the schema directory exists or not
    auto& vfs = resources.vfs();
    bool dir_exists = false;
    throw_if_not_ok(vfs.is_dir(
        uri.join_path(constants::array_schema_dir_name), &dir_exists));

    if (dir_exists) {
      return true;
    }

    bool schema_exists = false;
    // If there is no schema directory, we check schema file
    throw_if_not_ok(vfs.is_file(
        uri.join_path(constants::array_schema_filename), &schema_exists));
    return schema_exists;
  }
}

Status is_group(ContextResources& resources, const URI& uri, bool* is_group) {
  // Handle remote array
  if (uri.is_tiledb()) {
    auto&& [st, exists] =
        resources.rest_client()->check_group_exists_from_rest(uri);
    throw_if_not_ok(st);
    *is_group = *exists;
  } else {
    // Check for new group details directory
    auto& vfs = resources.vfs();
    throw_if_not_ok(
        vfs.is_dir(uri.join_path(constants::group_detail_dir_name), is_group));

    if (*is_group) {
      return Status::Ok();
    }

    // Fall back to older group file to check for legacy (pre-format 12) groups
    throw_if_not_ok(
        vfs.is_file(uri.join_path(constants::group_filename), is_group));
  }
  return Status::Ok();
}

}  // namespace tiledb::sm
