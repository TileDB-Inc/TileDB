/**
 * @file   object.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024-2025 TileDB, Inc.
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
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/storage_format/uri/parse_uri.h"

namespace tiledb::sm {

class ObjectException : public StatusException {
 public:
  explicit ObjectException(const std::string& message)
      : StatusException("Object", message) {
  }
};

/* ********************************* */
/*                API                */
/* ********************************* */

bool is_array(ContextResources& resources, const URI& uri) {
  // Handle remote array
  if (uri.is_tiledb()) {
    auto&& [st, exists] =
        resources.rest_client()->check_array_exists_from_rest(uri);
    throw_if_not_ok(st);
    return exists.value();
  } else {
    // Check if the schema directory exists or not
    auto& vfs = resources.vfs();
    if (vfs.is_dir(uri.join_path(constants::array_schema_dir_name))) {
      return true;
    }

    // If there is no schema directory, we check schema file
    return vfs.is_file(uri.join_path(constants::array_schema_filename));
  }
}

bool is_group(ContextResources& resources, const URI& uri) {
  // Handle remote array
  if (uri.is_tiledb()) {
    auto&& [st, exists] =
        resources.rest_client()->check_group_exists_from_rest(uri);
    throw_if_not_ok(st);
    return exists.value();
  } else {
    // Check for new group details directory
    auto& vfs = resources.vfs();
    if (vfs.is_dir(uri.join_path(constants::group_detail_dir_name))) {
      return true;
    }

    // Fall back to older group file to check for legacy (pre-format 12) groups
    return vfs.is_file(uri.join_path(constants::group_filename));
  }
}

ObjectType object_type(ContextResources& resources, const URI& uri) {
  URI dir_uri = uri;
  if (uri.is_s3() || uri.is_azure() || uri.is_gcs()) {
    // Always add a trailing '/' in the S3/Azure/GCS case so that listing the
    // URI as a directory will work as expected. Listing a non-directory object
    // is not an error for S3/Azure/GCS.
    auto uri_str = uri.to_string();
    dir_uri =
        URI(utils::parse::ends_with(uri_str, "/") ? uri_str : (uri_str + "/"));
  } else if (!uri.is_tiledb()) {
    // For non public cloud backends, listing a non-directory is an error.
    if (!resources.vfs().is_dir(uri)) {
      return ObjectType::INVALID;
    }
  }

  if (is_array(resources, uri)) {
    return ObjectType::ARRAY;
  }
  if (is_group(resources, uri)) {
    return ObjectType::GROUP;
  }

  return ObjectType::INVALID;
}

void object_move(
    ContextResources& resources, const char* old_path, const char* new_path) {
  auto old_uri = URI(old_path);
  if (old_uri.is_invalid()) {
    throw ObjectException(
        "Cannot move object '" + std::string(old_path) + "'; Invalid URI");
  }

  auto new_uri = URI(new_path);
  if (new_uri.is_invalid()) {
    throw ObjectException(
        "Cannot move object to '" + std::string(new_path) + "'; Invalid URI");
  }

  if (object_type(resources, old_uri) == ObjectType::INVALID) {
    throw ObjectException(
        "Cannot move object '" + std::string(old_path) +
        "'; Invalid TileDB object");
  }

  resources.vfs().move_dir(old_uri, new_uri);
}

void object_remove(ContextResources& resources, const char* path) {
  auto uri = URI(path);
  if (uri.is_invalid()) {
    throw ObjectException(
        "Cannot remove object '" + std::string(path) + "'; Invalid URI");
  }

  if (object_type(resources, uri) == ObjectType::INVALID) {
    throw ObjectException(
        "Cannot remove object '" + std::string(path) +
        "'; Invalid TileDB object");
  }

  resources.vfs().remove_dir(uri);
}

}  // namespace tiledb::sm
