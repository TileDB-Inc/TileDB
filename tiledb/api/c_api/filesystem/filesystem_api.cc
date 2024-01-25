/**
 * @file tiledb/api/c_api/filesystem/filesystem_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines the filesystem section of the C API for TileDB.
 */

#include "filesystem_api_external.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/enums/filesystem.h"

namespace tiledb::api {

capi_return_t tiledb_filesystem_to_str(
    tiledb_filesystem_t filesystem, const char** str) {
  const auto& strval =
      tiledb::sm::filesystem_str((tiledb::sm::Filesystem)filesystem);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_filesystem_from_str(
    const char* str, tiledb_filesystem_t* filesystem) {
  tiledb::sm::Filesystem val = tiledb::sm::Filesystem::S3;
  if (!tiledb::sm::filesystem_enum(str, &val).ok())
    return TILEDB_ERR;
  *filesystem = (tiledb_filesystem_t)val;
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_plain;

CAPI_INTERFACE(
    filesystem_to_str, tiledb_filesystem_t filesystem, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_filesystem_to_str>(
      filesystem, str);
}

CAPI_INTERFACE(
    filesystem_from_str, const char* str, tiledb_filesystem_t* filesystem) {
  return api_entry_plain<tiledb::api::tiledb_filesystem_from_str>(
      str, filesystem);
}
