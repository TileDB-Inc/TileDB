/**
 * @file filesystem.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This defines the tiledb Filesystem enum that maps to tiledb_filesystem_t
 * C-api enum.
 */

#ifndef TILEDB_FILESYSTEM_H
#define TILEDB_FILESYSTEM_H

#include "tiledb/sm/misc/constants.h"

namespace tiledb {
namespace sm {

/** Defines a filesystem. */
enum class Filesystem : uint8_t {
#define TILEDB_FILESYSTEM_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_FILESYSTEM_ENUM
};

/** Returns the string representation of the input filesystem type. */
inline const std::string& filesystem_str(Filesystem filesystem_type) {
  switch (filesystem_type) {
    case Filesystem::HDFS:
      return constants::filesystem_type_hdfs_str;
    case Filesystem::S3:
      return constants::filesystem_type_s3_str;
    case Filesystem::AZURE:
      return constants::filesystem_type_azure_str;
    case Filesystem::GCS:
      return constants::filesystem_type_gcs_str;
    case Filesystem::MEMFS:
      return constants::filesystem_type_mem_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the filesystem type given a string representation. */
inline Status filesystem_enum(
    const std::string& filesystem_type_str, Filesystem* filesystem_type) {
  if (filesystem_type_str == constants::filesystem_type_hdfs_str)
    *filesystem_type = Filesystem::HDFS;
  else if (filesystem_type_str == constants::filesystem_type_s3_str)
    *filesystem_type = Filesystem::S3;
  else if (filesystem_type_str == constants::filesystem_type_azure_str)
    *filesystem_type = Filesystem::AZURE;
  else if (filesystem_type_str == constants::filesystem_type_gcs_str)
    *filesystem_type = Filesystem::GCS;
  else if (filesystem_type_str == constants::filesystem_type_mem_str)
    *filesystem_type = Filesystem::MEMFS;
  else
    return Status::Error("Invalid Filesystem " + filesystem_type_str);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILESYSTEM_H
