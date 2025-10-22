/**
 * @file   local.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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
 * This file includes declarations of the base Local VFS.
 */

#include <string>

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/filesystem/filesystem_base.h"

namespace tiledb::sm {
/**
 * Contains the local filesystem code shared by the Windows and POSIX
 * filesystems.
 */
class LocalFilesystem : public FilesystemBase {
 public:
  LsObjects ls_filtered(
      const URI& parent,
      ResultFilter result_filter,
      bool recursive) const override;

  LsObjects ls_filtered_v2(
      const URI& parent,
      ResultFilterV2 result_filter,
      bool recursive) const override;

  void copy_file(const URI& old_uri, const URI& new_uri) override;

  void copy_dir(const URI& old_uri, const URI& new_uri) override;

 protected:
  /**
   * Creates the containing directories of a path if they do not exist.
   *
   * @param path The path to a file.
   * @return Status
   */
  static Status ensure_directory(const std::string& path);
};
}  // namespace tiledb::sm
