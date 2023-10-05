/**
 * @file vfs_experimental.h
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
 * This file declares the experimental C++ API for VFS.
 */

#ifndef TILEDB_VFS_EXPERIMENTAL_H
#define TILEDB_VFS_EXPERIMENTAL_H

#include <string>
#include <vector>
#include "context.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb_experimental.h"
#include "vfs.h"

namespace tiledb {
class VFSExperimental {
 public:
  /* ********************************* */
  /*       PUBLIC STATIC METHODS       */
  /* ********************************* */

  /**
   * Recursively lists objects at the input URI, invoking the provided callback
   * on each entry gathered. The callback should return true if the entry should
   * be included in the results. By default all entries are included using the
   * ls_include predicate, which simply returns true for all results.
   *
   * @param ctx The TileDB context.
   * @param vfs The VFS instance to use.
   * @param uri The base URI to list results recursively.
   * @param predicate The inclusion predicate to invoke on each entry collected.
   * @return Vector of pairs storing the path and object size of each result.
   * @sa ls_recursive_gather
   * @sa ls_include
   */
  static std::vector<std::pair<std::string, uint64_t>> ls_recursive(
      const Context& ctx,
      const VFS& vfs,
      const std::string& uri,
      const tiledb::sm::VFS::LsInclude& predicate =
          tiledb::sm::VFS::ls_include) {
    tiledb::sm::VFS::LsRecursiveData ls_data;
    ctx.handle_error(tiledb_vfs_ls_recursive(
        ctx.ptr().get(),
        vfs.ptr().get(),
        uri.c_str(),
        tiledb::sm::VFS::ls_recursive_gather,
        &ls_data));
    std::vector<std::pair<std::string, uint64_t>> results;
    for (size_t i = 0; i < ls_data.object_paths_.size(); i++) {
      if (predicate(ls_data.object_paths_[i])) {
        results.emplace_back(
            ls_data.object_paths_[i], ls_data.object_sizes_[i]);
      }
    }
    return results;
  }
};
}  // namespace tiledb

#endif  // TILEDB_VFS_EXPERIMENTAL_H
