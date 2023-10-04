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
#include "tiledb_experimental.h"
#include "vfs.h"

namespace tiledb {
class VFSExperimental {
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**
   * Typedef for ls inclusion predicate function used to check if a single
   * result should be included in the final results returned from ls or
   * ls_recursive functions.
   *
   * @param path The path of a visited object for the relative filesystem.
   * @return True if the result should be included, else false.
   * @sa ls_include
   */
  typedef std::function<bool(const std::string&)> LsInclude;

  /**
   * Struct to store recursive ls results data.
   * 'object_paths_' Stores all paths collected as a vector of strings.
   * `file_sizes` stores all file sizes as a vector of uint64_t.
   */
  struct LsRecursiveData {
    std::vector<std::string> object_paths_;
    std::vector<uint64_t> object_sizes_;
  };

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
   * @param uri The base URI to list results recursively.
   * @param callback The callback predicate to invoke on each entry collected.
   * @return Vector of pairs storing the path and object size of each result.
   * @sa ls_recursive_gather
   * @sa LsInclude
   */
  static std::vector<std::pair<std::string, uint64_t>> ls_recursive(
      const Context& ctx,
      const VFS& vfs,
      const std::string& uri,
      const LsInclude& callback = ls_include) {
    LsRecursiveData ls_data;
    ctx.handle_error(tiledb_vfs_ls_recursive(
        ctx.ptr().get(),
        vfs.ptr().get(),
        uri.c_str(),
        ls_recursive_gather,
        &ls_data));
    std::vector<std::pair<std::string, uint64_t>> results;
    for (size_t i = 0; i < ls_data.object_paths_.size(); i++) {
      if (callback(ls_data.object_paths_[i])) {
        results.emplace_back(
            ls_data.object_paths_[i], ls_data.object_sizes_[i]);
      }
    }
    return results;
  }

 private:
  /* ********************************* */
  /*       PRIVATE STATIC METHODS      */
  /* ********************************* */

  /**
   * Callback function to be used when invoking the C TileDB function
   * for recursive ls. The callback will cast 'data' to LsRecursiveData struct.
   * The struct stores a vector of strings for each path collected, and a uint64
   * vector of file sizes for the objects at each path.
   *
   * @param path The path of a visited TileDB object
   * @param data Cast to LsRecursiveData struct to store paths and offsets.
   * @return If `1` then the walk should continue to the next object.
   */
  static int ls_recursive_gather(
      const char* path, size_t path_length, uint64_t file_size, void* data) {
    auto ls_data = static_cast<LsRecursiveData*>(data);
    ls_data->object_paths_.emplace_back(path, path_length);
    ls_data->object_sizes_.push_back(file_size);
    return 1;
  }

  /**
   * Default inclusion predicate for ls functions. Optionally, a user can
   * provide their own inclusion predicate to filter results.
   *
   * @return True if the result should be included, else false.
   */
  static bool ls_include(const std::string_view&) {
    return true;
  }
};
}  // namespace tiledb

#endif  // TILEDB_VFS_EXPERIMENTAL_H
